use std::{error::Error, fmt, sync::OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use reqwest::{
    Client, StatusCode, Url,
    header::{
        ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, HeaderMap, HeaderValue, LOCATION, PRAGMA, REFERER,
        USER_AGENT,
    },
};
use serde::{Deserialize, de::DeserializeOwned};

use crate::core::{PlaybackByteRange, PlaybackTrack};

const MIXIN_KEY_ENC_TAB: [usize; 64] = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29,
    28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25,
    54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

const BILIBILI_API: &str = "https://api.bilibili.com/x";
const BILIBILI_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";
const BILIBILI_ORIGIN: &str = "https://www.bilibili.com";
const BILIBILI_HOME: &str = "https://www.bilibili.com/";
const BILIBILI_API_ACCEPT: &str = "application/json, text/plain, */*";
const FINGERPRINT_ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n\r\x0b\x0c";
const FINGERPRINT_SCREEN_SIZES: &[(u32, u32)] = &[
    (1920, 1080),
    (1366, 768),
    (1536, 864),
    (1280, 720),
    (2560, 1440),
    (1440, 900),
    (1600, 900),
];
const BVID_LEN: usize = 12;
const BILIBILI_SHORT_LINK_HOSTS: &[&str] = &["b23.tv", "bili2233.cn"];

#[derive(Debug, Deserialize)]
struct BilibiliResult<T> {
    code: i64,
    message: Option<String>,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct BilibiliVideoInfo {
    pages: Vec<BilibiliVideoPage>,
    title: String,
}

#[derive(Debug, Deserialize)]
struct BilibiliVideoPage {
    duration: u64,
    cid: u64,
    part: String,
}

#[derive(Debug, Deserialize)]
struct BilibiliNavInfo {
    wbi_img: BilibiliWbiInfo,
}

#[derive(Debug, Deserialize)]
struct BilibiliWbiInfo {
    img_url: String,
    sub_url: String,
}

#[derive(Debug, Deserialize)]
struct BilibiliPlayerInfo {
    dash: Option<BilibiliDash>,
    durl: Option<Vec<BilibiliDurl>>,
}

#[derive(Debug, Deserialize)]
struct BilibiliDash {
    audio: Option<Vec<BilibiliAudio>>,
}

#[derive(Debug, Deserialize)]
struct BilibiliAudio {
    id: u64,
    bandwidth: Option<u64>,
    codecs: Option<String>,
    #[serde(rename = "baseUrl")]
    base_url_camel: Option<String>,
    #[serde(rename = "base_url")]
    base_url_snake: Option<String>,
    #[serde(rename = "backupUrl", default)]
    backup_url_camel: Option<Vec<String>>,
    #[serde(default, rename = "backup_url")]
    backup_url_snake: Option<Vec<String>>,
    #[serde(rename = "SegmentBase")]
    segment_base_camel: Option<BilibiliSegmentBase>,
    #[serde(rename = "segment_base")]
    segment_base_snake: Option<BilibiliSegmentBase>,
}

#[derive(Debug, Deserialize)]
struct BilibiliDurl {
    url: String,
    backup_url: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct BilibiliSegmentBase {
    #[serde(rename = "Initialization")]
    initialization_camel: Option<String>,
    #[serde(rename = "initialization")]
    initialization_snake: Option<String>,
    #[serde(rename = "indexRange")]
    index_range_camel: Option<String>,
    #[serde(rename = "index_range")]
    index_range_snake: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MediaCandidate {
    progressive: bool,
    url: String,
    init_range: Option<PlaybackByteRange>,
    index_range: Option<PlaybackByteRange>,
}

impl BilibiliAudio {
    fn base_url(&self) -> Option<&str> {
        [
            self.base_url_camel.as_deref(),
            self.base_url_snake.as_deref(),
        ]
        .into_iter()
        .flatten()
        .find(|url| !url.trim().is_empty())
        .or_else(|| {
            self.backup_url_camel
                .iter()
                .chain(&self.backup_url_snake)
                .flatten()
                .find(|url| !url.trim().is_empty())
                .map(String::as_str)
        })
    }

    fn segment_base(&self) -> Option<&BilibiliSegmentBase> {
        self.segment_base_camel
            .as_ref()
            .or(self.segment_base_snake.as_ref())
    }

    fn quality_key(&self) -> Option<(u8, u64, u64)> {
        let codec_priority = self.decoder_codec_priority()?;
        Some((codec_priority, self.bandwidth.unwrap_or(0), self.id))
    }

    fn decoder_codec_priority(&self) -> Option<u8> {
        let Some(codecs) = self.codecs.as_deref().filter(|s| !s.trim().is_empty()) else {
            // Standard AAC audio rendition IDs allow the decoder to inspect missing codec metadata.
            return matches!(self.id, 30216 | 30232 | 30280).then_some(0);
        };
        let mut best = None;
        for codec in codecs
            .split(',')
            .map(|codec| codec.trim().to_ascii_lowercase())
        {
            best = best.max(aac_codec_priority(codec.as_str()));
        }
        best
    }
}

impl BilibiliSegmentBase {
    fn initialization_range(&self) -> Option<PlaybackByteRange> {
        self.initialization_camel
            .as_deref()
            .or(self.initialization_snake.as_deref())
            .and_then(parse_playback_byte_range)
    }

    fn index_range(&self) -> Option<PlaybackByteRange> {
        self.index_range_camel
            .as_deref()
            .or(self.index_range_snake.as_deref())
            .and_then(parse_playback_byte_range)
    }
}

fn parse_playback_byte_range(value: &str) -> Option<PlaybackByteRange> {
    let (start, end) = value.trim().split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end_inclusive = end.parse::<u64>().ok()?;
    (start <= end_inclusive).then_some(PlaybackByteRange {
        start,
        end_inclusive,
    })
}

#[cfg(not(feature = "fdk-aac-decoder"))]
fn aac_codec_priority(codec: &str) -> Option<u8> {
    match codec {
        "mp4a.40.2" => Some(3),
        _ => None,
    }
}

#[cfg(feature = "fdk-aac-decoder")]
fn aac_codec_priority(codec: &str) -> Option<u8> {
    match codec {
        "mp4a.40.2" => Some(3),
        "mp4a.40.5" => Some(2),
        "mp4a.40.29" => Some(1),
        _ => None,
    }
}

impl BilibiliDurl {
    fn candidate_urls(self) -> Vec<String> {
        let mut urls = Vec::new();
        if !self.url.is_empty() {
            urls.push(self.url);
        }
        if let Some(backup_urls) = self.backup_url {
            urls.extend(backup_urls.into_iter().filter(|url| !url.is_empty()));
        }
        urls
    }
}

pub fn client() -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(BILIBILI_UA));
    headers.insert(ACCEPT, HeaderValue::from_static(BILIBILI_API_ACCEPT));
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert("dnt", HeaderValue::from_static("1"));
    headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(
        "sec-ch-ua",
        HeaderValue::from_static(
            "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"",
        ),
    );
    headers.insert("sec-ch-ua-mobile", HeaderValue::from_static("?0"));
    headers.insert("sec-ch-ua-platform", HeaderValue::from_static("\"macOS\""));
    headers.insert("origin", HeaderValue::from_static(BILIBILI_ORIGIN));
    headers.insert("sec-fetch-site", HeaderValue::from_static("same-site"));
    headers.insert("sec-fetch-mode", HeaderValue::from_static("cors"));
    headers.insert("sec-fetch-dest", HeaderValue::from_static("empty"));

    Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .default_headers(headers)
        .build()
        .context("failed to build bilibili http client")
}

pub async fn extract_bvid_from_text_or_short_link(
    client: &Client,
    text: &str,
) -> Result<Option<String>> {
    if let Some(bvid) = extract_bvid(text) {
        return Ok(Some(bvid));
    }

    for url in bilibili_short_urls(text).into_iter().take(3) {
        let response = client
            .get(url.as_str())
            .header(ACCEPT, BILIBILI_API_ACCEPT)
            .header(REFERER, BILIBILI_ORIGIN)
            .send()
            .await
            .with_context(|| format!("failed to resolve bilibili short link {url}"))?;

        if let Some(bvid) = extract_bvid(response.url().as_str()) {
            return Ok(Some(bvid));
        }

        if let Some(location) = response.headers().get(LOCATION).and_then(|value| {
            value
                .to_str()
                .ok()
                .and_then(|location| extract_bvid(location))
        }) {
            return Ok(Some(location));
        }
    }

    Ok(None)
}

pub fn extract_bvid(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    if bytes.len() < BVID_LEN {
        return None;
    }

    for start in 0..=bytes.len() - BVID_LEN {
        if !matches!(bytes[start], b'B' | b'b') || !matches!(bytes[start + 1], b'V' | b'v') {
            continue;
        }

        let end = start + BVID_LEN;
        if !bytes[start + 2..end]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric())
        {
            continue;
        }

        let before_ok = start == 0 || !is_bvid_boundary_byte(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_bvid_boundary_byte(bytes[end]);
        if !before_ok || !after_ok {
            continue;
        }

        let suffix = std::str::from_utf8(&bytes[start + 2..end]).ok()?;
        return Some(format!("BV{suffix}"));
    }

    None
}

pub async fn resolve_track(
    client: &Client,
    bvid: &str,
    part_index: usize,
) -> Result<PlaybackTrack> {
    let referer = video_referer(bvid);
    let video = bilibili_api_get(
        client,
        format!("{BILIBILI_API}/web-interface/view"),
        &referer,
    )
    .query(&[("bvid", bvid)])
    .send()
    .await?
    .read_result::<BilibiliVideoInfo>("video info")
    .await?;

    let page = video
        .pages
        .get(part_index)
        .ok_or_else(|| anyhow!("part {} does not exist", part_index + 1))?;

    let primary = resolve_player_info_wbi(client, bvid, page.cid).await;
    let media = select_with_dash_fallback(
        primary,
        resolve_player_info_legacy(client, bvid, page.cid, &referer),
    )
    .await?;

    Ok(PlaybackTrack {
        track_id: format!("bilibili:{bvid}:{}:{}", part_index + 1, page.cid),
        title: format!("{} - {}", video.title, page.part),
        source_kind: "bilibili".to_string(),
        bvid: bvid.to_string(),
        part: part_index + 1,
        duration_ms: page.duration.saturating_mul(1000),
        audio_url: media.url,
        referer,
        media_init_range: media.init_range,
        media_index_range: media.index_range,
        media_is_progressive: media.progressive,
    })
}

async fn select_with_dash_fallback(
    primary: Result<BilibiliPlayerInfo>,
    legacy: impl std::future::Future<Output = Result<BilibiliPlayerInfo>>,
) -> Result<MediaCandidate> {
    let primary = match primary {
        Ok(info) => select_media(info),
        Err(err) if is_http_precondition_failed(&err) => Err(err),
        Err(err) => return Err(err),
    };
    if primary.as_ref().is_ok_and(|m| !m.progressive) {
        return primary;
    }
    let alternate = legacy.await.and_then(select_media);
    match (primary, alternate) {
        (_, Ok(audio)) if !audio.progressive => Ok(audio),
        (Ok(media), _) => Ok(media),
        (Err(_), Ok(media)) => Ok(media),
        (Err(primary), Err(legacy)) => Err(legacy).context(format!(
            "no playable Bilibili stream after primary lookup: {primary:#}"
        )),
    }
}

async fn resolve_player_info_wbi(
    client: &Client,
    bvid: &str,
    cid: u64,
) -> Result<BilibiliPlayerInfo> {
    let nav = bilibili_api_get(
        client,
        format!("{BILIBILI_API}/web-interface/nav"),
        BILIBILI_HOME,
    )
    .send()
    .await?
    .read_result::<BilibiliNavInfo>("nav")
    .await?;

    let query = wbi_playurl_query(
        bvid,
        cid,
        chrono::Utc::now().timestamp(),
        &mixin_key(&nav.wbi_img)?,
    );

    bilibili_api_get(
        client,
        format!("{BILIBILI_API}/player/wbi/playurl?{query}"),
        BILIBILI_HOME,
    )
    .send()
    .await?
    .read_result::<BilibiliPlayerInfo>("play url")
    .await
}

async fn resolve_player_info_legacy(
    client: &Client,
    bvid: &str,
    cid: u64,
    referer: &str,
) -> Result<BilibiliPlayerInfo> {
    bilibili_api_get(client, format!("{BILIBILI_API}/player/playurl"), referer)
        .query(&[
            ("bvid", bvid.to_string()),
            ("cid", cid.to_string()),
            ("fnver", "0".to_string()),
            ("fnval", "16".to_string()),
            ("fourk", "1".to_string()),
            ("qn", "16".to_string()),
        ])
        .send()
        .await?
        .read_result::<BilibiliPlayerInfo>("legacy play url")
        .await
}

fn select_media(player: BilibiliPlayerInfo) -> Result<MediaCandidate> {
    media_candidates(player)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("bilibili media stream with supported audio does not exist"))
}

#[cfg(test)]
fn best_media_url(player: BilibiliPlayerInfo) -> Result<String> {
    select_media(player).map(|candidate| candidate.url)
}

fn media_candidates(player: BilibiliPlayerInfo) -> Result<Vec<MediaCandidate>> {
    if let Some(audio) = player.dash.and_then(|dash| dash.audio) {
        let mut candidates = audio
            .into_iter()
            .filter_map(|audio| {
                let base_url = audio.base_url()?;
                Some((
                    audio.quality_key()?,
                    MediaCandidate {
                        progressive: false,
                        url: base_url.to_string(),
                        init_range: audio
                            .segment_base()
                            .and_then(BilibiliSegmentBase::initialization_range),
                        index_range: audio
                            .segment_base()
                            .and_then(BilibiliSegmentBase::index_range),
                    },
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.0.cmp(&left.0));
        if !candidates.is_empty() {
            return Ok(candidates
                .into_iter()
                .map(|(_, candidate)| candidate)
                .collect());
        }
    }

    if let Some(durls) = player.durl {
        if durls.len() > 1 {
            bail!("segmented bilibili durl streams are not supported yet");
        }
        let mut candidates = durls
            .into_iter()
            .flat_map(BilibiliDurl::candidate_urls)
            .filter(|url| !url.is_empty())
            .map(|url| MediaCandidate {
                progressive: true,
                url,
                init_range: None,
                index_range: None,
            })
            .collect::<Vec<_>>();
        if !candidates.is_empty() {
            return Ok(std::mem::take(&mut candidates));
        }
    }

    bail!("bilibili media stream with supported audio does not exist")
}

trait BilibiliResponseExt {
    async fn read_result<T>(self, endpoint: &'static str) -> Result<T>
    where
        T: DeserializeOwned;
}

#[derive(Debug)]
struct BilibiliHttpStatusError {
    endpoint: &'static str,
    status: StatusCode,
    body_preview: String,
}

impl fmt::Display for BilibiliHttpStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "bilibili {} request failed with HTTP {}: {}",
            self.endpoint, self.status, self.body_preview
        )
    }
}

impl Error for BilibiliHttpStatusError {}

impl BilibiliResponseExt for reqwest::Response {
    async fn read_result<T>(self, endpoint: &'static str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let status = self.status();
        let body = self
            .text()
            .await
            .with_context(|| format!("failed to read bilibili {endpoint} response"))?;
        if !status.is_success() {
            return Err(BilibiliHttpStatusError {
                endpoint,
                status,
                body_preview: preview_body(&body),
            }
            .into());
        }

        parse_api_result(&body, endpoint)
    }
}

fn parse_api_result<T: DeserializeOwned>(body: &str, endpoint: &str) -> Result<T> {
    let result = serde_json::from_str::<BilibiliResult<T>>(body).with_context(|| {
        format!(
            "failed to decode bilibili {endpoint} response: {}",
            preview_body(body)
        )
    })?;

    // Anonymous nav responses still contain usable WBI keys with code -101.
    // Other API errors must not be accepted just because they include data.
    if result.code != 0 && !(endpoint == "nav" && result.code == -101) {
        bail!(
            "bilibili {endpoint} returned code {}: {}",
            result.code,
            result.message.as_deref().unwrap_or("unknown error")
        );
    }

    result
        .data
        .ok_or_else(|| anyhow!("bilibili {endpoint} returned no data"))
}

fn bilibili_api_get(
    client: &Client,
    url: impl reqwest::IntoUrl,
    referer: &str,
) -> reqwest::RequestBuilder {
    client
        .get(url)
        .header(ACCEPT, BILIBILI_API_ACCEPT)
        .header(REFERER, referer)
        .header("origin", BILIBILI_ORIGIN)
}

fn video_referer(bvid: &str) -> String {
    format!("https://www.bilibili.com/video/{bvid}/")
}

fn bilibili_short_urls(text: &str) -> Vec<Url> {
    text.split_whitespace()
        .filter_map(|token| {
            let token = token.trim_matches(is_url_trim_char);
            let start = token.find("https://").or_else(|| token.find("http://"))?;
            let candidate = &token[start..];
            let end = candidate
                .char_indices()
                .find_map(|(index, ch)| is_url_hard_stop_char(ch).then_some(index))
                .unwrap_or(candidate.len());
            let candidate = candidate[..end].trim_matches(is_url_trim_char);
            let url = Url::parse(candidate).ok()?;
            is_bilibili_short_url(&url).then_some(url)
        })
        .collect()
}

fn is_bilibili_short_url(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    BILIBILI_SHORT_LINK_HOSTS
        .iter()
        .any(|candidate| host.eq_ignore_ascii_case(candidate))
}

fn is_url_trim_char(ch: char) -> bool {
    matches!(
        ch,
        '<' | '>'
            | '"'
            | '\''
            | '('
            | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | ','
            | '.'
            | ';'
            | ':'
            | '!'
            | '?'
            | '，'
            | '。'
            | '、'
            | '；'
            | '：'
            | '！'
            | '？'
            | '“'
            | '”'
            | '‘'
            | '’'
            | '【'
            | '】'
    )
}

fn is_url_hard_stop_char(ch: char) -> bool {
    matches!(
        ch,
        '<' | '>'
            | '"'
            | '\''
            | '('
            | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | ','
            | ';'
            | '!'
            | '，'
            | '。'
            | '、'
            | '；'
            | '！'
            | '？'
            | '“'
            | '”'
            | '‘'
            | '’'
            | '【'
            | '】'
    )
}

fn is_bvid_boundary_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn is_http_precondition_failed(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<BilibiliHttpStatusError>()
            .is_some_and(|error| error.status == StatusCode::PRECONDITION_FAILED)
    })
}

fn preview_body(body: &str) -> String {
    let mut preview = body
        .replace(['\r', '\n'], " ")
        .chars()
        .take(240)
        .collect::<String>();

    if body.chars().count() > 240 {
        preview.push_str("...");
    }

    preview
}

fn mixin_key(info: &BilibiliWbiInfo) -> Result<String> {
    let img_key = url_stem(&info.img_url)?;
    let sub_key = url_stem(&info.sub_url)?;
    let raw = format!("{img_key}{sub_key}");

    let mixed = MIXIN_KEY_ENC_TAB
        .iter()
        .filter_map(|index| raw.as_bytes().get(*index).copied())
        .map(char::from)
        .collect::<String>();

    if mixed.len() < 32 {
        bail!("invalid bilibili mixin key");
    }

    Ok(mixed[..32].to_string())
}

fn url_stem(url: &str) -> Result<String> {
    let file = url
        .rsplit('/')
        .next()
        .ok_or_else(|| anyhow!("invalid bilibili key url"))?;
    Ok(file.split('.').next().unwrap_or(file).to_string())
}

fn fingerprint_token(min_length: usize, max_length: usize) -> String {
    let raw: Vec<u8> = (0..rand::random_range(min_length..=max_length))
        .map(|_| FINGERPRINT_ALPHABET[rand::random_range(0..FINGERPRINT_ALPHABET.len())])
        .collect();
    encode_fingerprint_token(&raw)
}

fn encode_fingerprint_token(raw: &[u8]) -> String {
    let mut encoded = BASE64_STANDARD.encode(raw);
    // Match MusicPlugin.kt's dropLast(2), including when the output has no padding.
    encoded.truncate(encoded.len().saturating_sub(2));
    encoded
}

fn fingerprint_params() -> [(&'static str, String); 4] {
    // The reference keeps the screen size stable for the lifetime of the client.
    static SCREEN_SIZE: OnceLock<(u32, u32)> = OnceLock::new();
    let (width, height) = *SCREEN_SIZE.get_or_init(|| {
        FINGERPRINT_SCREEN_SIZES[rand::random_range(0..FINGERPRINT_SCREEN_SIZES.len())]
    });
    let screen_noise = rand::random_range(0..114);
    let scroll_top = rand::random_range(0..101);
    let offset_noise = rand::random_range(0..514);
    let interaction = format!(
        "{{\"ds\":[],\"wh\":[{},{},{}],\"of\":[{},{},{}]}}",
        2 * width + 2 * height + 3 * screen_noise,
        4 * width - height + screen_noise,
        screen_noise,
        3 * scroll_top + offset_noise,
        4 * scroll_top + 2 * offset_noise,
        offset_noise,
    );
    [
        ("dm_img_list", "[]".to_string()),
        ("dm_img_str", fingerprint_token(16, 64)),
        ("dm_cover_img_str", fingerprint_token(32, 128)),
        ("dm_img_inter", interaction),
    ]
}

fn wbi_playurl_query(bvid: &str, cid: u64, wts: i64, mixin_key: &str) -> String {
    let params = [
        ("bvid", bvid.to_string()),
        ("cid", cid.to_string()),
        ("fnval", "4048".to_string()),
        ("try_look", "1".to_string()),
        ("web_location", "1550101".to_string()),
        ("wts", wts.to_string()),
    ];
    signed_query(params.into_iter().chain(fingerprint_params()), mixin_key)
}

fn signed_query<'a>(
    params: impl IntoIterator<Item = (&'a str, String)>,
    mixin_key: &str,
) -> String {
    let mut params: Vec<_> = params
        .into_iter()
        .map(|(key, mut value)| {
            value.retain(|ch| !matches!(ch, '!' | '\'' | '(' | ')' | '*'));
            (key, value)
        })
        .collect();
    params.sort_by_key(|(key, _)| *key);

    // Java URLEncoder parity: preserve underscores, encode spaces as '+', and
    // sign exactly the same escaped bytes that are sent in the URL.
    let query = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish();

    let w_rid = format!("{:x}", md5::compute(format!("{query}{mixin_key}")));
    format!("{query}&w_rid={w_rid}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_bvid_finds_video_ids_in_links_and_share_text() {
        assert_eq!(
            extract_bvid("https://www.bilibili.com/video/BV196Ex61ES1/?share_source=copy_link")
                .as_deref(),
            Some("BV196Ex61ES1")
        );
        assert_eq!(
            extract_bvid("复制这条消息 bv196Ex61ES1 打开哔哩哔哩").as_deref(),
            Some("BV196Ex61ES1")
        );
        assert_eq!(extract_bvid("prefixBV196Ex61ES1").as_deref(), None);
        assert_eq!(extract_bvid("BV196Ex61ES10").as_deref(), None);
    }

    #[test]
    fn bilibili_short_urls_extracts_known_short_link_hosts() {
        let urls = bilibili_short_urls(
            "复制链接 https://b23.tv/abc123，更多文本 https://example.test/BV196Ex61ES1",
        );
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].as_str(), "https://b23.tv/abc123");

        let urls = bilibili_short_urls("【https://bili2233.cn/xyz987】");
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].host_str(), Some("bili2233.cn"));
    }

    #[test]
    fn url_stem_extracts_last_path_stem() {
        assert_eq!(
            url_stem("https://i0.hdslb.com/bfs/wbi/abc123.png").unwrap(),
            "abc123"
        );
        assert_eq!(url_stem("plainfile").unwrap(), "plainfile");
    }

    #[test]
    fn mixin_key_is_deterministic_and_rejects_short_keys() {
        let info = BilibiliWbiInfo {
            img_url: "https://example.test/abcdefghijklmnopqrstuvwxyzABCDEF.png".to_string(),
            sub_url: "https://example.test/GHIJKLMNOPQRSTUVWXYZ0123456789.png".to_string(),
        };

        assert_eq!(
            mixin_key(&info).unwrap(),
            "UVsc1ixGpYkF6dTJBRfXHjQtDCoNmMPn"
        );

        let short = BilibiliWbiInfo {
            img_url: "https://example.test/a.png".to_string(),
            sub_url: "https://example.test/b.png".to_string(),
        };
        assert!(mixin_key(&short).is_err());
    }

    #[test]
    fn signed_query_sorts_and_form_encodes_params() {
        let mixin = "0123456789abcdef0123456789abcdef";
        let query = signed_query(
            [("b", "two words".to_string()), ("a", "x/y".to_string())],
            mixin,
        );
        let expected_prefix = "a=x%2Fy&b=two+words";
        let expected_rid = format!("{:x}", md5::compute(format!("{expected_prefix}{mixin}")));

        assert_eq!(query, format!("{expected_prefix}&w_rid={expected_rid}"));
        assert_eq!(
            query,
            signed_query(
                [("a", "x/y".to_string()), ("b", "two words".to_string()),],
                mixin,
            )
        );
    }

    #[test]
    fn signed_query_matches_reference_escaping_and_filtering() {
        let query = signed_query(
            [
                ("wts", "1702204169".to_string()),
                ("foo_bar", "汉字 ~.-_ test".to_string()),
                ("dm_img_str", "ab+/=!'()*".to_string()),
            ],
            "0123456789abcdef0123456789abcdef",
        );
        assert_eq!(
            query,
            "dm_img_str=ab%2B%2F%3D&foo_bar=%E6%B1%89%E5%AD%97+%7E.-_+test&wts=1702204169&w_rid=3b6685a1a2e391e296b50f83395bcea3"
        );
    }

    #[test]
    fn fingerprint_tokens_drop_two_base64_characters_even_without_padding() {
        for (raw, expected) in [
            ("0123456789abcdef", "MDEyMzQ1Njc4OWFiY2RlZg"),
            ("0123456789abcdefg", "MDEyMzQ1Njc4OWFiY2RlZm"),
            ("0123456789abcdefgh", "MDEyMzQ1Njc4OWFiY2RlZm"),
        ] {
            assert_eq!(encode_fingerprint_token(raw.as_bytes()), expected);
        }
    }

    #[test]
    fn playurl_query_signs_fingerprints_and_keeps_screen_size_stable() {
        let mixin = "0123456789abcdef0123456789abcdef";
        let mut previous_screen = None;
        for _ in 0..32 {
            let query = wbi_playurl_query("BV196Ex61ES1", 12345, 1702204169, mixin);
            let (unsigned, signature) = query.rsplit_once("&w_rid=").unwrap();
            assert_eq!(
                signature,
                format!("{:x}", md5::compute(format!("{unsigned}{mixin}")))
            );
            let params: std::collections::BTreeMap<_, _> = form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect();
            assert_eq!(params.len(), 11);
            assert_eq!(params["bvid"], "BV196Ex61ES1");
            assert_eq!(params["cid"], "12345");
            assert_eq!(params["wts"], "1702204169");
            assert_eq!(params["fnval"], "4048");
            assert_eq!(params["try_look"], "1");
            assert_eq!(params["web_location"], "1550101");
            assert_eq!(params["dm_img_list"], "[]");
            for (key, min, max) in [("dm_img_str", 22, 86), ("dm_cover_img_str", 42, 170)] {
                let token = &params[key];
                assert!((min..=max).contains(&token.len()));
                assert!(
                    token
                        .bytes()
                        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'+' | b'/'))
                );
            }

            let interaction: serde_json::Value =
                serde_json::from_str(&params["dm_img_inter"]).unwrap();
            assert_eq!(interaction["ds"], serde_json::json!([]));
            let wh: [u32; 3] = serde_json::from_value(interaction["wh"].clone()).unwrap();
            let offset: [u32; 3] = serde_json::from_value(interaction["of"].clone()).unwrap();
            assert!(wh[2] < 114);
            let doubled_dimensions = wh[0] - 3 * wh[2];
            let skewed_dimensions = wh[1] - wh[2];
            assert_eq!((doubled_dimensions + 2 * skewed_dimensions) % 10, 0);
            let width = (doubled_dimensions + 2 * skewed_dimensions) / 10;
            let height = 4 * width - skewed_dimensions;
            assert!(FINGERPRINT_SCREEN_SIZES.contains(&(width, height)));
            if let Some(previous) = previous_screen {
                assert_eq!(previous, (width, height));
            }
            previous_screen = Some((width, height));
            assert!(offset[2] < 514);
            assert_eq!((offset[0] - offset[2]) % 3, 0);
            let scroll_top = (offset[0] - offset[2]) / 3;
            assert!(scroll_top <= 100);
            assert_eq!(offset[1], 4 * scroll_top + 2 * offset[2]);
        }
    }

    #[test]
    fn api_errors_with_data_are_rejected_except_anonymous_nav() {
        let nav = r#"{"code":-101,"message":"not logged in","data":{"wbi_img":{"img_url":"https://example.test/img.png","sub_url":"https://example.test/sub.png"}}}"#;
        assert!(parse_api_result::<BilibiliNavInfo>(nav, "nav").is_ok());
        assert!(parse_api_result::<serde_json::Value>(nav, "play url").is_err());
        for endpoint in ["nav", "video info", "play url", "legacy play url"] {
            let err = parse_api_result::<serde_json::Value>(
                r#"{"code":-352,"message":"risk control","data":{}}"#,
                endpoint,
            )
            .unwrap_err();
            assert!(err.to_string().contains("returned code -352: risk control"));
        }
        assert!(
            parse_api_result::<BilibiliNavInfo>(r#"{"code":-101,"data":null}"#, "nav").is_err()
        );
        assert!(
            parse_api_result::<serde_json::Value>(r#"{"code":0,"data":null}"#, "play url").is_err()
        );
        assert!(
            parse_api_result::<serde_json::Value>(r#"{"code":0,"data":{}}"#, "play url").is_ok()
        );
    }

    #[tokio::test]
    async fn api_request_sends_browser_headers_and_exact_signed_query() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            time::Duration,
        };

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 2048];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        let query = wbi_playurl_query("BV196Ex61ES1", 12345, 1702204169, "test-mixin-key");
        let response = bilibili_api_get(
            &client().unwrap(),
            format!("http://{address}/x/player/wbi/playurl?{query}"),
            BILIBILI_HOME,
        )
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
        assert!(response.status().is_success());
        let request = server.join().unwrap();
        assert!(request.starts_with(&format!("GET /x/player/wbi/playurl?{query} HTTP/1.1\r\n")));
        let headers: std::collections::BTreeMap<_, _> = request
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(":"))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim()))
            .collect();
        assert_eq!(headers["user-agent"], BILIBILI_UA);
        assert_eq!(headers["accept"], BILIBILI_API_ACCEPT);
        assert_eq!(headers["referer"], BILIBILI_HOME);
        assert_eq!(headers["origin"], BILIBILI_ORIGIN);
        assert_eq!(headers["dnt"], "1");
        assert_eq!(headers["sec-ch-ua-mobile"], "?0");
        assert_eq!(headers["sec-ch-ua-platform"], "\"macOS\"");
        assert!(headers["sec-ch-ua"].contains("\"Chromium\";v=\"121\""));
    }

    #[test]
    fn video_referer_uses_browser_video_page() {
        assert_eq!(
            video_referer("BV1xx411c7mD"),
            "https://www.bilibili.com/video/BV1xx411c7mD/"
        );
    }

    #[test]
    fn playback_byte_range_parser_accepts_bilibili_segment_base_ranges() {
        assert_eq!(
            parse_playback_byte_range("0-817"),
            Some(PlaybackByteRange {
                start: 0,
                end_inclusive: 817
            })
        );
        assert_eq!(parse_playback_byte_range("818-24417").unwrap().start, 818);
        assert_eq!(parse_playback_byte_range("24417-818"), None);
        assert_eq!(parse_playback_byte_range("not-a-range"), None);
    }

    #[test]
    fn http_412_errors_are_detected_for_fallback() {
        let error: anyhow::Error = BilibiliHttpStatusError {
            endpoint: "play url",
            status: StatusCode::PRECONDITION_FAILED,
            body_preview: "blocked".to_string(),
        }
        .into();

        assert!(is_http_precondition_failed(&error));
    }

    #[test]
    fn best_media_url_prefers_dash_audio() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![
                    BilibiliAudio {
                        id: 30216,
                        bandwidth: Some(64),
                        codecs: Some("mp4a.40.2".to_string()),
                        base_url_camel: Some("https://example.test/low.m4s".to_string()),
                        base_url_snake: None,
                        backup_url_camel: None,
                        backup_url_snake: None,
                        segment_base_camel: None,
                        segment_base_snake: None,
                    },
                    BilibiliAudio {
                        id: 30280,
                        bandwidth: Some(128),
                        codecs: Some("mp4a.40.2".to_string()),
                        base_url_camel: None,
                        base_url_snake: Some("https://example.test/high.m4s".to_string()),
                        backup_url_camel: None,
                        backup_url_snake: None,
                        segment_base_camel: None,
                        segment_base_snake: None,
                    },
                ]),
            }),
            durl: Some(vec![BilibiliDurl {
                url: "https://example.test/fallback.mp4".to_string(),
                backup_url: None,
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/high.m4s"
        );
    }

    #[test]
    fn best_media_preserves_dash_segment_base_ranges() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![BilibiliAudio {
                    id: 30280,
                    bandwidth: Some(128),
                    codecs: Some("mp4a.40.2".to_string()),
                    base_url_camel: Some("https://example.test/audio.m4s".to_string()),
                    base_url_snake: None,
                    backup_url_camel: None,
                    backup_url_snake: None,
                    segment_base_camel: Some(BilibiliSegmentBase {
                        initialization_camel: Some("0-817".to_string()),
                        initialization_snake: None,
                        index_range_camel: Some("818-24417".to_string()),
                        index_range_snake: None,
                    }),
                    segment_base_snake: None,
                }]),
            }),
            durl: None,
        };

        let media = select_media(player).unwrap();
        assert_eq!(media.url, "https://example.test/audio.m4s");
        assert_eq!(
            media.init_range,
            Some(PlaybackByteRange {
                start: 0,
                end_inclusive: 817
            })
        );
        assert_eq!(
            media.index_range,
            Some(PlaybackByteRange {
                start: 818,
                end_inclusive: 24417
            })
        );
    }

    #[test]
    fn best_media_url_prefers_decoder_supported_aac_lc() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![
                    BilibiliAudio {
                        id: 30280,
                        bandwidth: Some(192),
                        codecs: Some("mp4a.40.5".to_string()),
                        base_url_camel: Some("https://example.test/he-aac.m4s".to_string()),
                        base_url_snake: None,
                        backup_url_camel: None,
                        backup_url_snake: None,
                        segment_base_camel: None,
                        segment_base_snake: None,
                    },
                    BilibiliAudio {
                        id: 30232,
                        bandwidth: Some(132),
                        codecs: Some("mp4a.40.2".to_string()),
                        base_url_camel: Some("https://example.test/aac-lc.m4s".to_string()),
                        base_url_snake: None,
                        backup_url_camel: None,
                        backup_url_snake: None,
                        segment_base_camel: None,
                        segment_base_snake: None,
                    },
                ]),
            }),
            durl: None,
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/aac-lc.m4s"
        );
    }

    #[test]
    fn best_media_url_uses_single_durl_when_dash_audio_is_missing() {
        let player = BilibiliPlayerInfo {
            dash: None,
            durl: Some(vec![BilibiliDurl {
                url: "https://example.test/video.mp4".to_string(),
                backup_url: Some(vec!["https://example.test/backup.mp4".to_string()]),
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/video.mp4"
        );
    }

    #[test]
    fn best_media_url_falls_back_to_durl_when_dash_codec_is_unsupported() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![BilibiliAudio {
                    id: 30280,
                    bandwidth: Some(192),
                    codecs: Some("fLaC".to_string()),
                    base_url_camel: Some("https://example.test/flac.m4s".to_string()),
                    base_url_snake: None,
                    backup_url_camel: None,
                    backup_url_snake: None,
                    segment_base_camel: None,
                    segment_base_snake: None,
                }]),
            }),
            durl: Some(vec![BilibiliDurl {
                url: "https://example.test/video.mp4".to_string(),
                backup_url: None,
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/video.mp4"
        );
    }

    #[cfg(feature = "fdk-aac-decoder")]
    #[test]
    fn best_media_url_accepts_he_aac_with_fdk_decoder() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![BilibiliAudio {
                    id: 30280,
                    bandwidth: Some(192),
                    codecs: Some("mp4a.40.5".to_string()),
                    base_url_camel: Some("https://example.test/he-aac.m4s".to_string()),
                    base_url_snake: None,
                    backup_url_camel: None,
                    backup_url_snake: None,
                    segment_base_camel: None,
                    segment_base_snake: None,
                }]),
            }),
            durl: Some(vec![BilibiliDurl {
                url: "https://example.test/fallback.mp4".to_string(),
                backup_url: None,
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/he-aac.m4s"
        );
    }

    #[test]
    fn standard_audio_rendition_without_codec_metadata_precedes_muxed_video() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![BilibiliAudio {
                    id: 30280,
                    bandwidth: Some(192),
                    codecs: None,
                    base_url_camel: Some("https://example.test/unknown-aac.m4s".to_string()),
                    base_url_snake: None,
                    backup_url_camel: None,
                    backup_url_snake: None,
                    segment_base_camel: None,
                    segment_base_snake: None,
                }]),
            }),
            durl: Some(vec![BilibiliDurl {
                url: "https://example.test/video.mp4".to_string(),
                backup_url: None,
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/unknown-aac.m4s"
        );
    }

    #[tokio::test]
    async fn best_media_url_falls_back_when_primary_has_no_media() {
        let primary = BilibiliPlayerInfo {
            dash: Some(BilibiliDash { audio: None }),
            durl: None,
        };
        let fallback = BilibiliPlayerInfo {
            dash: None,
            durl: Some(vec![BilibiliDurl {
                url: String::new(),
                backup_url: Some(vec!["https://example.test/backup.mp4".to_string()]),
            }]),
        };

        assert_eq!(
            select_with_dash_fallback(Ok(primary), async { Ok(fallback) })
                .await
                .unwrap()
                .url,
            "https://example.test/backup.mp4"
        );
    }

    #[tokio::test]
    async fn primary_muxed_video_yields_to_legacy_dash_with_backup_url() {
        let primary =
            serde_json::from_value(serde_json::json!({"durl":[{"url":"muxed"}]})).unwrap();
        let legacy = serde_json::from_value(serde_json::json!({"dash":{"audio":[{"id":30280,"codecs":"mp4a.40.2","baseUrl":"","backup_url":null,"backupUrl":["audio"]}]}})).unwrap();
        let media = select_with_dash_fallback(Ok(primary), async { Ok(legacy) })
            .await
            .unwrap();
        assert!(!media.progressive);
        assert_eq!(media.url, "audio");
    }

    #[test]
    fn best_media_url_rejects_segmented_durl() {
        let player = BilibiliPlayerInfo {
            dash: None,
            durl: Some(vec![
                BilibiliDurl {
                    url: "https://example.test/part1.mp4".to_string(),
                    backup_url: None,
                },
                BilibiliDurl {
                    url: "https://example.test/part2.mp4".to_string(),
                    backup_url: None,
                },
            ]),
        };

        assert!(best_media_url(player).is_err());
    }
}
