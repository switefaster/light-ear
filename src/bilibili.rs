use std::{error::Error, fmt};

use anyhow::{Context, Result, anyhow, bail};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{
    Client, StatusCode, Url,
    header::{
        ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, HeaderMap, HeaderValue, LOCATION, PRAGMA, REFERER,
        USER_AGENT,
    },
};
use serde::{Deserialize, de::DeserializeOwned};

use crate::core::PlaybackTrack;

const MIXIN_KEY_ENC_TAB: [usize; 64] = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29,
    28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25,
    54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

const BILIBILI_API: &str = "https://api.bilibili.com/x";
const BILIBILI_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";
const BILIBILI_ORIGIN: &str = "https://www.bilibili.com";
const BILIBILI_API_ACCEPT: &str = "application/json, text/plain, */*";
const BILIBILI_MEDIA_ACCEPT: &str = "*/*";
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
}

#[derive(Debug, Deserialize)]
struct BilibiliDurl {
    order: u64,
    size: Option<u64>,
    url: String,
    backup_url: Option<Vec<String>>,
}

impl BilibiliAudio {
    fn base_url(&self) -> Option<&str> {
        self.base_url_camel
            .as_deref()
            .or(self.base_url_snake.as_deref())
    }

    fn quality_key(&self) -> Option<(u8, u64, u64)> {
        let codec_priority = self.decoder_codec_priority()?;
        Some((codec_priority, self.bandwidth.unwrap_or(0), self.id))
    }

    fn decoder_codec_priority(&self) -> Option<u8> {
        let codecs = self.codecs.as_deref()?;
        if codecs
            .split(',')
            .map(|codec| codec.trim().to_ascii_lowercase())
            .any(|codec| codec == "mp4a.40.2")
        {
            return Some(2);
        }

        None
    }
}

impl BilibiliDurl {
    fn quality_key(&self) -> (u64, u64) {
        (self.size.unwrap_or(0), u64::MAX.saturating_sub(self.order))
    }
}

pub fn client() -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(BILIBILI_UA));
    headers.insert(ACCEPT, HeaderValue::from_static(BILIBILI_API_ACCEPT));
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert("origin", HeaderValue::from_static(BILIBILI_ORIGIN));
    headers.insert("sec-fetch-site", HeaderValue::from_static("same-site"));
    headers.insert("sec-fetch-mode", HeaderValue::from_static("cors"));
    headers.insert("sec-fetch-dest", HeaderValue::from_static("empty"));

    Client::builder()
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

    let player = resolve_player_info(client, bvid, page.cid, &referer).await?;
    let media_url = match best_media_url(player) {
        Ok(url) => url,
        Err(err) => {
            let legacy = resolve_player_info_legacy(client, bvid, page.cid, &referer)
                .await
                .with_context(|| {
                    format!("play url had no usable media and legacy fallback failed: {err:#}")
                })?;
            best_media_url(legacy).with_context(|| {
                format!("legacy play url had no usable media after WBI media failure: {err:#}")
            })?
        }
    };

    Ok(PlaybackTrack {
        track_id: format!("bilibili:{bvid}:{}:{}", part_index + 1, page.cid),
        title: format!("{} - {}", video.title, page.part),
        source_kind: "bilibili".to_string(),
        bvid: bvid.to_string(),
        part: part_index + 1,
        duration_ms: page.duration.saturating_mul(1000),
        audio_url: media_url,
        referer,
    })
}

async fn resolve_player_info(
    client: &Client,
    bvid: &str,
    cid: u64,
    referer: &str,
) -> Result<BilibiliPlayerInfo> {
    match resolve_player_info_wbi(client, bvid, cid, referer).await {
        Ok(player) => Ok(player),
        Err(err) if is_http_precondition_failed(&err) => {
            resolve_player_info_legacy(client, bvid, cid, referer)
                .await
                .with_context(|| {
                    format!(
                        "WBI playurl returned HTTP 412 and legacy playurl fallback failed: {err:#}"
                    )
                })
        }
        Err(err) => Err(err),
    }
}

async fn resolve_player_info_wbi(
    client: &Client,
    bvid: &str,
    cid: u64,
    referer: &str,
) -> Result<BilibiliPlayerInfo> {
    let nav = bilibili_api_get(client, format!("{BILIBILI_API}/web-interface/nav"), referer)
        .send()
        .await?
        .read_result::<BilibiliNavInfo>("nav")
        .await?;

    let wts = chrono::Local::now().timestamp();
    let query = signed_query(
        [
            ("bvid", bvid.to_string()),
            ("cid", cid.to_string()),
            ("fnver", "0".to_string()),
            ("fnval", "16".to_string()),
            ("fourk", "1".to_string()),
            ("platform", "html5".to_string()),
            ("wts", wts.to_string()),
        ],
        &mixin_key(&nav.wbi_img)?,
    );

    bilibili_api_get(
        client,
        format!("{BILIBILI_API}/player/wbi/playurl?{query}"),
        referer,
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
            ("platform", "html5".to_string()),
            ("qn", "64".to_string()),
        ])
        .send()
        .await?
        .read_result::<BilibiliPlayerInfo>("legacy play url")
        .await
}

fn best_media_url(player: BilibiliPlayerInfo) -> Result<String> {
    if let Some(audio) = player.dash.and_then(|dash| dash.audio) {
        if let Some((_, url)) = audio
            .into_iter()
            .filter_map(|audio| {
                let base_url = audio.base_url()?;
                Some((audio.quality_key()?, base_url.to_string()))
            })
            .max_by_key(|(quality, _)| *quality)
        {
            return Ok(url);
        }
    }

    if let Some(durls) = player.durl {
        if durls.len() > 1 {
            bail!("segmented bilibili durl streams are not supported yet");
        }
        if let Some((_, url)) = durls
            .into_iter()
            .map(|durl| {
                let quality = durl.quality_key();
                let url = if durl.url.is_empty() {
                    durl.backup_url
                        .and_then(|mut backup_urls| backup_urls.pop())
                        .unwrap_or_default()
                } else {
                    durl.url
                };
                (quality, url)
            })
            .filter(|(_, url)| !url.is_empty())
            .max_by_key(|(quality, _)| *quality)
        {
            return Ok(url);
        }
    }

    bail!("bilibili media stream with decoder-supported audio does not exist")
}

#[cfg(test)]
fn best_media_url_with_fallback(
    primary: BilibiliPlayerInfo,
    fallback: impl FnOnce() -> Result<BilibiliPlayerInfo>,
) -> Result<String> {
    match best_media_url(primary) {
        Ok(url) => Ok(url),
        Err(err) => best_media_url(fallback()?).with_context(|| {
            format!("fallback play url had no usable media after primary media failure: {err:#}")
        }),
    }
}

pub async fn download_audio(client: &Client, track: &PlaybackTrack) -> Result<Vec<u8>> {
    let response = client
        .get(&track.audio_url)
        .header(ACCEPT, BILIBILI_MEDIA_ACCEPT)
        .header(REFERER, track.referer.as_str())
        .header("origin", BILIBILI_ORIGIN)
        .header("range", "bytes=0-")
        .send()
        .await?
        .error_for_status()?;

    Ok(response.bytes().await?.to_vec())
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

        let result = serde_json::from_str::<BilibiliResult<T>>(&body).with_context(|| {
            format!(
                "failed to decode bilibili {endpoint} response: {}",
                preview_body(&body)
            )
        })?;

        if let Some(data) = result.data {
            return Ok(data);
        }

        if result.code != 0 {
            bail!(
                "bilibili {endpoint} returned code {}: {}",
                result.code,
                result.message.as_deref().unwrap_or("unknown error")
            );
        }

        bail!("bilibili {endpoint} returned no data")
    }
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

fn signed_query<const N: usize>(params: [(&str, String); N], mixin_key: &str) -> String {
    let mut params = params;
    params.sort_by_key(|(key, _)| *key);

    let query = params
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                utf8_percent_encode(key, NON_ALPHANUMERIC),
                utf8_percent_encode(value, NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

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
    fn signed_query_sorts_and_percent_encodes_params() {
        let mixin = "0123456789abcdef0123456789abcdef";
        let query = signed_query(
            [("b", "two words".to_string()), ("a", "x/y".to_string())],
            mixin,
        );
        let expected_prefix = "a=x%2Fy&b=two%20words";
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
    fn video_referer_uses_browser_video_page() {
        assert_eq!(
            video_referer("BV1xx411c7mD"),
            "https://www.bilibili.com/video/BV1xx411c7mD/"
        );
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
                    },
                    BilibiliAudio {
                        id: 30280,
                        bandwidth: Some(128),
                        codecs: Some("mp4a.40.2".to_string()),
                        base_url_camel: None,
                        base_url_snake: Some("https://example.test/high.m4s".to_string()),
                    },
                ]),
            }),
            durl: Some(vec![BilibiliDurl {
                order: 1,
                size: Some(10_000),
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
                    },
                    BilibiliAudio {
                        id: 30232,
                        bandwidth: Some(132),
                        codecs: Some("mp4a.40.2".to_string()),
                        base_url_camel: Some("https://example.test/aac-lc.m4s".to_string()),
                        base_url_snake: None,
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
                order: 1,
                size: Some(5_880_463),
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
                }]),
            }),
            durl: Some(vec![BilibiliDurl {
                order: 1,
                size: Some(5_880_463),
                url: "https://example.test/video.mp4".to_string(),
                backup_url: None,
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/video.mp4"
        );
    }

    #[test]
    fn best_media_url_falls_back_to_durl_when_dash_codec_is_unknown() {
        let player = BilibiliPlayerInfo {
            dash: Some(BilibiliDash {
                audio: Some(vec![BilibiliAudio {
                    id: 30280,
                    bandwidth: Some(192),
                    codecs: None,
                    base_url_camel: Some("https://example.test/unknown-aac.m4s".to_string()),
                    base_url_snake: None,
                }]),
            }),
            durl: Some(vec![BilibiliDurl {
                order: 1,
                size: Some(5_880_463),
                url: "https://example.test/video.mp4".to_string(),
                backup_url: None,
            }]),
        };

        assert_eq!(
            best_media_url(player).unwrap(),
            "https://example.test/video.mp4"
        );
    }

    #[test]
    fn best_media_url_falls_back_when_primary_has_no_media() {
        let primary = BilibiliPlayerInfo {
            dash: Some(BilibiliDash { audio: None }),
            durl: None,
        };
        let fallback = BilibiliPlayerInfo {
            dash: None,
            durl: Some(vec![BilibiliDurl {
                order: 1,
                size: Some(5_880_463),
                url: String::new(),
                backup_url: Some(vec!["https://example.test/backup.mp4".to_string()]),
            }]),
        };

        assert_eq!(
            best_media_url_with_fallback(primary, || Ok(fallback)).unwrap(),
            "https://example.test/backup.mp4"
        );
    }

    #[test]
    fn best_media_url_rejects_segmented_durl() {
        let player = BilibiliPlayerInfo {
            dash: None,
            durl: Some(vec![
                BilibiliDurl {
                    order: 1,
                    size: Some(1),
                    url: "https://example.test/part1.mp4".to_string(),
                    backup_url: None,
                },
                BilibiliDurl {
                    order: 2,
                    size: Some(1),
                    url: "https://example.test/part2.mp4".to_string(),
                    backup_url: None,
                },
            ]),
        };

        assert!(best_media_url(player).is_err());
    }
}
