use anyhow::{Context, Result, anyhow, bail};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{
    Client,
    header::{HeaderMap, HeaderValue, REFERER, USER_AGENT},
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
}

#[derive(Debug, Deserialize)]
struct BilibiliDash {
    audio: Option<Vec<BilibiliAudio>>,
}

#[derive(Debug, Deserialize)]
struct BilibiliAudio {
    id: u64,
    bandwidth: Option<u64>,
    #[serde(rename = "baseUrl")]
    base_url_camel: Option<String>,
    #[serde(rename = "base_url")]
    base_url_snake: Option<String>,
}

impl BilibiliAudio {
    fn base_url(&self) -> Option<&str> {
        self.base_url_camel
            .as_deref()
            .or(self.base_url_snake.as_deref())
    }

    fn quality_key(&self) -> (u64, u64) {
        (self.bandwidth.unwrap_or(0), self.id)
    }
}

pub fn client() -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(BILIBILI_UA));
    headers.insert(
        "accept-language",
        HeaderValue::from_static("zh-CN,zh;q=0.9"),
    );
    headers.insert("cache-control", HeaderValue::from_static("no-cache"));
    headers.insert("pragma", HeaderValue::from_static("no-cache"));

    Client::builder()
        .default_headers(headers)
        .build()
        .context("failed to build bilibili http client")
}

pub async fn resolve_track(
    client: &Client,
    bvid: &str,
    part_index: usize,
) -> Result<PlaybackTrack> {
    let referer = format!("https://www.bilibili.com/video/{bvid}");
    let video = client
        .get(format!("{BILIBILI_API}/web-interface/view"))
        .query(&[("bvid", bvid)])
        .header(REFERER, referer.as_str())
        .send()
        .await?
        .read_result::<BilibiliVideoInfo>("video info")
        .await?;

    let page = video
        .pages
        .get(part_index)
        .ok_or_else(|| anyhow!("part {} does not exist", part_index + 1))?;

    let nav = client
        .get(format!("{BILIBILI_API}/web-interface/nav"))
        .header(REFERER, "https://www.bilibili.com/")
        .send()
        .await?
        .read_result::<BilibiliNavInfo>("nav")
        .await?;

    let wts = chrono::Local::now().timestamp();
    let query = signed_query(
        [
            ("bvid", bvid.to_string()),
            ("cid", page.cid.to_string()),
            ("fnval", "16".to_string()),
            ("wts", wts.to_string()),
        ],
        &mixin_key(&nav.wbi_img)?,
    );

    let player = client
        .get(format!("{BILIBILI_API}/player/wbi/playurl?{query}"))
        .header(REFERER, referer.as_str())
        .send()
        .await?
        .read_result::<BilibiliPlayerInfo>("play url")
        .await?;

    let audio = player
        .dash
        .ok_or_else(|| anyhow!("dash audio does not exist"))?
        .audio
        .ok_or_else(|| anyhow!("dash audio does not exist"))?
        .into_iter()
        .filter_map(|audio| {
            let base_url = audio.base_url()?;
            Some((audio.quality_key(), base_url.to_string()))
        })
        .max_by_key(|(quality, _)| *quality)
        .ok_or_else(|| anyhow!("audio stream does not exist"))?;

    Ok(PlaybackTrack {
        track_id: format!("bilibili:{bvid}:{}:{}", part_index + 1, page.cid),
        title: format!("{} - {}", video.title, page.part),
        source_kind: "bilibili".to_string(),
        bvid: bvid.to_string(),
        part: part_index + 1,
        duration_ms: page.duration.saturating_mul(1000),
        audio_url: audio.1,
        referer,
    })
}

pub async fn download_audio(client: &Client, track: &PlaybackTrack) -> Result<Vec<u8>> {
    let response = client
        .get(&track.audio_url)
        .header(REFERER, track.referer.as_str())
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

impl BilibiliResponseExt for reqwest::Response {
    async fn read_result<T>(self, endpoint: &'static str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let body = self
            .error_for_status()
            .with_context(|| format!("bilibili {endpoint} request failed"))?
            .text()
            .await
            .with_context(|| format!("failed to read bilibili {endpoint} response"))?;

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
