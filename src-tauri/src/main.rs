#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    fs,
    path::PathBuf,
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use libp2p::Multiaddr;
use light_ear::{
    backend::{self, BackendConfig},
    core::NetworkCommand,
};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager, State, WindowEvent};
use tauri_plugin_clipboard_manager::ClipboardExt;
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_updater::{Update, UpdaterExt};
use tokio::sync::{Mutex, mpsc};

#[derive(Default)]
struct BackendState {
    commands: Mutex<Option<mpsc::Sender<NetworkCommand>>>,
    closing: AtomicBool,
}

#[derive(Default)]
struct UpdaterState {
    pending: StdMutex<Option<Update>>,
    installing: AtomicBool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DesktopConfig {
    name: String,
    topic: String,
    listen: Vec<String>,
    peer: Vec<String>,
    relay: Vec<String>,
    no_mdns: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateInfo {
    version: String,
    current_version: String,
    date: Option<String>,
    body: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdaterEvent {
    event: &'static str,
    version: Option<String>,
    downloaded: Option<u64>,
    content_length: Option<u64>,
    message: Option<String>,
}

impl UpdateInfo {
    fn from_update(update: &Update) -> Self {
        Self {
            version: update.version.clone(),
            current_version: update.current_version.clone(),
            date: update.date.map(|date| date.to_string()),
            body: update.body.clone(),
        }
    }
}

impl UpdaterEvent {
    fn started(version: String) -> Self {
        Self {
            event: "started",
            version: Some(version),
            downloaded: None,
            content_length: None,
            message: None,
        }
    }

    fn progress(version: String, downloaded: u64, content_length: Option<u64>) -> Self {
        Self {
            event: "progress",
            version: Some(version),
            downloaded: Some(downloaded),
            content_length,
            message: None,
        }
    }

    fn finished(version: String) -> Self {
        Self {
            event: "finished",
            version: Some(version),
            downloaded: None,
            content_length: None,
            message: None,
        }
    }

    fn installed(version: String) -> Self {
        Self {
            event: "installed",
            version: Some(version),
            downloaded: None,
            content_length: None,
            message: None,
        }
    }

    fn failed(message: String) -> Self {
        Self {
            event: "failed",
            version: None,
            downloaded: None,
            content_length: None,
            message: Some(message),
        }
    }
}

#[tauri::command]
async fn start_backend(
    app: AppHandle,
    state: State<'_, BackendState>,
    config: DesktopConfig,
) -> Result<(), String> {
    let mut commands = state.commands.lock().await;
    if commands.is_some() {
        return Ok(());
    }

    let backend_config = BackendConfig {
        name: config.name,
        topic: config.topic,
        listen: parse_multiaddrs(config.listen)?,
        peer: parse_multiaddrs(config.peer)?,
        relay: parse_multiaddrs(config.relay)?,
        no_mdns: config.no_mdns,
    };

    let (command_tx, command_rx) = mpsc::channel(64);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    *commands = Some(command_tx);

    thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                let _ = app.emit("backend-error", format!("failed to start runtime: {err}"));
                return;
            }
        };
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, async move {
            let event_app = app.clone();
            tokio::task::spawn_local(async move {
                while let Some(event) = event_rx.recv().await {
                    let _ = event_app.emit("backend-event", event);
                }
            });

            if let Err(err) = backend::run_network(backend_config, command_rx, event_tx).await {
                let _ = app.emit("backend-error", format!("{err:#}"));
            }
        });
    });

    Ok(())
}

#[tauri::command]
async fn send_chat(state: State<'_, BackendState>, text: String) -> Result<(), String> {
    send_command(&state, NetworkCommand::Chat(text)).await
}

#[tauri::command]
async fn enqueue_bilibili(
    state: State<'_, BackendState>,
    bvid: String,
    part: Option<usize>,
    position: Option<usize>,
) -> Result<(), String> {
    send_command(
        &state,
        NetworkCommand::EnqueueBilibili {
            bvid,
            part: part.unwrap_or(1),
            position,
        },
    )
    .await
}

#[tauri::command]
async fn read_clipboard_text(app: AppHandle) -> Result<String, String> {
    tauri::async_runtime::spawn_blocking(move || app.clipboard().read_text())
        .await
        .map_err(|err| format!("clipboard read task failed: {err}"))?
        .map_err(|err| format!("failed to read clipboard: {err}"))
}

#[tauri::command]
async fn show_queue(state: State<'_, BackendState>) -> Result<(), String> {
    send_command(&state, NetworkCommand::ShowQueue).await
}

#[tauri::command]
async fn pause(state: State<'_, BackendState>) -> Result<(), String> {
    send_command(&state, NetworkCommand::Pause).await
}

#[tauri::command]
async fn resume(state: State<'_, BackendState>) -> Result<(), String> {
    send_command(&state, NetworkCommand::Resume).await
}

#[tauri::command]
async fn seek(state: State<'_, BackendState>, seconds: u64) -> Result<(), String> {
    send_command(&state, NetworkCommand::Seek(seconds.saturating_mul(1000))).await
}

#[tauri::command]
async fn set_volume(state: State<'_, BackendState>, percent: u8) -> Result<(), String> {
    send_command(&state, NetworkCommand::SetVolume(percent)).await
}

#[tauri::command]
async fn skip(state: State<'_, BackendState>) -> Result<(), String> {
    send_command(&state, NetworkCommand::Skip).await
}

#[tauri::command]
async fn remove_queue_item(state: State<'_, BackendState>, index: usize) -> Result<(), String> {
    send_command(&state, NetworkCommand::RemoveQueueItem(index)).await
}

#[tauri::command]
async fn move_queue_item(
    state: State<'_, BackendState>,
    from: usize,
    to: usize,
) -> Result<(), String> {
    send_command(&state, NetworkCommand::MoveQueueItem { from, to }).await
}

#[tauri::command]
async fn vote(state: State<'_, BackendState>, approve: bool) -> Result<(), String> {
    send_command(&state, NetworkCommand::Vote(approve)).await
}

#[tauri::command]
async fn export_status_logs(
    app: AppHandle,
    filename: String,
    content: String,
) -> Result<Option<String>, String> {
    if content.trim().is_empty() {
        return Err("log export is empty".to_string());
    }
    if content.len() > 8 * 1024 * 1024 {
        return Err("log export is too large".to_string());
    }

    let filename = sanitize_log_filename(&filename);
    let mut dialog = app
        .dialog()
        .file()
        .set_title("Export light-ear log")
        .add_filter("JSON Lines", &["jsonl"])
        .set_file_name(filename);
    if let Ok(directory) = app
        .path()
        .download_dir()
        .or_else(|_| app.path().app_data_dir())
    {
        dialog = dialog.set_directory(directory);
    }

    let Some(path) = dialog.blocking_save_file() else {
        return Ok(None);
    };
    let path = path
        .into_path()
        .map_err(|err| format!("unsupported log export path: {err}"))?;
    let path = normalize_log_export_path(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create log export directory: {err}"))?;
    }
    fs::write(&path, content).map_err(|err| format!("failed to write log export: {err}"))?;

    Ok(Some(path.display().to_string()))
}

#[tauri::command]
async fn check_for_update(
    app: AppHandle,
    state: State<'_, UpdaterState>,
) -> Result<Option<UpdateInfo>, String> {
    let update = app
        .updater()
        .map_err(|err| format!("failed to initialize updater: {err}"))?
        .check()
        .await
        .map_err(|err| format!("failed to check for updates: {err}"))?;
    let info = update.as_ref().map(UpdateInfo::from_update);
    *state
        .pending
        .lock()
        .map_err(|_| "updater state lock poisoned".to_string())? = update;
    Ok(info)
}

#[tauri::command]
async fn install_update(app: AppHandle, state: State<'_, UpdaterState>) -> Result<(), String> {
    if state.installing.swap(true, Ordering::SeqCst) {
        return Err("update installation is already running".to_string());
    }

    let update = state
        .pending
        .lock()
        .map_err(|_| "updater state lock poisoned".to_string())?
        .take();
    let Some(update) = update else {
        state.installing.store(false, Ordering::SeqCst);
        return Err("no pending update; check for updates first".to_string());
    };

    let version = update.version.clone();
    let _ = app.emit("updater-event", UpdaterEvent::started(version.clone()));

    let mut downloaded = 0_u64;
    let progress_app = app.clone();
    let progress_version = version.clone();
    let finished_app = app.clone();
    let finished_version = version.clone();

    let result = update
        .download_and_install(
            move |chunk_length, content_length| {
                downloaded = downloaded.saturating_add(chunk_length as u64);
                let _ = progress_app.emit(
                    "updater-event",
                    UpdaterEvent::progress(progress_version.clone(), downloaded, content_length),
                );
            },
            move || {
                let _ = finished_app.emit(
                    "updater-event",
                    UpdaterEvent::finished(finished_version.clone()),
                );
            },
        )
        .await;

    match result {
        Ok(()) => {
            let _ = app.emit("updater-event", UpdaterEvent::installed(version));
            app.restart()
        }
        Err(err) => {
            state.installing.store(false, Ordering::SeqCst);
            let message = format!("failed to install update: {err}");
            let _ = app.emit("updater-event", UpdaterEvent::failed(message.clone()));
            Err(message)
        }
    }
}

#[tauri::command]
async fn shutdown_backend(state: State<'_, BackendState>) -> Result<(), String> {
    shutdown_backend_state(state.inner()).await;
    Ok(())
}

async fn send_command(
    state: &State<'_, BackendState>,
    command: NetworkCommand,
) -> Result<(), String> {
    let commands = state.commands.lock().await;
    let sender = commands
        .as_ref()
        .ok_or_else(|| "backend is not running".to_string())?;
    sender
        .send(command)
        .await
        .map_err(|_| "backend command channel is closed".to_string())
}

async fn shutdown_backend_state(state: &BackendState) {
    let mut commands = state.commands.lock().await;
    if let Some(sender) = commands.take() {
        let _ = sender.send(NetworkCommand::Shutdown).await;
    }
}

fn parse_multiaddrs(values: Vec<String>) -> Result<Vec<Multiaddr>, String> {
    values
        .into_iter()
        .filter_map(|value| {
            let value = value.trim().to_string();
            (!value.is_empty()).then_some(value)
        })
        .map(|value| {
            value
                .parse::<Multiaddr>()
                .map_err(|err| format!("invalid multiaddr '{value}': {err}"))
        })
        .collect()
}

fn sanitize_log_filename(value: &str) -> String {
    let mut filename = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect::<String>();
    while filename.contains("..") {
        filename = filename.replace("..", ".");
    }
    filename = filename.trim_matches(['-', '.', '_']).to_string();
    if filename.is_empty() {
        filename = "light-ear-log.jsonl".to_string();
    }
    if !filename.ends_with(".jsonl") {
        filename.push_str(".jsonl");
    }
    filename
}

fn normalize_log_export_path(mut path: PathBuf) -> PathBuf {
    if path.extension().is_none() {
        path.set_extension("jsonl");
    }
    path
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{normalize_log_export_path, sanitize_log_filename};

    #[test]
    fn sanitize_log_filename_keeps_safe_jsonl_name() {
        assert_eq!(
            sanitize_log_filename("light-ear-log-20260613-120000.jsonl"),
            "light-ear-log-20260613-120000.jsonl"
        );
    }

    #[test]
    fn sanitize_log_filename_rejects_path_segments() {
        assert_eq!(
            sanitize_log_filename("../some/path/light-ear-log"),
            "some-path-light-ear-log.jsonl"
        );
    }

    #[test]
    fn normalize_log_export_path_adds_missing_extension() {
        assert_eq!(
            normalize_log_export_path(PathBuf::from("light-ear-log")),
            PathBuf::from("light-ear-log.jsonl")
        );
        assert_eq!(
            normalize_log_export_path(PathBuf::from("light-ear-log.txt")),
            PathBuf::from("light-ear-log.txt")
        );
    }
}

fn main() {
    tauri::Builder::default()
        .manage(BackendState::default())
        .manage(UpdaterState::default())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .on_window_event(|window, event| {
            if !matches!(event, WindowEvent::CloseRequested { .. }) {
                return;
            }

            let WindowEvent::CloseRequested { api, .. } = event else {
                return;
            };
            let app = window.app_handle().clone();
            let state = app.state::<BackendState>();
            if state.closing.swap(true, Ordering::SeqCst) {
                return;
            }

            api.prevent_close();
            let window = window.clone();
            tauri::async_runtime::spawn(async move {
                let state = app.state::<BackendState>();
                shutdown_backend_state(state.inner()).await;
                tokio::time::sleep(Duration::from_millis(800)).await;
                let _ = window.close();
            });
        })
        .invoke_handler(tauri::generate_handler![
            start_backend,
            send_chat,
            enqueue_bilibili,
            read_clipboard_text,
            show_queue,
            pause,
            resume,
            seek,
            set_volume,
            skip,
            remove_queue_item,
            move_queue_item,
            vote,
            export_status_logs,
            check_for_update,
            install_update,
            shutdown_backend
        ])
        .setup(|app| {
            let main_window = app
                .get_webview_window("main")
                .ok_or_else(|| "main window was not created".to_string())?;
            main_window.set_title("light-ear")?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("failed to run light-ear desktop");
}
