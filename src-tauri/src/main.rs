#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::thread;

use libp2p::Multiaddr;
use link_ear::{
    backend::{self, BackendConfig},
    core::NetworkCommand,
};
use serde::Deserialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::{Mutex, mpsc};

#[derive(Default)]
struct BackendState {
    commands: Mutex<Option<mpsc::Sender<NetworkCommand>>>,
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

fn main() {
    tauri::Builder::default()
        .manage(BackendState::default())
        .invoke_handler(tauri::generate_handler![
            start_backend,
            send_chat,
            enqueue_bilibili,
            show_queue,
            pause,
            resume,
            seek,
            set_volume,
            skip,
            remove_queue_item,
            move_queue_item,
            vote
        ])
        .setup(|app| {
            let main_window = app
                .get_webview_window("main")
                .ok_or_else(|| "main window was not created".to_string())?;
            main_window.set_title("link-ear")?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("failed to run link-ear desktop");
}
