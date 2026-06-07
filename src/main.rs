use std::{collections::VecDeque, io};

use anyhow::{Context, Result, anyhow};
use chrono::Local;
use clap::Parser;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use libp2p::Multiaddr;
use link_ear::{
    backend::{self, BackendConfig},
    core::{
        ChatRecord, FrontendEvent as UiEvent, MAX_MESSAGES, NetworkCommand, PlaybackView,
        format_duration_ms, normalize_timestamp_micros,
    },
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Parser)]
#[command(author, version, about = "P2P TUI chat built on libp2p")]
struct Cli {
    #[arg(long, default_value = "link-ear")]
    name: String,

    #[arg(long, default_value = "link-ear.chat.v1")]
    topic: String,

    #[arg(long, value_parser = parse_multiaddr)]
    listen: Vec<Multiaddr>,

    #[arg(long, value_parser = parse_multiaddr)]
    peer: Vec<Multiaddr>,

    #[arg(long, value_parser = parse_multiaddr)]
    relay: Vec<Multiaddr>,

    #[arg(long)]
    no_mdns: bool,
}

struct App {
    input: String,
    messages: VecDeque<ChatRecord>,
    statuses: VecDeque<String>,
    peer_count: usize,
    local_peer_id: Option<String>,
    playback: Option<PlaybackView>,
}

impl App {
    fn new() -> Self {
        Self {
            input: String::new(),
            messages: VecDeque::with_capacity(MAX_MESSAGES),
            statuses: VecDeque::with_capacity(80),
            peer_count: 0,
            local_peer_id: None,
            playback: None,
        }
    }

    fn push_event(&mut self, event: UiEvent) {
        match event {
            UiEvent::Status(status) => {
                bounded_push(&mut self.statuses, status, 80);
            }
            UiEvent::PeerCount(count) => self.peer_count = count,
            UiEvent::LocalPeerId(peer_id) => self.local_peer_id = Some(peer_id),
            UiEvent::History(records) => {
                self.messages.clear();
                for record in records {
                    bounded_push(&mut self.messages, record, MAX_MESSAGES);
                }
            }
            UiEvent::Playback(playback) => self.playback = playback,
            UiEvent::Queue(_) | UiEvent::Vote(_) => {}
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tokio::task::LocalSet::new().run_until(run_app()).await
}

async fn run_app() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let cli = Cli::parse();
    let backend_config = BackendConfig {
        name: cli.name,
        topic: cli.topic,
        listen: cli.listen,
        peer: cli.peer,
        relay: cli.relay,
        no_mdns: cli.no_mdns,
    };
    let (network_tx, network_rx) = mpsc::channel(64);
    let (ui_tx, mut ui_rx) = mpsc::channel(256);

    let network_task =
        tokio::task::spawn_local(backend::run_network(backend_config, network_rx, ui_tx));

    let mut terminal = setup_terminal()?;
    let mut events = EventStream::new();
    let mut app = App::new();

    let result = loop {
        terminal.draw(|frame| render(frame, &app))?;

        tokio::select! {
            Some(event) = ui_rx.recv() => {
                app.push_event(event);
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press && should_quit(key) => break Ok(()),
                    Some(Ok(Event::Key(key))) => handle_key(key, &mut app, &network_tx).await?,
                    Some(Ok(Event::Paste(text))) => handle_paste(text, &mut app),
                    Some(Ok(_)) => {}
                    Some(Err(err)) => break Err(anyhow!(err)),
                    None => break Ok(()),
                }
            }
        }
    };

    restore_terminal(&mut terminal)?;
    network_task.abort();
    result
}

async fn handle_key(
    key: KeyEvent,
    app: &mut App,
    network: &mpsc::Sender<NetworkCommand>,
) -> Result<()> {
    if key.kind != KeyEventKind::Press {
        return Ok(());
    }

    match key.code {
        KeyCode::Enter => {
            let text = app.input.trim().to_string();
            if !text.is_empty() {
                network.send(parse_input_command(&text)).await?;
            }
            app.input.clear();
        }
        KeyCode::Backspace => {
            app.input.pop();
        }
        KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.input.push(c);
        }
        _ => {}
    }
    Ok(())
}

fn handle_paste(text: String, app: &mut App) {
    app.input.push_str(text.replace(['\r', '\n'], " ").as_str());
}

fn parse_input_command(text: &str) -> NetworkCommand {
    let mut parts = text.split_whitespace();
    let Some(command) = parts.next() else {
        return NetworkCommand::Chat(String::new());
    };

    match command {
        "/bv" | "/play" => {
            let Some(bvid) = parts.next() else {
                return NetworkCommand::Chat(text.to_string());
            };
            if !is_bvid(bvid) {
                return NetworkCommand::Chat(text.to_string());
            }

            let part = parts
                .next()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|part| *part > 0)
                .unwrap_or(1);

            NetworkCommand::EnqueueBilibili {
                bvid: bvid.to_string(),
                part,
                position: None,
            }
        }
        "/insert" => {
            let Some(position) = parts.next().and_then(|value| value.parse::<usize>().ok()) else {
                return NetworkCommand::Chat(text.to_string());
            };
            let Some(bvid) = parts.next() else {
                return NetworkCommand::Chat(text.to_string());
            };
            if !is_bvid(bvid) {
                return NetworkCommand::Chat(text.to_string());
            }
            let part = parts
                .next()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|part| *part > 0)
                .unwrap_or(1);

            NetworkCommand::EnqueueBilibili {
                bvid: bvid.to_string(),
                part,
                position: Some(position),
            }
        }
        "/queue" | "/q" => NetworkCommand::ShowQueue,
        "/skip" => NetworkCommand::Skip,
        "/remove" | "/rm" | "/delete" => parts
            .next()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|index| *index > 0)
            .map(NetworkCommand::RemoveQueueItem)
            .unwrap_or_else(|| NetworkCommand::Chat(text.to_string())),
        "/move" | "/mv" => {
            let from = parts.next().and_then(|value| value.parse::<usize>().ok());
            let to = parts.next().and_then(|value| value.parse::<usize>().ok());
            match (from, to) {
                (Some(from), Some(to)) if from > 0 && to > 0 => {
                    NetworkCommand::MoveQueueItem { from, to }
                }
                _ => NetworkCommand::Chat(text.to_string()),
            }
        }
        "/vote" => {
            let Some(value) = parts.next() else {
                return NetworkCommand::Chat(text.to_string());
            };
            match value {
                "yes" | "y" | "approve" | "ok" => NetworkCommand::Vote(true),
                "no" | "n" | "reject" => NetworkCommand::Vote(false),
                _ => NetworkCommand::Chat(text.to_string()),
            }
        }
        "/yes" => NetworkCommand::Vote(true),
        "/no" => NetworkCommand::Vote(false),
        "/pause" => NetworkCommand::Pause,
        "/resume" | "/playback" => NetworkCommand::Resume,
        "/seek" => parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .map(|seconds| NetworkCommand::Seek(seconds.saturating_mul(1000)))
            .unwrap_or_else(|| NetworkCommand::Chat(text.to_string())),
        _ => NetworkCommand::Chat(text.to_string()),
    }
}

fn is_bvid(value: &str) -> bool {
    value.len() == 12
        && value.starts_with("BV")
        && value.chars().skip(2).all(|ch| ch.is_ascii_alphanumeric())
}

fn render(frame: &mut Frame<'_>, app: &App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    render_header(frame, root[0], app);
    render_playback(frame, root[1], app);
    render_messages(frame, root[2], app);
    render_status(frame, root[3], app);
    render_input(frame, root[4], app);
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let peer = app
        .local_peer_id
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| "starting".to_string());
    let text = Line::from(vec![
        Span::styled(
            "link-ear",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  peers: {}  local: {peer}", app.peer_count)),
    ]);
    frame.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_messages(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let visible_rows = area.height.saturating_sub(2) as usize;
    let items: Vec<ListItem> = app
        .messages
        .iter()
        .rev()
        .take(visible_rows)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|record| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("[{}] ", format_timestamp(record.sent_at)),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    record.author.clone(),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(": "),
                Span::raw(record.text.clone()),
            ]))
        })
        .collect();

    frame.render_widget(
        List::new(items)
            .block(Block::default().title("Chat").borders(Borders::ALL))
            .style(Style::default().fg(Color::White)),
        area,
    );
}

fn render_playback(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let text = if let Some(playback) = &app.playback {
        let state = if playback.playing {
            "playing"
        } else {
            "paused"
        };
        Line::from(vec![
            Span::styled(
                state,
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "  {} / {}  {}  leader: {}",
                format_duration_ms(playback.position_ms),
                format_duration_ms(playback.duration_ms),
                playback.title,
                playback.leader_peer_id,
            )),
        ])
    } else {
        Line::from("music idle")
    };

    frame.render_widget(
        Paragraph::new(text).block(Block::default().title("Music").borders(Borders::ALL)),
        area,
    );
}

fn render_status(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let text = app
        .statuses
        .iter()
        .rev()
        .take(3)
        .rev()
        .cloned()
        .map(Line::from)
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::default().title("Network").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_input(frame: &mut Frame<'_>, area: Rect, app: &App) {
    frame.render_widget(
        Paragraph::new(app.input.as_str())
            .block(Block::default().title("Message").borders(Borders::ALL)),
        area,
    );
    let cursor_x = area
        .x
        .saturating_add(UnicodeWidthStr::width(app.input.as_str()) as u16)
        .saturating_add(1);
    frame.set_cursor_position((cursor_x, area.y + 1));
}

fn format_timestamp(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp_micros(normalize_timestamp_micros(timestamp))
        .map(|dt| dt.with_timezone(&Local).format("%H:%M:%S").to_string())
        .unwrap_or_else(|| Local::now().format("%H:%M:%S").to_string())
}

fn should_quit(key: KeyEvent) -> bool {
    key.code == KeyCode::Esc
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout)).context("failed to create terminal")
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn parse_multiaddr(value: &str) -> Result<Multiaddr, String> {
    value.parse().map_err(|err| format!("{err}"))
}

fn bounded_push<T>(items: &mut VecDeque<T>, value: T, limit: usize) {
    if items.len() >= limit {
        items.pop_front();
    }
    items.push_back(value);
}
