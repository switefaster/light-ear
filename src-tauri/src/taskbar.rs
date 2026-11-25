use light_ear::core::{FrontendEvent, PlaybackView};
use std::sync::Mutex;
use tauri::{
    Manager, Window,
    window::{ProgressBarState, ProgressBarStatus},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PlaybackProgress {
    percent: u64,
    playing: bool,
}

impl PlaybackProgress {
    fn from_view(view: &PlaybackView) -> Option<Self> {
        let duration = u128::from(view.duration_ms);
        let position = u128::from(view.position_ms.min(view.duration_ms));
        Some(Self {
            percent: (100 * position).checked_div(duration)? as u64,
            playing: view.playing,
        })
    }
}

#[derive(Default)]
pub(super) struct Taskbar {
    room: Option<String>,
    progress: Option<PlaybackProgress>,
}

impl Taskbar {
    pub(super) fn observe(&mut self, window: &Window, event: &FrontendEvent) {
        match event {
            FrontendEvent::RoomDirectory { active, .. } if self.room != *active => {
                self.clear(window);
                self.room.clone_from(active);
            }
            FrontendEvent::Playback(view) => {
                self.set_progress(window, view.as_ref().and_then(PlaybackProgress::from_view));
            }
            FrontendEvent::IncomingChat => schedule_badge(window, BadgeEvent::Incoming),
            _ => {}
        }
    }

    pub(super) fn clear(&mut self, window: &Window) {
        self.set_progress(window, None);
        schedule_badge(window, BadgeEvent::Clear);
    }

    fn set_progress(&mut self, window: &Window, next: Option<PlaybackProgress>) {
        if self.progress == next {
            return;
        }
        let status = match next {
            None => ProgressBarStatus::None,
            Some(p) if p.playing => ProgressBarStatus::Normal,
            Some(_) => ProgressBarStatus::Paused,
        };
        if window
            .set_progress_bar(ProgressBarState {
                status: Some(status),
                progress: next.map(|p| p.percent),
            })
            .is_ok()
        {
            self.progress = next;
        }
    }
}

#[derive(Default)]
pub(super) struct UnreadCount(Mutex<i64>);

#[derive(Clone, Copy)]
enum BadgeEvent {
    Incoming,
    Clear,
}

impl BadgeEvent {
    fn count(self, old: i64, minimized: bool) -> i64 {
        match self {
            Self::Incoming if minimized => old.saturating_add(1),
            Self::Incoming => old,
            Self::Clear => 0,
        }
    }
}

// Order the minimized check and badge writes with the native window focus events.
fn schedule_badge(window: &Window, event: BadgeEvent) {
    let target = window.clone();
    let _ = window.run_on_main_thread(move || update_badge(&target, event));
}

pub(super) fn on_focus(window: &Window) {
    update_badge(window, BadgeEvent::Clear);
}

fn update_badge(window: &Window, event: BadgeEvent) {
    let minimized = matches!(event, BadgeEvent::Incoming) && window.is_minimized().unwrap_or(false);
    let unread = window.state::<UnreadCount>();
    let mut count = unread.0.lock().expect("taskbar unread count poisoned");
    let old = *count;
    let new = event.count(old, minimized);
    if old == new {
        return;
    }
    *count = new;
    drop(count);
    #[cfg(windows)]
    if old == 0 || new == 0 {
        let _ = window.set_overlay_icon((new > 0).then(unread_dot));
    }
    #[cfg(not(windows))]
    let _ = window.set_badge_count((new > 0).then_some(new));
}

#[cfg(windows)]
fn unread_dot() -> tauri::image::Image<'static> {
    // Windows overlays are 16 px. A white rim keeps the dot visible on dark and light icons.
    let mut rgba = Vec::with_capacity(16 * 16 * 4);
    for y in 0..16 {
        for x in 0..16 {
            let radius = (x as f32 - 7.5).hypot(y as f32 - 7.5);
            let rgb = if radius <= 5.5 {
                [221, 50, 65]
            } else {
                [255; 3]
            };
            rgba.extend_from_slice(&rgb);
            rgba.push(((7.5 - radius).clamp(0.0, 1.0) * 255.0) as u8);
        }
    }
    tauri::image::Image::new_owned(rgba, 16, 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_tracks_seeks_pause_and_idle() {
        let mut view = PlaybackView {
            title: "song".into(),
            leader_peer_id: "peer".into(),
            position_ms: 45_000,
            duration_ms: 60_000,
            playing: true,
        };
        assert_eq!(
            PlaybackProgress::from_view(&view),
            Some(PlaybackProgress {
                percent: 75,
                playing: true
            })
        );
        view.playing = false;
        view.position_ms = 12_000;
        assert_eq!(
            PlaybackProgress::from_view(&view),
            Some(PlaybackProgress {
                percent: 20,
                playing: false
            })
        );
        view.position_ms = u64::MAX;
        assert_eq!(PlaybackProgress::from_view(&view).unwrap().percent, 100);
        view.duration_ms = 0;
        assert_eq!(PlaybackProgress::from_view(&view), None);
    }

    #[test]
    fn unread_chat_accumulates_while_minimized_and_clears_on_return() {
        let mut count = BadgeEvent::Incoming.count(0, false);
        assert_eq!(count, 0);
        count = BadgeEvent::Incoming.count(count, true);
        count = BadgeEvent::Incoming.count(count, true);
        assert_eq!(count, 2);
        assert_eq!(BadgeEvent::Clear.count(count, false), 0);
        assert_eq!(BadgeEvent::Clear.count(count, true), 0);
    }
}
