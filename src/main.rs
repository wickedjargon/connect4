use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{ConnectInfo, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

static INDEX_HTML: &str = include_str!("../index.html");
static FAVICON: &[u8] = include_bytes!("../favicon.ico");

fn grace_period() -> Duration {
    let secs = std::env::var("GRACE")
        .ok()
        .and_then(|s| s.trim_end_matches('s').parse().ok())
        .unwrap_or(60);
    Duration::from_secs(secs)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

struct Conn {
    id: u64,
    tx: mpsc::UnboundedSender<String>,
}

// A player is one browser tab (anonymous session); the IP is only a display name.
struct Player {
    sid: String,
    ip: String,
    conns: Vec<Conn>,
}

struct Game {
    players: [Option<Player>; 2],
    board: [[u8; 7]; 6], // 0 empty, 1 player 1, 2 player 2; row 0 is top
    phase: &'static str, // waiting, ready, playing, finished
    turn: u8,
    starter: u8,
    winner: u8,
    win_reason: &'static str,
    win_line: Option<[[u8; 2]; 4]>,
    last_move: Option<[u8; 2]>,
    rematch: [bool; 2],
    score: [u32; 2], // games won by each player; lives as long as the pair does
    draws: u32,
    grace_end: [i64; 2], // unix seconds, 0 = none
    grace_gen: [u64; 2],
    next_conn_id: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StateMsg<'a> {
    phase: &'a str,
    board: [[u8; 7]; 6],
    you: u8,
    #[serde(rename = "yourIP")]
    your_ip: &'a str,
    #[serde(rename = "oppIP")]
    opp_ip: &'a str,
    opp_online: bool,
    turn: u8,
    winner: u8,
    win_reason: &'a str,
    win_line: Option<[[u8; 2]; 4]>,
    last_move: Option<[u8; 2]>,
    rematch_you: bool,
    rematch_opp: bool,
    score_you: u32,
    score_opp: u32,
    draws: u32,
    grace_end: i64,
}

impl Game {
    fn new() -> Self {
        Game {
            players: [None, None],
            board: [[0; 7]; 6],
            phase: "waiting",
            turn: 0,
            starter: 1,
            winner: 0,
            win_reason: "",
            win_line: None,
            last_move: None,
            rematch: [false; 2],
            score: [0; 2],
            draws: 0,
            grace_end: [0; 2],
            grace_gen: [0; 2],
            next_conn_id: 0,
        }
    }

    fn player_index(&self, sid: &str) -> Option<usize> {
        self.players
            .iter()
            .position(|p| p.as_ref().is_some_and(|p| p.sid == sid))
    }

    fn reset_board(&mut self) {
        self.board = [[0; 7]; 6];
        self.winner = 0;
        self.win_reason = "";
        self.win_line = None;
        self.last_move = None;
        self.rematch = [false; 2];
        self.grace_end = [0; 2];
    }

    fn state_for(&self, idx: usize) -> String {
        let other = 1 - idx;
        let opp = self.players[other].as_ref();
        let msg = StateMsg {
            phase: self.phase,
            board: self.board,
            you: idx as u8 + 1,
            your_ip: &self.players[idx].as_ref().unwrap().ip,
            opp_ip: opp.map_or("", |p| &p.ip),
            opp_online: opp.is_some_and(|p| !p.conns.is_empty()),
            turn: self.turn,
            winner: self.winner,
            win_reason: self.win_reason,
            win_line: self.win_line,
            last_move: self.last_move,
            rematch_you: self.rematch[idx],
            rematch_opp: self.rematch[other],
            score_you: self.score[idx],
            score_opp: self.score[other],
            draws: self.draws,
            grace_end: if opp.is_some() { self.grace_end[other] } else { 0 },
        };
        serde_json::to_string(&msg).unwrap()
    }

    fn broadcast(&self) {
        for idx in 0..2 {
            if self.players[idx].is_none() {
                continue;
            }
            let msg = self.state_for(idx);
            for conn in &self.players[idx].as_ref().unwrap().conns {
                let _ = conn.tx.send(msg.clone());
            }
        }
    }

    // A player leaving outside of an active game.
    fn remove_player(&mut self, idx: usize) {
        self.players[idx] = None;
        self.reset_board();
        self.score = [0; 2];
        self.draws = 0;
        self.phase = "waiting";
        self.starter = 1;
        self.turn = 0;
    }

    // An active game ends because a player never came back.
    fn forfeit(&mut self, idx: usize) {
        self.players[idx] = None;
        self.phase = "finished";
        self.winner = 2 - idx as u8; // the other player's number
        self.win_reason = "forfeit";
        self.score[1 - idx] += 1;
        self.rematch = [false; 2];
        self.grace_end = [0; 2];
    }

    fn find_win(&self, p: u8) -> Option<[[u8; 2]; 4]> {
        let dirs: [(i32, i32); 4] = [(0, 1), (1, 0), (1, 1), (1, -1)];
        for r in 0..6i32 {
            for c in 0..7i32 {
                for (dr, dc) in dirs {
                    let cells: Vec<(i32, i32)> =
                        (0..4).map(|i| (r + dr * i, c + dc * i)).collect();
                    if cells.iter().all(|&(rr, cc)| {
                        (0..6).contains(&rr)
                            && (0..7).contains(&cc)
                            && self.board[rr as usize][cc as usize] == p
                    }) {
                        let mut line = [[0u8; 2]; 4];
                        for (i, &(rr, cc)) in cells.iter().enumerate() {
                            line[i] = [rr as u8, cc as u8];
                        }
                        return Some(line);
                    }
                }
            }
        }
        None
    }

    fn full(&self) -> bool {
        self.board[0].iter().all(|&c| c != 0)
    }
}

struct App {
    game: Mutex<Game>,
}

type SharedApp = Arc<App>;

fn client_ip(headers: &HeaderMap, addr: &SocketAddr) -> String {
    headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| addr.ip().to_string())
}

fn session_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-session")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 64)
        .map(str::to_owned)
}

fn conn_lost(app: &SharedApp, idx: usize) {
    let mut g = app.game.lock().unwrap();
    match &g.players[idx] {
        Some(p) if p.conns.is_empty() => {}
        _ => return,
    }
    if g.phase == "playing" {
        g.grace_gen[idx] += 1;
        let gen = g.grace_gen[idx];
        let grace = grace_period();
        g.grace_end[idx] = now_unix() + grace.as_secs() as i64;
        let app = app.clone();
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let mut g = app.game.lock().unwrap();
            if g.grace_gen[idx] != gen {
                return;
            }
            match &g.players[idx] {
                Some(p) if p.conns.is_empty() => {}
                _ => return,
            }
            if g.phase == "playing" {
                g.forfeit(idx);
            } else {
                g.remove_player(idx);
            }
            g.broadcast();
        });
    } else {
        g.remove_player(idx);
    }
    g.broadcast();
}

// Removes this connection from its player when the SSE stream is dropped.
struct ConnGuard {
    app: SharedApp,
    idx: usize,
    conn_id: u64,
    handle: tokio::runtime::Handle,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        {
            let mut g = self.app.game.lock().unwrap();
            if let Some(p) = &mut g.players[self.idx] {
                p.conns.retain(|c| c.id != self.conn_id);
            }
        }
        let app = self.app.clone();
        let idx = self.idx;
        self.handle.spawn(async move { conn_lost(&app, idx) });
    }
}

// SSE stream: busy visitors get one event and then silence (keep-alives only);
// players get personalized state updates until they disconnect.
struct EventStream {
    rx: UnboundedReceiverStream<String>,
    _guard: Option<ConnGuard>,
    busy: bool,
    busy_sent: bool,
    _keep_open: Option<mpsc::UnboundedSender<String>>,
}

impl Stream for EventStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.busy {
            if !self.busy_sent {
                self.busy_sent = true;
                let ev = Event::default()
                    .retry(Duration::from_secs(30))
                    .data("{\"phase\":\"busy\"}");
                return Poll::Ready(Some(Ok(ev)));
            }
            return Poll::Pending; // keep-alive pings keep the connection open
        }
        match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(Event::default().data(msg)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn events(
    State(app): State<SharedApp>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    let Some(sid) = params
        .get("session")
        .filter(|s| !s.is_empty() && s.len() <= 64)
        .cloned()
    else {
        return Err((StatusCode::BAD_REQUEST, "missing session"));
    };
    let ip = client_ip(&headers, &addr);
    let (tx, rx) = mpsc::unbounded_channel();

    let mut g = app.game.lock().unwrap();
    let mut idx = g.player_index(&sid);
    if idx.is_none() {
        if let Some(free) = g.players.iter().position(|p| p.is_none()) {
            g.players[free] = Some(Player {
                sid,
                ip: ip.clone(),
                conns: Vec::new(),
            });
            g.reset_board();
            g.score = [0; 2];
            g.draws = 0;
            g.starter = 1;
            g.turn = 0;
            g.phase = if g.players[1 - free].is_some() {
                "ready"
            } else {
                "waiting"
            };
            idx = Some(free);
        }
    }

    let stream = match idx {
        Some(idx) => {
            g.next_conn_id += 1;
            let id = g.next_conn_id;
            g.players[idx]
                .as_mut()
                .unwrap()
                .conns
                .push(Conn { id, tx: tx.clone() });
            g.grace_gen[idx] += 1; // cancel any pending grace timer
            g.grace_end[idx] = 0;
            g.broadcast();
            EventStream {
                rx: UnboundedReceiverStream::new(rx),
                _guard: Some(ConnGuard {
                    app: app.clone(),
                    idx,
                    conn_id: id,
                    handle: tokio::runtime::Handle::current(),
                }),
                busy: false,
                busy_sent: false,
                _keep_open: Some(tx),
            }
        }
        None => EventStream {
            rx: UnboundedReceiverStream::new(rx),
            _guard: None,
            busy: true,
            busy_sent: false,
            _keep_open: Some(tx),
        },
    };
    drop(g);

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(25))
            .text("ping"),
    ))
}

async fn start(State(app): State<SharedApp>, headers: HeaderMap) -> impl IntoResponse {
    let Some(sid) = session_of(&headers) else {
        return (StatusCode::BAD_REQUEST, "missing session");
    };
    let mut g = app.game.lock().unwrap();
    if g.player_index(&sid).is_none() || g.phase != "ready" {
        return (StatusCode::CONFLICT, "cannot start");
    }
    g.phase = "playing";
    g.turn = g.starter;
    g.broadcast();
    (StatusCode::OK, "")
}

#[derive(Deserialize)]
struct MoveReq {
    col: usize,
}

async fn play_move(
    State(app): State<SharedApp>,
    headers: HeaderMap,
    Json(req): Json<MoveReq>,
) -> impl IntoResponse {
    let Some(sid) = session_of(&headers) else {
        return (StatusCode::BAD_REQUEST, "missing session");
    };
    if req.col > 6 {
        return (StatusCode::BAD_REQUEST, "bad move");
    }
    let mut g = app.game.lock().unwrap();
    let Some(idx) = g.player_index(&sid) else {
        return (StatusCode::CONFLICT, "not your turn");
    };
    let me = idx as u8 + 1;
    if g.phase != "playing" || g.turn != me {
        return (StatusCode::CONFLICT, "not your turn");
    }
    let Some(row) = (0..6).rev().find(|&r| g.board[r][req.col] == 0) else {
        return (StatusCode::CONFLICT, "column full");
    };
    g.board[row][req.col] = me;
    g.last_move = Some([row as u8, req.col as u8]);
    if let Some(line) = g.find_win(me) {
        g.phase = "finished";
        g.winner = me;
        g.win_reason = "connect4";
        g.win_line = Some(line);
        g.score[idx] += 1;
    } else if g.full() {
        g.phase = "finished";
        g.winner = 0;
        g.win_reason = "draw";
        g.draws += 1;
    } else {
        g.turn = 3 - g.turn;
    }
    g.broadcast();
    (StatusCode::OK, "")
}

async fn rematch(State(app): State<SharedApp>, headers: HeaderMap) -> impl IntoResponse {
    let Some(sid) = session_of(&headers) else {
        return (StatusCode::BAD_REQUEST, "missing session");
    };
    let mut g = app.game.lock().unwrap();
    let Some(idx) = g.player_index(&sid) else {
        return (StatusCode::CONFLICT, "cannot rematch");
    };
    if g.phase != "finished" || g.players[1 - idx].is_none() {
        return (StatusCode::CONFLICT, "cannot rematch");
    }
    g.rematch[idx] = true;
    if g.rematch == [true, true] {
        g.reset_board();
        g.starter = 3 - g.starter;
        g.turn = g.starter;
        g.phase = "playing";
    }
    g.broadcast();
    (StatusCode::OK, "")
}

async fn index() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-store")], Html(INDEX_HTML))
}

async fn favicon() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/x-icon"),
            (header::CACHE_CONTROL, "public, max-age=604800"),
        ],
        FAVICON,
    )
}

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8085".into());
    let app: SharedApp = Arc::new(App {
        game: Mutex::new(Game::new()),
    });

    let router = Router::new()
        .route("/", get(index))
        .route("/favicon.ico", get(favicon))
        .route("/events", get(events))
        .route("/start", post(start))
        .route("/move", post(play_move))
        .route("/rematch", post(rematch))
        .with_state(app);

    let addr = format!("0.0.0.0:{port}");
    println!("connect4 listening on :{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}
