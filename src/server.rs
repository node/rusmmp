// SSMP – Stupid-Simple Messaging Protocol – Reference Implementation (Rust)
// cargo run -- --server   -> listen 127.0.0.1:8080
// cargo run -- --client   -> interaction REPL connect to server

use bytes::Bytes;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::codec::{Framed, LinesCodec};

// ---------- protocol final via ----------

const MAX_LINE_LEN: usize = 4096;
const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

// ---------- message defination ----------

#[derive(Debug)]
enum Msg {
    // client -> server
    Login { id: String, scheme: String, cred: Option<String> },
    Close,
    Ping,
    Pong,
    Subscribe { topic: String, presence: bool },
    Unsubscribe { topic: String },
    Ucast { to: String, payload: Payload },
    Mcast { topic: String, payload: Payload },
    Bcast { payload: Payload },
    Unknown(String), // support forward compatibility
}

#[derive(Debug, Clone)]
enum Payload {
    Text(String),
    Binary(Bytes),
}

impl Payload {
    fn encode(&self) -> String {
        match self {
            Payload::Text(s) => s.clone(),
            Payload::Binary(b) => {
                // binary encode: lead length 2 bytes + content
                let mut out = Vec::with_capacity(2 + b.len());
                let len = (b.len() - 1) as u16; // protocol: length-1
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(b);
                // convert to invisible charactors, base64 to use LinesCodec
                base64::encode(&out)
            }
        }
    }

    fn decode(raw: &str) -> Result<Payload, ()> {
        if raw.is_empty() { return Ok(Payload::Text("".into())); }
        let first = raw.as_bytes()[0];
        if matches!(first, 0 | 1 | 2 | 3) {
            // binary
            let bytes = base64::decode(raw).map_err(|_| ())?;
            if bytes.len() < 2 { return Err(()); }
            let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize + 1;
            if len != bytes.len() - 2 { return Err(()); }
            Ok(Payload::Binary(Bytes::copy_from_slice(&bytes[2..])))
        } else {
            Ok(Payload::Text(raw.into()))
        }
    }
}

// ---------- decode ----------

fn parse_msg(line: &str) -> Result<Msg, ()> {
    let mut toks = line.splitn(2, ' ');
    let verb = toks.next().ok_or(())?;
    let rest = toks.next().unwrap_or("");

    match verb {
        "LOGIN" => {
            let mut t = rest.splitn(3, ' ');
            let id = t.next().ok_or(())?.to_string();
            let scheme = t.next().ok_or(())?.to_string();
            let cred = t.next().map(|s| s.to_string());
            Ok(Msg::Login { id, scheme, cred })
        }
        "CLOSE" => Ok(Msg::Close),
        "PING" => Ok(Msg::Ping),
        "PONG" => Ok(Msg::Pong),
        "SUBSCRIBE" => {
            let mut t = rest.splitn(2, ' ');
            let topic = t.next().ok_or(())?.to_string();
            let presence = t.next() == Some("PRESENCE");
            Ok(Msg::Subscribe { topic, presence })
        }
        "UNSUBSCRIBE" => {
            let topic = rest.to_string();
            if topic.is_empty() { return Err(()); }
            Ok(Msg::Unsubscribe { topic })
        }
        "UCAST" => {
            let mut t = rest.splitn(2, ' ');
            let to = t.next().ok_or(())?.to_string();
            let payload = Payload::decode(t.next().unwrap_or(""))?;
            Ok(Msg::Ucast { to, payload })
        }
        "MCAST" => {
            let mut t = rest.splitn(2, ' ');
            let topic = t.next().ok_or(())?.to_string();
            let payload = Payload::decode(t.next().unwrap_or(""))?;
            Ok(Msg::Mcast { topic, payload })
        }
        "BCAST" => {
            let payload = Payload::decode(rest)?;
            Ok(Msg::Bcast { payload })
        }
        _ => Ok(Msg::Unknown(line.to_string())),
    }
}

// ---------- server ----------

type Tx = mpsc::UnboundedSender<String>;

#[derive(Default)]
struct ServerState {
    peers: RwLock<HashMap<String, Peer>>,
    topics: RwLock<HashMap<String, HashSet<String>>>, // topic -> set<peer_id>
}

#[derive(Clone)]
struct Peer {
    tx: Tx,
    topics: Arc<Mutex<HashSet<String>>>, // this peer subscribed topic
    presence: Arc<Mutex<HashSet<String>>>, // this peer require presence topic
}

impl ServerState {
    async fn add_peer(&self, id: String, tx: Tx) -> Peer {
        let peer = Peer {
            tx,
            topics: Default::default(),
            presence: Default::default(),
        };
        self.peers.write().await.insert(id.clone(), peer.clone());
        peer
    }

    async fn remove_peer(&self, id: &str) {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.remove(id) {
            let topics = { peer.topics.lock().await.clone() };
            for topic in topics {
                self.unsubscribe(topic, id).await;
            }
        }
    }

    async fn subscribe(&self, topic: String, peer_id: &str, presence: bool) {
        {
            let mut tmap = self.topics.write().await;
            tmap.entry(topic.clone()).or_default().insert(peer_id.to_string());
        }
        if let Some(peer) = self.peers.read().await.get(peer_id) {
            peer.topics.lock().await.insert(topic.clone());
            if presence {
                peer.presence.lock().await.insert(topic.clone());
            }
            // send SUBSCRIBE event to other presence clients
            let event = format!("000 {} SUBSCRIBE {} {}", peer_id, topic, if presence { "PRESENCE" } else { "" });
            self.broadcast_event(&event, &topic).await;
        }
    }

    async fn unsubscribe(&self, topic: String, peer_id: &str) {
        {
            let mut tmap = self.topics.write().await;
            if let Some(set) = tmap.get_mut(&topic) {
                set.remove(peer_id);
                if set.is_empty() {
                    tmap.remove(&topic);
                }
            }
        }
        if let Some(peer) = self.peers.read().await.get(peer_id) {
            peer.topics.lock().await.remove(&topic);
            peer.presence.lock().await.remove(&topic);
            let event = format!("000 {} UNSUBSCRIBE {}", peer_id, topic);
            self.broadcast_event(&event, &topic).await;
        }
    }

    async fn broadcast_event(&self, event: &str, skip_topic: &str) {
        let topics = self.topics.read().await;
        let peers = self.peers.read().await;
        if let Some(subscribers) = topics.get(skip_topic) {
            for id in subscribers {
                if let Some(peer) = peers.get(id) {
                    let _ = peer.tx.send(event.to_string());
                }
            }
        }
    }

    async fn send_unicast(&self, to: &str, event: String) -> bool {
        if let Some(peer) = self.peers.read().await.get(to) {
            let _ = peer.tx.send(event);
            true
        } else {
            false
        }
    }

    async fn send_multicast(&self, topic: &str, from: &str, payload: Payload) {
        let topics = self.topics.read().await;
        let peers = self.peers.read().await;
        if let Some(subscribers) = topics.get(topic) {
            let event = format!("000 {} MCAST {} {}", from, topic, payload.encode());
            for id in subscribers {
                if id != from {
                    if let Some(peer) = peers.get(id) {
                        let _ = peer.tx.send(event.clone());
                    }
                }
            }
        }
    }

    async fn send_broadcast(&self, from: &str, payload: Payload) {
        let peers = self.peers.read().await;
        let mut sent = HashSet::new();
        // find from subscribed topic
        if let Some(from_peer) = peers.get(from) {
            let from_topics = from_peer.topics.lock().await.clone();
            for topic in from_topics {
                if let Some(subscribers) = self.topics.read().await.get(&topic) {
                    for id in subscribers {
                        if id != from && !sent.contains(id) {
                            if let Some(peer) = peers.get(id) {
                                let event = format!("000 {} BCAST {}", from, payload.encode());
                                let _ = peer.tx.send(event);
                                sent.insert(id.clone());
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn handle_client(stream: TcpStream, state: Arc<ServerState>) {
    let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(MAX_LINE_LEN));
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // required LOGIN
    let login_line = match framed.next().await {
        Some(Ok(l)) => l,
        _ => return,
    };
    let (peer_id, peer) = match parse_msg(&login_line) {
        Ok(Msg::Login { id, scheme: _, cred: _ }) => {
            // simply ignore scheme/cred
            let p = state.add_peer(id.clone(), tx).await;
            let _ = framed.send("200").await;
            (id, p)
        }
        _ => {
            let _ = framed.send("400").await;
            return;
        }
    };

    // read async & heartbeat
    let (mut sink, mut stream) = framed.split();
    let state_clone = state.clone();
    let peer_id_clone = peer_id.clone();

    // writeback task
    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if sink.send(line).await.is_err() {
                break;
            }
        }
    });

    // heartbeat
    let ping_task = {
        let tx = peer.tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(PING_INTERVAL).await;
                if tx.send("000 . PING".into()).is_err() {
                    break;
                }
            }
        })
    };

    // main loop
    while let Some(Ok(line)) = stream.next().await {
        match parse_msg(&line) {
            Ok(Msg::Close) => {
                let _ = peer.tx.send("200".into());
                break;
            }
            Ok(Msg::Ping) => {
                let _ = peer.tx.send("000 . PONG".into());
            }
            Ok(Msg::Pong) => { /* ignore */ }
            Ok(Msg::Subscribe { topic, presence }) => {
                state.subscribe(topic, &peer_id, presence).await;
                let _ = peer.tx.send("200".into());
            }
            Ok(Msg::Unsubscribe { topic }) => {
                state.unsubscribe(topic, &peer_id).await;
                let _ = peer.tx.send("200".into());
            }
            Ok(Msg::Ucast { to, payload }) => {
                let event = format!("000 {} UCAST {} {}", peer_id, to, payload.encode());
                if !state.send_unicast(&to, event).await {
                    let _ = peer.tx.send("404".into());
                }
            }
            Ok(Msg::Mcast { topic, payload }) => {
                state.send_multicast(&topic, &peer_id, payload).await;
            }
            Ok(Msg::Bcast { payload }) => {
                state.send_broadcast(&peer_id, payload).await;
            }
            Ok(Msg::Unknown(line)) => {
                let _ = peer.tx.send("501".into());
            }
            Err(_) => {
                let _ = peer.tx.send("400".into());
                break;
            }
        }
    }

    ping_task.abort();
    state.remove_peer(&peer_id_clone).await;
}

#[tokio::main]
async fn run_server(port: u16) {

    let state = Arc::new(ServerState::default());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    println!("SSMP server listening on 127.0.0.1:{}", port);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_client(stream, state.clone()));
    }
}

// ---------- client REPL ----------

async fn client_repl() {
    let mut framed = Framed::new(
        TcpStream::connect("127.0.0.1:8080").await.unwrap(),
        LinesCodec::new_with_max_length(MAX_LINE_LEN),
    );

    println!("Login identifier:");
    let mut id = String::new();
    std::io::stdin().read_line(&mut id).unwrap();
    let id = id.trim();
    framed.send(format!("LOGIN {} open")).await.unwrap();

    let resp = framed.next().await.unwrap().unwrap();
    if resp != "200" {
        println!("Login failed: {}", resp);
        return;
    }
    println!("Logged in as {}", id);

    // read/write 
    let (mut sink, mut stream) = framed.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            let _ = sink.send(line).await;
        }
    });

    // print events
    tokio::spawn(async move {
        while let Some(Ok(line)) = stream.next().await {
            println!("< {}", line);
        }
    });

    // REPL
    loop {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        let line = line.trim();
        if line == "quit" { break; }
        tx.send(line.into()).unwrap();
    }
}

// ---------- main ----------

#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[arg(long)]
    server: bool,
    #[arg(long)]
    client: bool,
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if args.server {
        run_server().await;
    } else if args.client {
        client_repl().await;
    } else {
        println!("Use --server or --client");
    }
}