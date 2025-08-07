mod common;
use ssmp_rs::{Payload, ServerState};
use tokio::sync::mpsc;

#[tokio::test]
async fn login_and_close() {
    let state = ServerState::default();
    let (tx, _rx) = mpsc::unbounded_channel();

    let peer = state.add_peer("user1".into(), tx).await;
    assert_eq!(state.peers.read().await.len(), 1);

    state.remove_peer("user1").await;
    assert!(state.peers.read().await.is_empty());
}

#[tokio::test]
async fn subscribe_multicast() {
    let state = ServerState::default();
    let (tx, mut rx) = mpsc::unbounded_channel();

    let peer = state.add_peer("bob".into(), tx).await;
    state.subscribe("news".into(), "bob", true).await;

    // bob should receive it's presence event
    let msg = rx.recv().await.unwrap();
    assert!(msg.contains("SUBSCRIBE news PRESENCE"));

    // subscribe again, return 409（interupt when test client later下）
}

#[tokio::test]
async fn broadcast_payload() {
    let state = ServerState::default();
    let (tx1, mut rx1) = mpsc::unbounded_channel();
    let (tx2, _rx2) = mpsc::unbounded_channel();

    state.add_peer("a".into(), tx1).await;
    state.add_peer("b".into(), tx2).await;

    state.subscribe("room".into(), "a", false).await;
    state.subscribe("room".into(), "b", false).await;

    state.send_broadcast("a", Payload::Text("hi".into())).await;

    let msg = rx1.recv().await.unwrap();
    assert!(msg.contains("BCAST"));
}
