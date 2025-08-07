mod common;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::test]
async fn full_flow() {
    let (_srv, port) = common::ServerHandle::start();
    let stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let mut framed = Framed::new(stream, LinesCodec::new());

    // LOGIN
    framed.send("LOGIN alice open").await.unwrap();
    let resp = framed.next().await.unwrap().unwrap();
    assert_eq!(resp, "200");

    // SUBSCRIBE
    framed.send("SUBSCRIBE news PRESENCE").await.unwrap();
    assert_eq!(framed.next().await.unwrap().unwrap(), "200");

    // MCAST
    framed.send("MCAST news hello").await.unwrap();
    let event = framed.next().await.unwrap().unwrap();
    assert!(event.starts_with("000 alice MCAST news hello"));

    // DUPLICATE SUBSCRIBE
    framed.send("SUBSCRIBE news").await.unwrap();
    assert_eq!(framed.next().await.unwrap().unwrap(), "409");

    // CLOSE
    framed.send("CLOSE").await.unwrap();
    assert_eq!(framed.next().await.unwrap().unwrap(), "200");
}

