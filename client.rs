use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::TcpStream};
use std::io::{self};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);
    let mut line = String::new();

    // LOGIN
    w.write_all(b"LOGIN client1 example.org\r\n").await?;
    reader.read_line(&mut line).await?;
    println!("[RECV] {}", line.trim_end());
    line.clear();

    // SUBSCRIBE to topic1
    w.write_all(b"SUBSCRIBE topic1\r\n").await?;

    // MCAST to topic1
    w.write_all(b"MCAST topic1 Hello, topic1!\r\n").await?;

    // BCAST
    w.write_all(b"BCAST Hello, all!\r\n").await?;

    // PING
    w.write_all(b"PING\r\n").await?;
    reader.read_line(&mut line).await?;
    println!("[RECV] {}", line.trim_end());
    line.clear();

    // Loop for incoming server messages (000 lines)
    loop {
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            println!("[DISCONNECTED]");
            break;
        }
        println!("[SERVER] {}", line.trim_end());
        line.clear();
    }

    Ok(())
}
