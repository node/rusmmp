use std::process::{Child, Command};
use std::time::Duration;

pub struct ServerHandle {
    proc: Child,
}

impl ServerHandle {
    /// start server at random port 
    pub fn start() -> (Self, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // release port to reuse server

        let proc = Command::new("cargo")
            .args(&["run", "--", "--server", "--port", &port.to_string()])
            .spawn()
            .expect("failed to start server");

        // wait to restart server 
        std::thread::sleep(Duration::from_millis(300));

        (Self { proc }, port)
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.proc.kill();
        let _ = self.proc.wait();
    }
}
