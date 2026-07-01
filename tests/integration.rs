use std::time::Duration;

use agent_pair::{AgentClient, Broker};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

async fn start_broker() -> std::net::SocketAddr {
    let port = free_port().await;
    let a: std::net::SocketAddr = ([127, 0, 0, 1], port).into();

    tokio::spawn(async move {
        Broker::new().listen(a).await.unwrap_err();
    });

    for _ in 0..40 {
        if tokio::net::TcpStream::connect(a).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return a;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker didn't start")
}

async fn free_port() -> u16 {
    portpicker::pick_unused_port().expect("no free port")
}

async fn start_echo(port: u16) {
    let l = TcpListener::bind(format!("127.0.0.1:{port}")).await.unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let (mut r, mut w) = s.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            } else {
                break;
            }
        }
    });
}

async fn read_line(s: &mut TcpStream) -> String {
    let mut r = BufReader::new(s);
    let mut l = String::new();
    r.read_line(&mut l).await.unwrap();
    l
}

fn ws_addr(addr: std::net::SocketAddr) -> String {
    format!("ws://{addr}")
}

async fn wait_for_port(port: u16) {
    let a: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(a).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("port {port} not ready")
}

async fn setup_tunnel() -> (std::net::SocketAddr, u16) {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .bind(echo, "srv")
            .await;
    });

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .forward(local, "srv")
            .await;
    });

    wait_for_port(local).await;
    (b, local)
}

// ===== Tests =====

#[tokio::test]
async fn bind_then_forward() {
    let (_b, local) = setup_tunnel().await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"hello\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "hello\n");
}

#[tokio::test]
async fn multi_label() {
    let b = start_broker().await;
    let e1 = free_port().await;
    let e2 = free_port().await;
    let f1 = free_port().await;
    let f2 = free_port().await;
    start_echo(e1).await;
    start_echo(e2).await;

    let a1 = ws_addr(b);
    let a2 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a1).await.unwrap().bind(e1, "web").await;
    });
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a2).await.unwrap().bind(e2, "ssh").await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let a1 = ws_addr(b);
    let a2 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a1).await.unwrap().forward(f1, "web").await;
    });
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a2).await.unwrap().forward(f2, "ssh").await;
    });
    wait_for_port(f1).await;
    wait_for_port(f2).await;

    let mut w = TcpStream::connect(format!("127.0.0.1:{f1}")).await.unwrap();
    w.write_all(b"web\n").await.unwrap();
    assert_eq!(read_line(&mut w).await, "web\n");
    let mut s = TcpStream::connect(format!("127.0.0.1:{f2}")).await.unwrap();
    s.write_all(b"ssh\n").await.unwrap();
    assert_eq!(read_line(&mut s).await, "ssh\n");
}

#[tokio::test]
async fn forward_cancel_and_restart() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .bind(echo, "web")
            .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let addr = ws_addr(b);
    let h = tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .forward(local, "web")
            .await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"before\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "before\n");
    drop(c);
    h.abort();
    let _ = h.await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .forward(local, "web")
            .await;
    });
    wait_for_port(local).await;
    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"after\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "after\n");
}

#[tokio::test]
async fn http_multiple_roundtrips() {
    let (_b, local) = setup_tunnel().await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    for i in 0..10 {
        let m = format!("r{i}\n");
        c.write_all(m.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut c).await, m, "round {i}");
    }
}

#[tokio::test]
async fn many_concurrent() {
    let (_b, local) = setup_tunnel().await;

    let mut hs = vec![];
    for i in 0..30 {
        let addr = format!("127.0.0.1:{local}");
        hs.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(i as u64 % 3 * 10)).await;
            let mut c = TcpStream::connect(addr).await.unwrap();
            let m = format!("c{i}\n");
            c.write_all(m.as_bytes()).await.unwrap();
            assert_eq!(read_line(&mut c).await, m, "conn {i}");
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn multiple_forwards_same_label() {
    let b = start_broker().await;
    let echo = free_port().await;
    let f1 = free_port().await;
    let f2 = free_port().await;
    start_echo(echo).await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .bind(echo, "srv")
            .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let a1 = ws_addr(b);
    let a2 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a1)
            .await
            .unwrap()
            .forward(f1, "srv")
            .await;
    });
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a2)
            .await
            .unwrap()
            .forward(f2, "srv")
            .await;
    });
    wait_for_port(f1).await;
    wait_for_port(f2).await;

    for &p in &[f1, f2] {
        let mut c = TcpStream::connect(format!("127.0.0.1:{p}")).await.unwrap();
        c.write_all(b"multi\n").await.unwrap();
        assert_eq!(read_line(&mut c).await, "multi\n");
    }
}

#[tokio::test]
async fn interleaved_integrity() {
    let (_b, local) = setup_tunnel().await;

    let mut hs = vec![];
    for g in 0..3 {
        let addr = format!("127.0.0.1:{local}");
        hs.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(g * 20)).await;
            let mut c = TcpStream::connect(addr).await.unwrap();
            for r in 0..5 {
                let m = format!("g{g}r{r}\n");
                c.write_all(m.as_bytes()).await.unwrap();
                assert_eq!(read_line(&mut c).await, m, "g{g}r{r}");
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn large_payload_1mb() {
    let (_b, local) = setup_tunnel().await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i ^ (i >> 8)) as u8).collect();
    c.write_all(&data).await.unwrap();
    let mut buf = vec![0u8; data.len()];
    c.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, data, "1MB mismatch");
}

#[tokio::test]
async fn unknown_label() {
    let b = start_broker().await;
    let local = free_port().await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .forward(local, "nope")
            .await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"x\n").await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(2), c.read(&mut [0u8; 1])).await;
    assert!(r.is_ok(), "should close");
}

#[tokio::test]
async fn bind_forward_shared_tcp() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let c = AgentClient::connect(&ws_addr(b)).await.unwrap();

    tokio::spawn({
        let c = c.clone();
        async move { let _ = c.bind(echo, "web").await; }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::spawn({
        let c = c.clone();
        async move { let _ = c.forward(local, "web").await; }
    });
    wait_for_port(local).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    s.write_all(b"shared\n").await.unwrap();
    assert_eq!(read_line(&mut s).await, "shared\n");
}

#[tokio::test]
async fn bind_duplicate_label() {
    let b = start_broker().await;
    let echo = free_port().await;
    start_echo(echo).await;

    let c = AgentClient::connect(&ws_addr(b)).await.unwrap();
    let c2 = c.clone();
    tokio::spawn(async move { let _ = c2.bind(echo, "web").await; });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = c.bind(echo, "web").await;
    assert!(r.is_err(), "duplicate bind should fail");
}

#[tokio::test]
async fn bind_reconnect() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let bind = AgentClient::connect(&ws_addr(b)).await.unwrap();
    let h = tokio::spawn({
        let c = bind.clone();
        async move { let _ = c.bind(echo, "web").await; }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let fwd = AgentClient::connect(&ws_addr(b)).await.unwrap();
    tokio::spawn({
        let c = fwd.clone();
        async move { let _ = c.forward(local, "web").await; }
    });
    wait_for_port(local).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    s.write_all(b"first\n").await.unwrap();
    assert_eq!(read_line(&mut s).await, "first\n");
    drop(s);

    h.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let bind2 = AgentClient::connect(&ws_addr(b)).await.unwrap();
    tokio::spawn({
        let c = bind2.clone();
        async move { let _ = c.bind(echo, "web").await; }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut s2 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    s2.write_all(b"reconnect\n").await.unwrap();
    assert_eq!(read_line(&mut s2).await, "reconnect\n");
}

#[tokio::test]
async fn concurrent_streams_100() {
    let (_b, local) = setup_tunnel().await;

    let mut hs = vec![];
    for i in 0..100 {
        let addr = format!("127.0.0.1:{local}");
        hs.push(tokio::spawn(async move {
            let mut c = TcpStream::connect(addr).await.unwrap();
            let m = format!("c{i}\n");
            c.write_all(m.as_bytes()).await.unwrap();
            let mut buf = [0u8; 64];
            let n = c.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], m.as_bytes(), "c{i}");
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn throughput_50x100kb() {
    let (_b, local) = setup_tunnel().await;

    let payload: Vec<u8> = (0..100 * 1024).map(|i| (i ^ (i >> 8)) as u8).collect();
    let mut hs = vec![];
    for i in 0..50 {
        let addr = format!("127.0.0.1:{local}");
        let p = payload.clone();
        hs.push(tokio::spawn(async move {
            let mut c = TcpStream::connect(addr).await.unwrap();
            c.write_all(&p).await.unwrap();
            let mut buf = vec![0u8; p.len()];
            c.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, p, "throughput {i}");
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn bind_target_unreachable_to_ok() {
    let b = start_broker().await;
    let closed = free_port().await;
    let local = free_port().await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .bind(closed, "web")
            .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr)
            .await
            .unwrap()
            .forward(local, "web")
            .await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"x\n").await.unwrap();
    let mut buf = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(2), c.read(&mut buf)).await;
    assert!(r.is_ok(), "bind unreachable should close");
    drop(c);

    start_echo(closed).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut c2 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c2.write_all(b"ok\n").await.unwrap();
    assert_eq!(read_line(&mut c2).await, "ok\n");
}
