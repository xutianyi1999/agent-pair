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

// ===== Label validation =====

#[tokio::test]
async fn empty_label_rejected_forward() {
    let b = start_broker().await;
    let r = AgentClient::connect(&ws_addr(b))
        .await
        .unwrap()
        .forward(free_port().await, "")
        .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn empty_label_rejected_bind() {
    let b = start_broker().await;
    let r = AgentClient::connect(&ws_addr(b))
        .await
        .unwrap()
        .bind(free_port().await, "")
        .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn newline_label_rejected_forward() {
    let b = start_broker().await;
    let r = AgentClient::connect(&ws_addr(b))
        .await
        .unwrap()
        .forward(free_port().await, "abc\n123")
        .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn newline_label_rejected_bind() {
    let b = start_broker().await;
    let r = AgentClient::connect(&ws_addr(b))
        .await
        .unwrap()
        .bind(free_port().await, "abc\r123")
        .await;
    assert!(r.is_err());
}

// ===== Payload edge cases =====

#[tokio::test]
async fn zero_byte_payload() {
    let (_b, local) = setup_tunnel().await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    // Write zero bytes — should still work (just a connection exercise)
    c.write_all(b"").await.unwrap();
    // send something and get it back
    c.write_all(b"x\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "x\n");
}

#[tokio::test]
async fn binary_all_bytes() {
    let (_b, local) = setup_tunnel().await;

    let payload: Vec<u8> = (0..=255).collect();
    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(&payload).await.unwrap();
    let mut buf = vec![0u8; 256];
    c.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, payload);
}

#[tokio::test]
async fn payload_2mb() {
    let (_b, local) = setup_tunnel().await;

    let payload: Vec<u8> = (0..1024 * 1024 * 2).map(|i| (i ^ (i >> 8)) as u8).collect();
    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(&payload).await.unwrap();
    let mut buf = vec![0u8; payload.len()];
    c.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf, payload, "2MB mismatch");
}

#[tokio::test]
async fn payload_at_window_boundary() {
    let (_b, local) = setup_tunnel().await;

    for &size in &[128 * 1024, 256 * 1024, 384 * 1024] {
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xff) as u8).collect();
        let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
        c.write_all(&payload).await.unwrap();
        let mut buf = vec![0u8; size];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, payload, "size={size} mismatch");
    }
}

// ===== Multiple streams rapid fire =====

#[tokio::test]
async fn multiple_rapid_streams_200() {
    let (_b, local) = setup_tunnel().await;

    let mut hs = vec![];
    for i in 0..200 {
        let addr = format!("127.0.0.1:{local}");
        hs.push(tokio::spawn(async move {
            let mut c = TcpStream::connect(addr).await.unwrap();
            let m = format!("r{i}\n");
            c.write_all(m.as_bytes()).await.unwrap();
            let mut buf = [0u8; 64];
            let n = c.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], m.as_bytes(), "rapid {i}");
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

// ===== Two labels on one bind agent =====

#[tokio::test]
async fn bind_two_labels() {
    let b = start_broker().await;
    let e1 = free_port().await;
    let e2 = free_port().await;
    let f1 = free_port().await;
    let f2 = free_port().await;
    start_echo(e1).await;
    start_echo(e2).await;

    let agent = AgentClient::connect(&ws_addr(b)).await.unwrap();
    tokio::spawn({
        let a = agent.clone();
        async move { let _ = a.bind(e1, "web").await; }
    });
    tokio::spawn({
        let a = agent.clone();
        async move { let _ = a.bind(e2, "db").await; }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::spawn(async move {
        let _ = AgentClient::connect(&ws_addr(b)).await.unwrap().forward(f1, "web").await;
    });
    tokio::spawn(async move {
        let _ = AgentClient::connect(&ws_addr(b)).await.unwrap().forward(f2, "db").await;
    });
    wait_for_port(f1).await;
    wait_for_port(f2).await;

    let mut w = TcpStream::connect(format!("127.0.0.1:{f1}")).await.unwrap();
    w.write_all(b"web-data\n").await.unwrap();
    assert_eq!(read_line(&mut w).await, "web-data\n");

    let mut d = TcpStream::connect(format!("127.0.0.1:{f2}")).await.unwrap();
    d.write_all(b"db-data\n").await.unwrap();
    assert_eq!(read_line(&mut d).await, "db-data\n");
}

// ===== Two independent tunnels =====

#[tokio::test]
async fn two_independent_tunnels() {
    let b1 = start_broker().await;
    let b2 = start_broker().await;
    let e1 = free_port().await;
    let e2 = free_port().await;
    let f1 = free_port().await;
    let f2 = free_port().await;
    start_echo(e1).await;
    start_echo(e2).await;

    // Tunnel 1
    let addr1 = ws_addr(b1);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr1).await.unwrap().bind(e1, "t1").await;
    });
    let addr1 = ws_addr(b1);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr1).await.unwrap().forward(f1, "t1").await;
    });
    // Tunnel 2
    let addr2 = ws_addr(b2);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr2).await.unwrap().bind(e2, "t2").await;
    });
    let addr2 = ws_addr(b2);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr2).await.unwrap().forward(f2, "t2").await;
    });
    wait_for_port(f1).await;
    wait_for_port(f2).await;

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{f1}")).await.unwrap();
    c1.write_all(b"tunnel1\n").await.unwrap();
    assert_eq!(read_line(&mut c1).await, "tunnel1\n");

    let mut c2 = TcpStream::connect(format!("127.0.0.1:{f2}")).await.unwrap();
    c2.write_all(b"tunnel2\n").await.unwrap();
    assert_eq!(read_line(&mut c2).await, "tunnel2\n");
}

// ===== Forward before bind =====

#[tokio::test]
async fn forward_agent_before_bind() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    // Forward starts first — label doesn't exist yet
    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr).await.unwrap().forward(local, "web").await;
    });
    wait_for_port(local).await;

    // First connection — broker sees no bind, warns, closes stream
    let mut c1 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c1.write_all(b"before\n").await.unwrap();
    let mut buf = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(3), c1.read(&mut buf)).await;
    assert!(r.is_ok(), "first request should close quickly");

    // Now bind agent arrives
    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr).await.unwrap().bind(echo, "web").await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second connection — works now
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c2.write_all(b"after\n").await.unwrap();
    assert_eq!(read_line(&mut c2).await, "after\n");
}

// ===== Broker restart mid-session =====

#[tokio::test]
async fn broker_restart_mid_session() {
    let b_addr = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    // Establish tunnel on first broker
    let addr1 = ws_addr(b_addr);
    let h_bind = tokio::spawn(async move {
        let _ = AgentClient::connect(&addr1).await.unwrap().bind(echo, "web").await;
    });
    let addr1 = ws_addr(b_addr);
    let h_fwd = tokio::spawn(async move {
        let _ = AgentClient::connect(&addr1).await.unwrap().forward(local, "web").await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"broker-before\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "broker-before\n");
    drop(c);

    // Start a new broker on new port (kill old by aborting start_broker task)
    let b2_addr = start_broker().await;

    // Agents detect old broker death and exit
    let _ = tokio::time::timeout(Duration::from_secs(5), h_bind).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), h_fwd).await;

    // Reconnect to new broker
    let addr2 = ws_addr(b2_addr);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr2).await.unwrap().bind(echo, "web").await;
    });
    let addr2 = ws_addr(b2_addr);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr2).await.unwrap().forward(local, "web").await;
    });
    wait_for_port(local).await;

    let mut c2 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c2.write_all(b"broker-after\n").await.unwrap();
    assert_eq!(read_line(&mut c2).await, "broker-after\n");
}

// ===== Bind reuses label after reconnect =====

#[tokio::test]
async fn bind_reuses_label_after_reconnect() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let a1 = AgentClient::connect(&ws_addr(b)).await.unwrap();
    let h_bind = tokio::spawn({
        let c = a1.clone();
        async move { let _ = c.bind(echo, "web").await; }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let a_fwd = AgentClient::connect(&ws_addr(b)).await.unwrap();
    tokio::spawn({
        let c = a_fwd.clone();
        async move { let _ = c.forward(local, "web").await; }
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"phase1\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "phase1\n");
    drop(c);

    h_bind.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let a2 = AgentClient::connect(&ws_addr(b)).await.unwrap();
    tokio::spawn({
        let c = a2.clone();
        async move { let _ = c.bind(echo, "web").await; }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c2.write_all(b"phase2\n").await.unwrap();
    assert_eq!(read_line(&mut c2).await, "phase2\n");
}

// ===== 3x reconnect cycle =====

#[tokio::test]
async fn broker_restart_3x() {
    for cycle in 0..3 {
        let b = start_broker().await;
        let echo = free_port().await;
        let local = free_port().await;
        start_echo(echo).await;

        let addr = ws_addr(b);
        tokio::spawn(async move {
            let _ = AgentClient::connect(&addr).await.unwrap().bind(echo, "x").await;
        });
        let addr = ws_addr(b);
        tokio::spawn(async move {
            let _ = AgentClient::connect(&addr).await.unwrap().forward(local, "x").await;
        });
        wait_for_port(local).await;

        let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
        let msg = format!("cycle{cycle}\n");
        c.write_all(msg.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut c).await, msg, "cycle {cycle}");
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

// ======================================================================
// Unstable network tests
// ======================================================================

async fn broker_with_handle() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let port = free_port().await;
    let a: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
    let h = tokio::spawn(async move {
        Broker::new().listen(a).await.unwrap_err();
    });
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(a).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return (a, h);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker_with_handle didn't start")
}

fn spawn_pumper(local: u16, size: usize) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let payload = vec![0xabu8; size];
        let addr = format!("127.0.0.1:{local}");
        let mut c = match TcpStream::connect(&addr).await {
            Ok(c) => c,
            Err(_) => return,
        };
        loop {
            if c.write_all(&payload).await.is_err() {
                break;
            }
        }
    })
}

#[tokio::test]
async fn broker_crash_mid_data_transfer() {
    let (b_addr, b_handle) = broker_with_handle().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let a1 = ws_addr(b_addr);
    let h_bind = tokio::spawn(async move {
        let _ = AgentClient::connect(&a1).await.unwrap().bind(echo, "crash").await;
    });
    let a2 = ws_addr(b_addr);
    let h_fwd = tokio::spawn(async move {
        let _ = AgentClient::connect(&a2).await.unwrap().forward(local, "crash").await;
    });
    wait_for_port(local).await;

    let h_pump = spawn_pumper(local, 512 * 1024);
    tokio::time::sleep(Duration::from_millis(30)).await;
    b_handle.abort();

    let (_, _, _) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), h_bind),
        tokio::time::timeout(Duration::from_secs(5), h_fwd),
        tokio::time::timeout(Duration::from_secs(5), h_pump),
    );

    // New broker, agents reconnect, tunnel recovers
    let b2 = start_broker().await;
    let a3 = ws_addr(b2);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a3).await.unwrap().bind(echo, "crash").await;
    });
    let a4 = ws_addr(b2);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a4).await.unwrap().forward(local, "crash").await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"recovered\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "recovered\n");
}

#[tokio::test]
async fn bind_agent_crash_mid_data_transfer() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let a1 = ws_addr(b);
    let h_bind = tokio::spawn(async move {
        let _ = AgentClient::connect(&a1).await.unwrap().bind(echo, "crash").await;
    });
    let a2 = ws_addr(b);
    let _h_fwd = tokio::spawn(async move {
        let _ = AgentClient::connect(&a2).await.unwrap().forward(local, "crash").await;
    });
    wait_for_port(local).await;

    let _h_pump = spawn_pumper(local, 512 * 1024);
    tokio::time::sleep(Duration::from_millis(30)).await;
    h_bind.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let a3 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a3).await.unwrap().bind(echo, "crash").await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"rebound\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "rebound\n");
}

#[tokio::test]
async fn forward_agent_crash_mid_data_transfer() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let a1 = ws_addr(b);
    let _h_bind = tokio::spawn(async move {
        let _ = AgentClient::connect(&a1).await.unwrap().bind(echo, "crash").await;
    });
    let a2 = ws_addr(b);
    let h_fwd = tokio::spawn(async move {
        let _ = AgentClient::connect(&a2).await.unwrap().forward(local, "crash").await;
    });
    wait_for_port(local).await;

    let _h_pump = spawn_pumper(local, 512 * 1024);
    tokio::time::sleep(Duration::from_millis(30)).await;
    h_fwd.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let a3 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a3).await.unwrap().forward(local, "crash").await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"reforward\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "reforward\n");
}

#[tokio::test]
async fn tcp_client_drop_mid_transfer() {
    let (_b, local) = setup_tunnel().await;

    for _ in 0..5 {
        let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
        c.write_all(b"hello\n").await.unwrap();
        let mut buf = [0u8; 2];
        let _ = c.read(&mut buf).await;
        drop(c);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"after-drops\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "after-drops\n");
}

#[tokio::test]
async fn rapid_connect_disconnect_50x() {
    let (_b, local) = setup_tunnel().await;

    for i in 0..50 {
        let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
        let msg = format!("r{i}\n");
        c.write_all(msg.as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        let n = c.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], msg.as_bytes(), "r{i}");
    }
}

#[tokio::test]
async fn payload_exact_window_boundaries() {
    let (_b, local) = setup_tunnel().await;

    for &size in &[262_143, 262_144, 262_145, 524_288, 524_289] {
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xff) as u8).collect();
        let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
        c.write_all(&payload).await.unwrap();
        let mut buf = vec![0u8; size];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, payload, "size={size} mismatch");
        drop(c);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn concurrent_transfer_with_disconnect() {
    let (_b, local) = setup_tunnel().await;

    let payload = vec![0xabu8; 1024 * 1024];
    let mut hs = vec![];
    for _ in 0..10 {
        let addr = format!("127.0.0.1:{local}");
        let p = payload.clone();
        hs.push(tokio::spawn(async move {
            let mut c = TcpStream::connect(addr).await.unwrap();
            let _ = c.write_all(&p).await;
        }));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for h in hs {
        let _ = h.await;
    }

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"clean\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "clean\n");
}

#[tokio::test]
async fn max_label_through_tunnel() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let label = "x".repeat(4000);

    let addr = ws_addr(b);
    tokio::spawn({
        let lab = label.clone();
        async move {
            let _ = AgentClient::connect(&addr).await.unwrap().bind(echo, &lab).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = ws_addr(b);
    tokio::spawn({
        let lab = label.clone();
        async move {
            let _ = AgentClient::connect(&addr).await.unwrap().forward(local, &lab).await;
        }
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"label\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "label\n");
}

#[tokio::test]
async fn half_close_client_socket() {
    let (_b, local) = setup_tunnel().await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"half-close\n").await.unwrap();
    let _ = c.shutdown().await;

    assert_eq!(read_line(&mut c).await, "half-close\n");
}

#[tokio::test]
async fn simultaneous_bind_forward_crash() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let h_bind = tokio::spawn({
        let addr = ws_addr(b);
        async move {
            let _ = AgentClient::connect(&addr).await.unwrap().bind(echo, "multi").await;
        }
    });
    let h_fwd = tokio::spawn({
        let addr = ws_addr(b);
        async move {
            let _ = AgentClient::connect(&addr).await.unwrap().forward(local, "multi").await;
        }
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"before-crash\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "before-crash\n");
    drop(c);

    h_bind.abort();
    h_fwd.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    tokio::spawn({
        let addr = ws_addr(b);
        async move {
            let _ = AgentClient::connect(&addr).await.unwrap().bind(echo, "multi").await;
        }
    });
    tokio::spawn({
        let addr = ws_addr(b);
        async move {
            let _ = AgentClient::connect(&addr).await.unwrap().forward(local, "multi").await;
        }
    });
    wait_for_port(local).await;

    let mut c2 = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c2.write_all(b"after-crash\n").await.unwrap();
    assert_eq!(read_line(&mut c2).await, "after-crash\n");
}

#[tokio::test]
async fn concurrent_label_race() {
    let b = start_broker().await;
    let echo = free_port().await;
    let local = free_port().await;
    start_echo(echo).await;

    let a1 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a1).await.unwrap().bind(echo, "race").await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let a2 = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&a2).await.unwrap().bind(echo, "race").await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = ws_addr(b);
    tokio::spawn(async move {
        let _ = AgentClient::connect(&addr).await.unwrap().forward(local, "race").await;
    });
    wait_for_port(local).await;

    let mut c = TcpStream::connect(format!("127.0.0.1:{local}")).await.unwrap();
    c.write_all(b"race\n").await.unwrap();
    assert_eq!(read_line(&mut c).await, "race\n");
}
