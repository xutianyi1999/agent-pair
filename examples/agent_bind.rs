use agent_pair::AgentClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let server = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:7799".into());
    let port: u16 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8080);
    let label = std::env::args().nth(3).unwrap_or_else(|| "default".into());

    println!("bind: server={server}, local service on :{port}, label={label}");

    AgentClient::connect(&server)
        .await?
        .bind(port, &label)
        .await?;

    Ok(())
}
