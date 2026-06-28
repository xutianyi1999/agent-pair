use agent_pair::Broker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("Starting broker on 0.0.0.0:7799");
    Broker::new().listen("0.0.0.0:7799").await?;
    Ok(())
}
