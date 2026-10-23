//! Thin CLI wrapper around the in-gateway migration module.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    queqiao_router::db::migrate::run_cli().await
}
