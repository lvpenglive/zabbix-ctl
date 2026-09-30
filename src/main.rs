mod auth;
mod config;
mod db;
mod error;
mod jobs;
mod routes;
mod state;
mod worker;
mod zabbix;

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::config::load;
use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("zabbix_ctl=info".parse()?))
        .init();

    let path = env::var("ZABBIX_CTL_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("zabbix-ctl.toml"));
    let cfg = load(&path)?;
    let bind: SocketAddr = cfg.server.bind.parse()?;
    let service_token = cfg.server.resolved_service_token();
    if service_token.is_empty() {
        tracing::warn!("未配置 service_token（toml [server] 或环境变量 ZABBIX_CTL_SERVICE_TOKEN），除 /health 外的接口会拒绝访问");
    }

    let db = match db::connect(&cfg.database).await {
        Ok(pool) => {
            tracing::info!("数据库已连接，迁移完成");
            Some(pool)
        }
        Err(e) => {
            tracing::warn!(error = %e, "数据库连接失败，只读接口可用，任务功能不可用");
            None
        }
    };

    let state = AppState {
        config: Arc::new(cfg),
        http: reqwest::Client::builder().build()?,
        service_token,
        db,
    };

    if state.db.is_some() {
        worker::spawn(state.clone());
    }

    let app = routes::router()
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "zabbix-ctl 已启动");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("正在退出");
}
