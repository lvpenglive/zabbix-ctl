use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;

use crate::config::DatabaseConfig;

pub type DbPool = MySqlPool;

pub async fn connect(cfg: &DatabaseConfig) -> anyhow::Result<DbPool> {
    let pool = MySqlPoolOptions::new()
        .max_connections(cfg.max_connections)
        .connect(&cfg.url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}
