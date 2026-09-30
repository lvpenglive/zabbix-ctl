use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;

use crate::config::DatabaseConfig;

pub type DbPool = MySqlPool;

pub async fn connect(cfg: &DatabaseConfig) -> anyhow::Result<DbPool> {
    let pool = MySqlPoolOptions::new()
        .max_connections(cfg.max_connections)
        .connect(&cfg.url)
        .await?;
    // 与 Gateway 共用 meridianops 库和 _sqlx_migrations 表。
    // Gateway 已写入的版本对本进程不可见，需 ignore_missing，否则会拒绝启动。
    // zabbix-ctl 自己的 zbx_* 迁移版本号与 Gateway 不冲突，仍会正常 apply。
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator.run(&pool).await?;
    Ok(pool)
}
