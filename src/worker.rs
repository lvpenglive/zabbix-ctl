//! 后台工人：取 pending 任务 → 维护期 → 逐台作业 → 回读版本 → 关维护期。

use std::sync::Arc;

use serde_json::Value;
use sqlx::Row;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::AppConfig;
use crate::db::DbPool;
use crate::jobs::JobClient;
use crate::state::AppState;
use crate::zabbix::ZabbixClient;

const MAINTENANCE_SECS: i64 = 30 * 60;

pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = tick(&state).await {
                error!(error = %e, "任务工人出错");
            }
            sleep(Duration::from_secs(2)).await;
        }
    });
}

async fn tick(state: &AppState) -> anyhow::Result<()> {
    let Some(pool) = state.db.as_ref() else {
        return Ok(());
    };
    let row = sqlx::query(
        "SELECT id, instance_code, action, target_version, package_id, concurrency, requested_by \
         FROM zbx_tasks WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let task_id: String = row.try_get("id")?;
    let instance_code: String = row.try_get("instance_code")?;
    let action: String = row.try_get("action")?;
    let target_version: Option<String> = row.try_get("target_version")?;
    let package_id: Option<String> = row.try_get("package_id")?;
    let concurrency: i32 = row.try_get("concurrency").unwrap_or(10);
    let updated = sqlx::query(
        "UPDATE zbx_tasks SET status = 'running' WHERE id = ? AND status = 'pending'",
    )
    .bind(&task_id)
    .execute(pool)
    .await?
    .rows_affected();
    if updated == 0 {
        return Ok(());
    }
    info!(%task_id, %action, %instance_code, "开始执行批量任务");
    if let Err(e) = run_task(
        state,
        pool,
        &task_id,
        &instance_code,
        &action,
        target_version.as_deref(),
        package_id.as_deref(),
        concurrency.max(1) as usize,
    )
    .await
    {
        error!(%task_id, error = %e, "批量任务失败");
        let _ = sqlx::query(
            "UPDATE zbx_tasks SET status = 'failed', error = ?, finished_at = UTC_TIMESTAMP() WHERE id = ?",
        )
        .bind(e.to_string())
        .bind(&task_id)
        .execute(pool)
        .await;
    }
    Ok(())
}

async fn run_task(
    state: &AppState,
    pool: &DbPool,
    task_id: &str,
    instance_code: &str,
    action: &str,
    target_version: Option<&str>,
    package_id: Option<&str>,
    concurrency: usize,
) -> anyhow::Result<()> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == instance_code)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("未找到 Zabbix 实例 {instance_code}"))?;
    if action == "install" {
        return run_install(state, pool, task_id, instance_code, concurrency).await;
    }
    let zbx = ZabbixClient::new(state.http.clone(), instance)?;
    let hosts = sqlx::query(
        "SELECT host_id, ci_id, version_before FROM zbx_task_hosts WHERE task_id = ?",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await?;
    let host_ids: Vec<String> = hosts
        .iter()
        .filter_map(|r| r.try_get::<String, _>("host_id").ok())
        .collect();
    if host_ids.is_empty() {
        anyhow::bail!("任务没有主机");
    }

    let maintenance_id = zbx
        .create_maintenance(
            &format!("zabbix-ctl-{task_id}"),
            &host_ids,
            MAINTENANCE_SECS,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    sqlx::query("UPDATE zbx_tasks SET maintenance_id = ? WHERE id = ?")
        .bind(&maintenance_id)
        .bind(task_id)
        .execute(pool)
        .await?;

    let job_id = resolve_job_id(pool, &state.config.meridianops, action).await?;
    let default_install_path = state
        .config
        .meridianops
        .resolved_default_install_path()
        .to_string();
    let job_client = match (&job_id, MeridianOpsConfigExt::try_job_client(state)) {
        (Some(_), Ok(c)) => Some(c),
        (Some(_), Err(e)) => {
            warn!(error = %e, "作业客户端不可用");
            None
        }
        (None, _) => None,
    };

    let sem = Arc::new(Semaphore::new(concurrency));
    let mut joins = Vec::new();
    for row in hosts {
        let host_id: String = row.try_get("host_id")?;
        let ci_id: Option<String> = row.try_get("ci_id")?;
        let permit = sem.clone().acquire_owned().await?;
        let pool = pool.clone();
        let zbx_http = state.http.clone();
        let zbx_cfg = state
            .config
            .zabbix
            .iter()
            .find(|z| z.code == instance_code)
            .cloned()
            .unwrap();
        let action = action.to_string();
        let instance_code = instance_code.to_string();
        let default_install_path = default_install_path.clone();
        let target_version = target_version.map(|s| s.to_string());
        let package_id = package_id.map(|s| s.to_string());
        let job_id = job_id;
        let job_client = job_client.clone();
        let task_id = task_id.to_string();
        joins.push(tokio::spawn(async move {
            let _permit = permit;
            let result = process_host(
                &pool,
                &task_id,
                &instance_code,
                &host_id,
                ci_id.as_deref(),
                &action,
                target_version.as_deref(),
                package_id.as_deref(),
                default_install_path.as_str(),
                job_id,
                job_client.as_ref(),
                zbx_http,
                zbx_cfg,
            )
            .await;
            if let Err(e) = result {
                let _ = sqlx::query(
                    "UPDATE zbx_task_hosts SET status = 'failed', error = ? WHERE task_id = ? AND host_id = ?",
                )
                .bind(e.to_string())
                .bind(&task_id)
                .bind(&host_id)
                .execute(&pool)
                .await;
            }
        }));
    }
    for j in joins {
        let _ = j.await;
    }

    if let Err(e) = zbx.delete_maintenance(&maintenance_id).await {
        warn!(error = %e, %maintenance_id, "关闭维护期失败");
    }

    let counts = sqlx::query(
        "SELECT \
            CAST(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS SIGNED) AS ok_n, \
            CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS fail_n, \
            CAST(COUNT(*) AS SIGNED) AS total_n \
         FROM zbx_task_hosts WHERE task_id = ?",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await?;
    let ok_n: i64 = counts.try_get::<Option<i64>, _>("ok_n")?.unwrap_or(0);
    let fail_n: i64 = counts.try_get::<Option<i64>, _>("fail_n")?.unwrap_or(0);
    let total_n: i64 = counts.try_get::<Option<i64>, _>("total_n")?.unwrap_or(0);
    let status = if fail_n == 0 && ok_n == total_n {
        "succeeded"
    } else if ok_n == 0 {
        "failed"
    } else {
        "partial"
    };
    sqlx::query(
        "UPDATE zbx_tasks SET status = ?, finished_at = UTC_TIMESTAMP() WHERE id = ?",
    )
    .bind(status)
    .bind(task_id)
    .execute(pool)
    .await?;
    info!(%task_id, %status, ok_n, fail_n, "批量任务结束");
    Ok(())
}

struct MeridianOpsConfigExt;
impl MeridianOpsConfigExt {
    fn try_job_client(state: &AppState) -> Result<JobClient, crate::error::AppError> {
        JobClient::from_config(state.http.clone(), &state.config.meridianops)
    }
}

async fn resolve_job_id(
    pool: &DbPool,
    cfg: &crate::config::MeridianOpsConfig,
    action: &str,
) -> anyhow::Result<Option<i64>> {
    if let Some(id) = cfg.job_id_for(action) {
        return Ok(Some(id));
    }
    let Some(name) = cfg.job_name_for(action) else {
        return Ok(None);
    };
    let id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM job_definitions WHERE name = ? AND enabled = 1 ORDER BY id DESC LIMIT 1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(id)
}

async fn process_host(
    pool: &DbPool,
    task_id: &str,
    instance_code: &str,
    host_id: &str,
    ci_id: Option<&str>,
    action: &str,
    target_version: Option<&str>,
    package_id: Option<&str>,
    default_install_path: &str,
    job_id: Option<i64>,
    job_client: Option<&JobClient>,
    http: reqwest::Client,
    zbx_cfg: crate::config::ZabbixInstanceConfig,
) -> anyhow::Result<()> {
    let zbx = ZabbixClient::new(http, zbx_cfg)?;
    let before = zbx.agent_version(host_id).await.ok().flatten();
    sqlx::query(
        "UPDATE zbx_task_hosts SET status = 'running', version_before = COALESCE(?, version_before) \
         WHERE task_id = ? AND host_id = ?",
    )
    .bind(&before)
    .bind(task_id)
    .bind(host_id)
    .execute(pool)
    .await?;

    let Some(ci_id) = ci_id.filter(|s| !s.is_empty()) else {
        anyhow::bail!("未关联配置项，无法执行作业");
    };
    let Some(job_id) = job_id else {
        anyhow::bail!("未配置该动作的作业剧本 ID");
    };
    let Some(job_client) = job_client else {
        anyhow::bail!("作业客户端不可用，检查 MERIDIANOPS_JOB_TOKEN");
    };

    let link = sqlx::query(
        "SELECT install_path, package_id, proxy_id, proxy_group_id FROM zbx_host_links \
         WHERE instance_code = ? AND host_id = ?",
    )
    .bind(instance_code)
    .bind(host_id)
    .fetch_optional(pool)
    .await?;
    let install_path: String = link
        .as_ref()
        .and_then(|r| r.try_get::<Option<String>, _>("install_path").ok().flatten())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default_install_path.to_string());
    let mut env = serde_json::Map::new();
    env.insert(
        "ZBX_INSTALL_PATH".into(),
        serde_json::json!(install_path),
    );
    env.insert(
        "ZBX_INSTANCE_CODE".into(),
        serde_json::json!(instance_code),
    );
    // 对照表里的通道，供 Gateway 解析 SSH 跳板
    if let Some(link) = link.as_ref() {
        if let Ok(Some(pid)) = link.try_get::<Option<String>, _>("proxy_id") {
            if !pid.trim().is_empty() {
                env.insert("ZBX_PROXY_ID".into(), serde_json::json!(pid.trim()));
            }
        }
        if let Ok(Some(gid)) = link.try_get::<Option<String>, _>("proxy_group_id") {
            if !gid.trim().is_empty() {
                env.insert("ZBX_PROXY_GROUP_ID".into(), serde_json::json!(gid.trim()));
            }
        }
    }
    if matches!(action, "upgrade" | "rollback") {
        let pkg = package_id.map(str::trim).filter(|s| !s.is_empty()).unwrap_or("");
        if pkg.is_empty() {
            anyhow::bail!("升级/回滚未指定 Agent 包");
        }
        env.insert("ZBX_PACKAGE_ID".into(), serde_json::json!(pkg));
    }
    let env = serde_json::Value::Object(env);

    let wait_secs = if matches!(action, "upgrade" | "rollback") {
        900u64
    } else {
        600
    };
    let run_id = job_client
        .execute_with_env(job_id, ci_id, &env)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    sqlx::query("UPDATE zbx_task_hosts SET job_run_id = ? WHERE task_id = ? AND host_id = ?")
        .bind(run_id)
        .bind(task_id)
        .bind(host_id)
        .execute(pool)
        .await?;
    let overall = job_client
        .wait_run(run_id, wait_secs)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // 作业结束后稍等 Agent 上报版本
    sleep(Duration::from_secs(5)).await;
    let after = zbx.agent_version(host_id).await.ok().flatten();

    let ok = match action {
        "upgrade" | "rollback" => {
            if overall != "success" {
                false
            } else if let Some(target) = target_version.filter(|s| !s.is_empty()) {
                // 版本项可能尚未刷新；作业成功且版本未回读时先认成功
                match after.as_deref() {
                    Some(v) => v == target || v.contains(target),
                    None => true,
                }
            } else {
                true
            }
        }
        _ => overall == "success",
    };
    if ok {
        sqlx::query(
            "UPDATE zbx_task_hosts SET status = 'succeeded', version_after = ?, error = NULL \
             WHERE task_id = ? AND host_id = ?",
        )
        .bind(&after)
        .bind(task_id)
        .bind(host_id)
        .execute(pool)
        .await?;
        if matches!(action, "upgrade" | "rollback") {
            if let Some(pkg) = package_id.map(str::trim).filter(|s| !s.is_empty()) {
                let _ = sqlx::query(
                    "UPDATE zbx_host_links SET package_id = ?, agent_version = COALESCE(?, agent_version), \
                     synced_at = UTC_TIMESTAMP() \
                     WHERE instance_code = ? AND host_id = ?",
                )
                .bind(pkg)
                .bind(&after)
                .bind(instance_code)
                .bind(host_id)
                .execute(pool)
                .await;
            }
        }
    } else {
        let msg = format!("作业状态={overall}，版本前={:?}，版本后={:?}", before, after);
        sqlx::query(
            "UPDATE zbx_task_hosts SET status = 'failed', version_after = ?, error = ? \
             WHERE task_id = ? AND host_id = ?",
        )
        .bind(&after)
        .bind(msg)
        .bind(task_id)
        .bind(host_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn run_install(
    state: &AppState,
    pool: &DbPool,
    task_id: &str,
    instance_code: &str,
    concurrency: usize,
) -> anyhow::Result<()> {
    let task = sqlx::query(
        "SELECT server_addr, agent_port, server_active, proxy_id, proxy_group_id, package_id, install_path, conf_suffix, \
                disable_host, group_ids, template_ids FROM zbx_tasks WHERE id = ?",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await?;
    let server: String = task
        .try_get::<Option<String>, _>("server_addr")?
        .unwrap_or_default();
    let agent_port: String = task
        .try_get::<Option<String>, _>("agent_port")?
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "10050".to_string());
    let server_active: String = task
        .try_get::<Option<String>, _>("server_active")?
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| server.clone());
    let proxy_id: String = task
        .try_get::<Option<String>, _>("proxy_id")?
        .unwrap_or_default();
    let proxy_group_id: String = task
        .try_get::<Option<String>, _>("proxy_group_id")?
        .unwrap_or_default();
    let package_id: String = task
        .try_get::<Option<String>, _>("package_id")?
        .unwrap_or_default();
    let install_path: String = task
        .try_get::<Option<String>, _>("install_path")?
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            state
                .config
                .meridianops
                .resolved_default_install_path()
                .to_string()
        });
    let conf_suffix: String = task
        .try_get::<Option<String>, _>("conf_suffix")?
        .unwrap_or_default();
    let disable_host: bool = task.try_get::<i8, _>("disable_host").unwrap_or(0) == 1;
    let group_ids = parse_id_list(task.try_get::<Option<String>, _>("group_ids")?.as_deref());
    let template_ids = parse_id_list(task.try_get::<Option<String>, _>("template_ids")?.as_deref());
    let rows = sqlx::query(
        "SELECT host_id, ci_id FROM zbx_task_hosts WHERE task_id = ?",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        anyhow::bail!("任务没有配置项");
    }

    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == instance_code)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("未找到 Zabbix 实例 {instance_code}"))?;
    let zbx = ZabbixClient::new(state.http.clone(), instance.clone())?;
    let mut targets = Vec::new();
    let mut existing_hosts = Vec::new();
    for row in &rows {
        let placeholder: String = row.try_get("host_id")?;
        let ci_id: String = row
            .try_get::<Option<String>, _>("ci_id")?
            .unwrap_or_default();
        let ci = load_ci(pool, &ci_id).await?;
        if let Some(host_id) = zbx
            .find_existing_host(&ci.hostname, &ci.ip)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
        {
            existing_hosts.push(host_id);
        }
        targets.push((placeholder, ci));
    }
    let maintenance_id = if existing_hosts.is_empty() {
        None
    } else {
        let id = zbx
            .create_maintenance(
                &format!("zabbix-ctl-{task_id}"),
                &existing_hosts,
                MAINTENANCE_SECS,
            )
            .await
            .map_err(|e| anyhow::anyhow!("创建维护期失败，已停止安装: {e}"))?;
        sqlx::query("UPDATE zbx_tasks SET maintenance_id = ? WHERE id = ?")
            .bind(&id)
            .bind(task_id)
            .execute(pool)
            .await?;
        Some(id)
    };

    let job_id = resolve_job_id(pool, &state.config.meridianops, "install").await?;
    let job_client = match (&job_id, MeridianOpsConfigExt::try_job_client(state)) {
        (Some(_), Ok(c)) => Some(c),
        (Some(_), Err(e)) => {
            warn!(error = %e, "作业客户端不可用");
            None
        }
        (None, _) => None,
    };

    let sem = Arc::new(Semaphore::new(concurrency));
    let mut joins = Vec::new();
    for (placeholder, ci) in targets {
        let permit = sem.clone().acquire_owned().await?;
        let pool = pool.clone();
        let http = state.http.clone();
        let read_cfg = instance.clone();
        let write_cfg = instance.clone();
        let job_id = job_id;
        let job_client = job_client.clone();
        let task_id = task_id.to_string();
        let instance_code = instance_code.to_string();
        let server = server.clone();
        let agent_port = agent_port.clone();
        let server_active = server_active.clone();
        let proxy_id = proxy_id.clone();
        let proxy_group_id = proxy_group_id.clone();
        let package_id = package_id.clone();
        let install_path = install_path.clone();
        let conf_suffix = conf_suffix.clone();
        let group_ids = group_ids.clone();
        let template_ids = template_ids.clone();
        let ci_id = ci.id.clone();
        joins.push(tokio::spawn(async move {
            let _permit = permit;
            let result = process_install_host(
                &pool,
                &task_id,
                &instance_code,
                &placeholder,
                &ci,
                &server,
                &agent_port,
                &server_active,
                &proxy_id,
                &proxy_group_id,
                &package_id,
                &install_path,
                &conf_suffix,
                &group_ids,
                &template_ids,
                disable_host,
                job_id,
                job_client.as_ref(),
                http,
                read_cfg,
                write_cfg,
            )
            .await;
            if let Err(e) = result {
                let _ = sqlx::query(
                    "UPDATE zbx_task_hosts SET status = 'failed', error = ? WHERE task_id = ? AND ci_id = ?",
                )
                .bind(e.to_string())
                .bind(&task_id)
                .bind(&ci_id)
                .execute(&pool)
                .await;
            }
        }));
    }
    for join in joins {
        let _ = join.await;
    }
    if let Some(id) = maintenance_id {
        if let Err(e) = zbx.delete_maintenance(&id).await {
            warn!(error = %e, %id, "关闭维护期失败");
        }
    }
    finish_task(pool, task_id).await
}

struct InstallCi {
    id: String,
    hostname: String,
    display_name: String,
    ip: String,
}

async fn load_ci(pool: &DbPool, ci_id: &str) -> anyhow::Result<InstallCi> {
    let row = sqlx::query(
        "SELECT name, attributes FROM ci_instances WHERE id = ?",
    )
    .bind(ci_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("配置项不存在: {ci_id}"))?;
    let name: String = row.try_get("name").unwrap_or_default();
    let attributes: Value = row.try_get("attributes").unwrap_or(Value::Null);
    let ip = ["ip", "mgmt_ip"]
        .iter()
        .find_map(|key| {
            attributes
                .get(*key)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("配置项 {name} 没有管理 IP"))?;
    let hostname = attributes
        .get("hostname")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(name.as_str())
        .to_string();
    Ok(InstallCi {
        id: ci_id.to_string(),
        hostname,
        display_name: name,
        ip,
    })
}

async fn process_install_host(
    pool: &DbPool,
    task_id: &str,
    instance_code: &str,
    placeholder: &str,
    ci: &InstallCi,
    server: &str,
    agent_port: &str,
    server_active: &str,
    proxy_id: &str,
    proxy_group_id: &str,
    package_id: &str,
    install_path: &str,
    conf_suffix: &str,
    group_ids: &[String],
    template_ids: &[String],
    disable_host: bool,
    job_id: Option<i64>,
    job_client: Option<&JobClient>,
    http: reqwest::Client,
    read_cfg: crate::config::ZabbixInstanceConfig,
    write_cfg: crate::config::ZabbixInstanceConfig,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE zbx_task_hosts SET status = 'running' WHERE task_id = ? AND host_id = ?",
    )
    .bind(task_id)
    .bind(placeholder)
    .execute(pool)
    .await?;
    let Some(job_id) = job_id else {
        anyhow::bail!("未配置安装剧本。请确认作业中心存在「Zabbix Agent 安装」");
    };
    let Some(job_client) = job_client else {
        anyhow::bail!("作业客户端不可用，检查 MERIDIANOPS_JOB_TOKEN");
    };
    if package_id.trim().is_empty() {
        anyhow::bail!("未指定 Agent 安装包");
    }
    let via_proxy = !proxy_id.trim().is_empty() || !proxy_group_id.trim().is_empty();
    let mut env = serde_json::Map::new();
    env.insert("ZBX_HOSTNAME".into(), serde_json::json!(ci.hostname));
    env.insert("ZBX_PACKAGE_ID".into(), serde_json::json!(package_id.trim()));
    env.insert("ZBX_INSTALL_PATH".into(), serde_json::json!(install_path.trim()));
    env.insert(
        "ZBX_INSTANCE_CODE".into(),
        serde_json::json!(instance_code),
    );
    if !proxy_id.trim().is_empty() {
        env.insert("ZBX_PROXY_ID".into(), serde_json::json!(proxy_id.trim()));
    }
    if !proxy_group_id.trim().is_empty() {
        env.insert(
            "ZBX_PROXY_GROUP_ID".into(),
            serde_json::json!(proxy_group_id.trim()),
        );
    }
    if !conf_suffix.trim().is_empty() {
        env.insert("ZBX_CONF_SUFFIX".into(), serde_json::json!(conf_suffix.trim()));
    }
    if !server.trim().is_empty() {
        env.insert("ZBX_SERVER".into(), serde_json::json!(server.trim()));
    }
    if !server_active.trim().is_empty() {
        env.insert(
            "ZBX_SERVER_ACTIVE".into(),
            serde_json::json!(server_active.trim()),
        );
    }
    let listen_port = {
        let p = agent_port.trim();
        if p.is_empty() {
            "10050"
        } else {
            p
        }
    };
    env.insert("ZBX_LISTEN_PORT".into(), serde_json::json!(listen_port));
    let env = serde_json::Value::Object(env);
    let run_id = job_client
        .execute_with_env(job_id, &ci.id, &env)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    sqlx::query(
        "UPDATE zbx_task_hosts SET job_run_id = ? WHERE task_id = ? AND host_id = ?",
    )
    .bind(run_id)
    .bind(task_id)
    .bind(placeholder)
    .execute(pool)
    .await?;
    let overall = job_client
        .wait_run(run_id, 900)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if overall != "success" {
        if via_proxy {
            anyhow::bail!("安装作业状态={overall}。请检查包下发、SSH 凭据、conf 模板，以及主机到 Proxy 是否可达");
        }
        anyhow::bail!("安装作业状态={overall}。请检查包下发、SSH 凭据与 conf 模板");
    }

    let writer = ZabbixClient::for_write(http.clone(), write_cfg)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let mut register_body = serde_json::json!({
        "hostname": ci.hostname,
        "displayName": ci.display_name,
        "ip": ci.ip,
        "port": listen_port,
        "groupIds": group_ids,
        "templateIds": template_ids,
    });
    if !proxy_group_id.trim().is_empty() {
        register_body["proxyGroupId"] = serde_json::json!(proxy_group_id.trim());
    } else if !proxy_id.trim().is_empty() {
        register_body["proxyId"] = serde_json::json!(proxy_id.trim());
    }
    let registered = writer
        .register_host(&register_body)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let host_id = registered["hostId"].as_str().unwrap_or("").to_string();
    if host_id.is_empty() {
        anyhow::bail!("登记主机未返回 hostId");
    }
    sqlx::query(
        "UPDATE zbx_task_hosts SET host_id = ? WHERE task_id = ? AND host_id = ?",
    )
    .bind(&host_id)
    .bind(task_id)
    .bind(placeholder)
    .execute(pool)
    .await?;

    let reader = ZabbixClient::new(http, read_cfg).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let mut up = false;
    // 经 Proxy / Proxy Group 上报通常更慢，给约 5 分钟
    for _ in 0..30 {
        sleep(Duration::from_secs(10)).await;
        if reader
            .agent_ping_up(&host_id)
            .await
            .unwrap_or(false)
        {
            up = true;
            break;
        }
    }
    if !up {
        if via_proxy {
            anyhow::bail!("agent.ping 在 5 分钟内未通。安装与登记已完成，但经 Proxy/Proxy Group 未在时限内上报");
        }
        anyhow::bail!("agent.ping 在 5 分钟内未通。安装已完成，但主机到 Zabbix Server 10051 可能不通");
    }
    // 验证通后再禁用，避免安装阶段无法做 agent.ping，同时防止立刻告警
    if disable_host {
        writer
            .set_host_status(&host_id, 1)
            .await
            .map_err(|e| anyhow::anyhow!("验证通过但禁用主机失败: {e}"))?;
    }
    let proxy_id_store = if proxy_id.trim().is_empty() {
        None
    } else {
        Some(proxy_id.trim())
    };
    let proxy_group_id_store = if proxy_group_id.trim().is_empty() {
        None
    } else {
        Some(proxy_group_id.trim())
    };
    sqlx::query(
        "INSERT INTO zbx_host_links \
         (instance_code, host_id, hostname, ip, ci_id, agent_available, install_path, package_id, conf_suffix, \
          proxy_id, proxy_group_id, synced_at) \
         VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, UTC_TIMESTAMP()) \
         ON DUPLICATE KEY UPDATE hostname = VALUES(hostname), ip = VALUES(ip), ci_id = VALUES(ci_id), \
         agent_available = 1, install_path = VALUES(install_path), package_id = VALUES(package_id), \
         conf_suffix = VALUES(conf_suffix), proxy_id = VALUES(proxy_id), proxy_group_id = VALUES(proxy_group_id), \
         synced_at = UTC_TIMESTAMP()",
    )
    .bind(instance_code)
    .bind(&host_id)
    .bind(&ci.hostname)
    .bind(&ci.ip)
    .bind(&ci.id)
    .bind(install_path)
    .bind(if package_id.trim().is_empty() {
        None::<&str>
    } else {
        Some(package_id.trim())
    })
    .bind(if conf_suffix.trim().is_empty() {
        None::<&str>
    } else {
        Some(conf_suffix.trim())
    })
    .bind(proxy_id_store)
    .bind(proxy_group_id_store)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE zbx_task_hosts SET status = 'succeeded', error = NULL WHERE task_id = ? AND host_id = ?",
    )
    .bind(task_id)
    .bind(&host_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn parse_id_list(raw: Option<&str>) -> Vec<String> {
    raw.and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

async fn finish_task(pool: &DbPool, task_id: &str) -> anyhow::Result<()> {
    let counts = sqlx::query(
        "SELECT \
            CAST(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS SIGNED) AS ok_n, \
            CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS fail_n, \
            CAST(COUNT(*) AS SIGNED) AS total_n \
         FROM zbx_task_hosts WHERE task_id = ?",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await?;
    let ok_n: i64 = counts.try_get::<Option<i64>, _>("ok_n")?.unwrap_or(0);
    let fail_n: i64 = counts.try_get::<Option<i64>, _>("fail_n")?.unwrap_or(0);
    let total_n: i64 = counts.try_get::<Option<i64>, _>("total_n")?.unwrap_or(0);
    let status = if fail_n == 0 && ok_n == total_n {
        "succeeded"
    } else if ok_n == 0 {
        "failed"
    } else {
        "partial"
    };
    sqlx::query(
        "UPDATE zbx_tasks SET status = ?, finished_at = UTC_TIMESTAMP() WHERE id = ?",
    )
    .bind(status)
    .bind(task_id)
    .execute(pool)
    .await?;
    info!(%task_id, %status, ok_n, fail_n, "批量任务结束");
    Ok(())
}

/// 创建安装任务。host_id 先用占位符，安装并登记成功后再改成 Zabbix hostid。
pub async fn create_install_task(
    pool: &DbPool,
    cfg: &AppConfig,
    instance_code: &str,
    ci_ids: &[String],
    group_ids: &[String],
    template_ids: &[String],
    server: &str,
    agent_port: &str,
    server_active: &str,
    proxy_id: &str,
    proxy_group_id: &str,
    package_id: &str,
    install_path: &str,
    conf_suffix: &str,
    disable_host: bool,
    concurrency: i32,
    requested_by: &str,
) -> anyhow::Result<String> {
    if ci_ids.is_empty() {
        anyhow::bail!("配置项列表为空");
    }
    if group_ids.is_empty() {
        anyhow::bail!("请选择主机组");
    }
    if template_ids.is_empty() {
        anyhow::bail!("请选择模板");
    }
    let package_id = package_id.trim();
    if package_id.is_empty() {
        anyhow::bail!("请选择 Agent 安装包");
    }
    let conf_suffix = conf_suffix.trim();
    if server.trim().is_empty() && conf_suffix.is_empty() {
        anyhow::bail!("请填写 Server 地址，或指定 conf 模板后缀");
    }
    if server.trim().len() > 1024 || server_active.trim().len() > 1024 {
        anyhow::bail!("Server / ServerActive 地址过长");
    }
    let agent_port = {
        let p = agent_port.trim();
        if p.is_empty() {
            "10050".to_string()
        } else {
            let n: u16 = p
                .parse()
                .map_err(|_| anyhow::anyhow!("Agent 端口无效"))?;
            if n == 0 {
                anyhow::bail!("Agent 端口无效");
            }
            n.to_string()
        }
    };
    let install_path = install_path.trim();
    if install_path.is_empty() || !install_path.starts_with('/') || install_path.contains("..") {
        anyhow::bail!("安装目录无效");
    }
    let proxy_id = proxy_id.trim();
    let proxy_group_id = proxy_group_id.trim();
    if !proxy_id.is_empty() && !proxy_group_id.is_empty() {
        anyhow::bail!("不能同时指定 Proxy 与 Proxy Group");
    }
    if !proxy_id.is_empty() && !proxy_id.chars().all(|c| c.is_ascii_digit()) {
        anyhow::bail!("Proxy ID 无效");
    }
    if !proxy_group_id.is_empty() && !proxy_group_id.chars().all(|c| c.is_ascii_digit()) {
        anyhow::bail!("Proxy Group ID 无效");
    }
    if !cfg.zabbix.iter().any(|z| z.enabled && z.code == instance_code) {
        anyhow::bail!("未找到 Zabbix 实例 {instance_code}");
    }
    let task_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO zbx_tasks (id, instance_code, action, server_addr, agent_port, server_active, proxy_id, proxy_group_id, \
                package_id, install_path, conf_suffix, disable_host, group_ids, template_ids, \
                concurrency, status, requested_by, created_at) \
         VALUES (?, ?, 'install', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, UTC_TIMESTAMP())",
    )
    .bind(&task_id)
    .bind(instance_code)
    .bind(if server.trim().is_empty() {
        None::<&str>
    } else {
        Some(server.trim())
    })
    .bind(&agent_port)
    .bind(if server_active.trim().is_empty() {
        if server.trim().is_empty() {
            None::<&str>
        } else {
            Some(server.trim())
        }
    } else {
        Some(server_active.trim())
    })
    .bind(if proxy_id.is_empty() { None::<&str> } else { Some(proxy_id) })
    .bind(if proxy_group_id.is_empty() {
        None::<&str>
    } else {
        Some(proxy_group_id)
    })
    .bind(package_id)
    .bind(install_path)
    .bind(if conf_suffix.is_empty() {
        None::<&str>
    } else {
        Some(conf_suffix)
    })
    .bind(if disable_host { 1i8 } else { 0i8 })
    .bind(serde_json::to_string(group_ids)?)
    .bind(serde_json::to_string(template_ids)?)
    .bind(concurrency.clamp(1, 50))
    .bind(requested_by)
    .execute(pool)
    .await?;
    for ci_id in ci_ids {
        let placeholder = format!("p:{ci_id}");
        if placeholder.len() > 64 {
            anyhow::bail!("配置项标识过长: {ci_id}");
        }
        sqlx::query(
            "INSERT INTO zbx_task_hosts (task_id, host_id, ci_id, status) VALUES (?, ?, ?, 'pending')",
        )
        .bind(&task_id)
        .bind(&placeholder)
        .bind(ci_id)
        .execute(pool)
        .await?;
    }
    Ok(task_id)
}

/// 创建任务并写入主机明细。
pub async fn create_task(
    pool: &DbPool,
    cfg: &AppConfig,
    instance_code: &str,
    action: &str,
    host_ids: &[String],
    target_version: Option<&str>,
    package_id: Option<&str>,
    concurrency: i32,
    requested_by: &str,
) -> anyhow::Result<String> {
    match action {
        "start" | "stop" | "restart" | "upgrade" | "rollback" => {}
        _ => anyhow::bail!("不支持的动作: {action}"),
    }
    if host_ids.is_empty() {
        anyhow::bail!("主机列表为空");
    }
    if !cfg.zabbix.iter().any(|z| z.enabled && z.code == instance_code) {
        anyhow::bail!("未找到 Zabbix 实例 {instance_code}");
    }
    let package_id = package_id.map(str::trim).filter(|s| !s.is_empty());
    if matches!(action, "upgrade" | "rollback") && package_id.is_none() {
        anyhow::bail!("升级/回滚必须指定 packageId");
    }

    let task_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO zbx_tasks (id, instance_code, action, target_version, package_id, concurrency, status, requested_by, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, UTC_TIMESTAMP())",
    )
    .bind(&task_id)
    .bind(instance_code)
    .bind(action)
    .bind(target_version.filter(|s| !s.is_empty()))
    .bind(package_id)
    .bind(concurrency.clamp(1, 50))
    .bind(requested_by)
    .execute(pool)
    .await?;

    for host_id in host_ids {
        let link = sqlx::query(
            "SELECT ci_id, agent_version FROM zbx_host_links WHERE instance_code = ? AND host_id = ?",
        )
        .bind(instance_code)
        .bind(host_id)
        .fetch_optional(pool)
        .await?;
        let (ci_id, version_before): (Option<String>, Option<String>) = match link {
            Some(r) => (r.try_get("ci_id")?, r.try_get("agent_version")?),
            None => (None, None),
        };
        sqlx::query(
            "INSERT INTO zbx_task_hosts (task_id, host_id, ci_id, version_before, status) \
             VALUES (?, ?, ?, ?, 'pending')",
        )
        .bind(&task_id)
        .bind(host_id)
        .bind(ci_id)
        .bind(version_before)
        .execute(pool)
        .await?;
    }
    Ok(task_id)
}

pub async fn get_task(pool: &DbPool, task_id: &str) -> anyhow::Result<Option<Value>> {
    let task = sqlx::query(
        "SELECT id, instance_code, action, target_version, concurrency, maintenance_id, status, \
                requested_by, error, created_at, finished_at \
         FROM zbx_tasks WHERE id = ?",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    let Some(task) = task else {
        return Ok(None);
    };
    let hosts = sqlx::query(
        "SELECT host_id, ci_id, job_run_id, version_before, version_after, status, error \
         FROM zbx_task_hosts WHERE task_id = ? ORDER BY id",
    )
    .bind(task_id)
    .fetch_all(pool)
    .await?;
    let host_json: Vec<Value> = hosts
        .iter()
        .map(|r| {
            serde_json::json!({
                "hostId": r.try_get::<String, _>("host_id").unwrap_or_default(),
                "ciId": r.try_get::<Option<String>, _>("ci_id").ok().flatten(),
                "jobRunId": r.try_get::<Option<i64>, _>("job_run_id").ok().flatten(),
                "versionBefore": r.try_get::<Option<String>, _>("version_before").ok().flatten(),
                "versionAfter": r.try_get::<Option<String>, _>("version_after").ok().flatten(),
                "status": r.try_get::<String, _>("status").unwrap_or_default(),
                "error": r.try_get::<Option<String>, _>("error").ok().flatten(),
            })
        })
        .collect();
    Ok(Some(serde_json::json!({
        "id": task.try_get::<String, _>("id").unwrap_or_default(),
        "instanceCode": task.try_get::<String, _>("instance_code").unwrap_or_default(),
        "action": task.try_get::<String, _>("action").unwrap_or_default(),
        "targetVersion": task.try_get::<Option<String>, _>("target_version").ok().flatten(),
        "concurrency": task.try_get::<i32, _>("concurrency").unwrap_or(10),
        "maintenanceId": task.try_get::<Option<String>, _>("maintenance_id").ok().flatten(),
        "status": task.try_get::<String, _>("status").unwrap_or_default(),
        "requestedBy": task.try_get::<String, _>("requested_by").unwrap_or_default(),
        "error": task.try_get::<Option<String>, _>("error").ok().flatten(),
        "createdAt": format!("{:?}", task.try_get::<chrono::NaiveDateTime, _>("created_at").ok()),
        "finishedAt": task.try_get::<Option<chrono::NaiveDateTime>, _>("finished_at").ok().flatten().map(|t| format!("{t:?}")),
        "hosts": host_json,
    })))
}

/// 从 Zabbix 拉主机写入对照表，并按 IP 尝试匹配 ci_instances。
/// `host_ids` 非空时只同步指定主机；空则同步全部启用主机。
pub async fn sync_host_links(
    pool: &DbPool,
    state: &AppState,
    instance_code: &str,
    host_ids: Option<&[String]>,
) -> anyhow::Result<usize> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == instance_code)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("未找到实例"))?;
    let zbx = ZabbixClient::new(state.http.clone(), instance)?;
    // 同步对照只需主机/IP/Proxy，不拉 agent.ping（全量 item.get 易超时）
    let hosts = if let Some(ids) = host_ids.filter(|s| !s.is_empty()) {
        zbx.hosts_by_ids(ids)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
    } else {
        zbx.hosts_light(Some(0))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
    };
    let mut n = 0usize;
    for h in hosts {
        let host_id = h["hostId"].as_str().unwrap_or("");
        let hostname = h["hostname"].as_str().unwrap_or("");
        let ip = h["ip"].as_str().unwrap_or("");
        let version = h["agentVersion"].as_str();
        let available = match h["agentAvailable"].as_bool() {
            Some(true) => Some(1i8),
            Some(false) => Some(0i8),
            None => None,
        };
        let ci_id: Option<String> = if ip.is_empty() {
            None
        } else {
            sqlx::query_scalar(
                "SELECT id FROM ci_instances WHERE \
                 JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.ip')) = ? \
                 OR JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.mgmt_ip')) = ? \
                 OR JSON_UNQUOTE(JSON_EXTRACT(attributes, '$.bk_host_innerip')) = ? \
                 LIMIT 1",
            )
            .bind(ip)
            .bind(ip)
            .bind(ip)
            .fetch_optional(pool)
            .await?
        };
        let proxy_id = h["proxyId"].as_str().filter(|s| !s.is_empty());
        let proxy_group_id = h["proxyGroupId"].as_str().filter(|s| !s.is_empty());
        sqlx::query(
            "INSERT INTO zbx_host_links \
             (instance_code, host_id, hostname, ip, ci_id, agent_version, agent_available, \
              proxy_id, proxy_group_id, synced_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP()) \
             ON DUPLICATE KEY UPDATE hostname = VALUES(hostname), ip = VALUES(ip), \
               ci_id = COALESCE(VALUES(ci_id), ci_id), agent_version = VALUES(agent_version), \
               agent_available = VALUES(agent_available), \
               proxy_id = VALUES(proxy_id), proxy_group_id = VALUES(proxy_group_id), \
               synced_at = UTC_TIMESTAMP()",
        )
        .bind(instance_code)
        .bind(host_id)
        .bind(hostname)
        .bind(ip)
        .bind(ci_id)
        .bind(version)
        .bind(available)
        .bind(proxy_id)
        .bind(proxy_group_id)
        .execute(pool)
        .await?;
        n += 1;
    }
    Ok(n)
}
