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
        "SELECT id, instance_code, action, target_version, concurrency, requested_by \
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
    concurrency: usize,
) -> anyhow::Result<()> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == instance_code)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("未找到 Zabbix 实例 {instance_code}"))?;
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
        let target_version = target_version.map(|s| s.to_string());
        let job_id = job_id;
        let job_client = job_client.clone();
        let task_id = task_id.to_string();
        joins.push(tokio::spawn(async move {
            let _permit = permit;
            let result = process_host(
                &pool,
                &task_id,
                &host_id,
                ci_id.as_deref(),
                &action,
                target_version.as_deref(),
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
            SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS ok_n, \
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS fail_n, \
            COUNT(*) AS total_n \
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
    host_id: &str,
    ci_id: Option<&str>,
    action: &str,
    target_version: Option<&str>,
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

    let run_id = job_client
        .execute(job_id, ci_id)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    sqlx::query("UPDATE zbx_task_hosts SET job_run_id = ? WHERE task_id = ? AND host_id = ?")
        .bind(run_id)
        .bind(task_id)
        .bind(host_id)
        .execute(pool)
        .await?;
    let overall = job_client
        .wait_run(run_id, 600)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // 作业结束后稍等 Agent 上报版本
    sleep(Duration::from_secs(5)).await;
    let after = zbx.agent_version(host_id).await.ok().flatten();

    let ok = match action {
        "upgrade" | "rollback" => {
            if let Some(target) = target_version {
                after.as_deref() == Some(target)
            } else {
                overall == "success"
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

/// 创建任务并写入主机明细。
pub async fn create_task(
    pool: &DbPool,
    cfg: &AppConfig,
    instance_code: &str,
    action: &str,
    host_ids: &[String],
    target_version: Option<&str>,
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
    if matches!(action, "upgrade" | "rollback") && target_version.unwrap_or("").is_empty() {
        anyhow::bail!("升级/回滚必须指定 targetVersion");
    }

    let task_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO zbx_tasks (id, instance_code, action, target_version, concurrency, status, requested_by, created_at) \
         VALUES (?, ?, ?, ?, ?, 'pending', ?, UTC_TIMESTAMP())",
    )
    .bind(&task_id)
    .bind(instance_code)
    .bind(action)
    .bind(target_version)
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
pub async fn sync_host_links(
    pool: &DbPool,
    state: &AppState,
    instance_code: &str,
) -> anyhow::Result<usize> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == instance_code)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("未找到实例"))?;
    let zbx = ZabbixClient::new(state.http.clone(), instance)?;
    let hosts = zbx
        .list_hosts(None, None)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
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
        sqlx::query(
            "INSERT INTO zbx_host_links \
             (instance_code, host_id, hostname, ip, ci_id, agent_version, agent_available, synced_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP()) \
             ON DUPLICATE KEY UPDATE hostname = VALUES(hostname), ip = VALUES(ip), \
               ci_id = COALESCE(VALUES(ci_id), ci_id), agent_version = VALUES(agent_version), \
               agent_available = VALUES(agent_available), synced_at = UTC_TIMESTAMP()",
        )
        .bind(instance_code)
        .bind(host_id)
        .bind(hostname)
        .bind(ip)
        .bind(ci_id)
        .bind(version)
        .bind(available)
        .execute(pool)
        .await?;
        n += 1;
    }
    Ok(n)
}
