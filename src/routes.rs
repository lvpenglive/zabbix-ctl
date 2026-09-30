use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::auth::ServiceAuth;
use crate::error::AppError;
use crate::state::AppState;
use crate::worker;
use crate::zabbix::ZabbixClient;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/api/instances", get(list_instances))
        .route("/api/instances/:code/version", get(instance_version))
        .route("/api/instances/:code/hosts", get(list_hosts))
        .route(
            "/api/instances/:code/hosts/:host_id/problems",
            get(host_problems),
        )
        .route(
            "/api/instances/:code/hosts/:host_id/metrics",
            get(host_metrics),
        )
        .route("/api/instances/:code/sync", post(sync_hosts))
        .route("/api/instances/:code/governance", get(governance))
        .route("/api/tasks", post(create_task))
        .route("/api/tasks/:id", get(get_task))
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "code": 0,
        "data": {
            "status": "ok",
            "db": state.db.is_some(),
        }
    }))
}

async fn list_instances(
    State(state): State<AppState>,
    _auth: ServiceAuth,
) -> Json<serde_json::Value> {
    let items: Vec<_> = state
        .config
        .zabbix
        .iter()
        .filter(|z| z.enabled)
        .map(|z| {
            json!({
                "code": z.code,
                "name": z.name,
                "tokenConfigured": z.resolved_api_token().is_some(),
            })
        })
        .collect();
    Json(json!({ "code": 0, "data": items }))
}

async fn instance_version(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let client = client_for(&state, &code)?;
    let version = client.version().await?;
    Ok(Json(json!({
        "code": 0,
        "data": { "code": code, "version": version }
    })))
}

#[derive(Debug, Deserialize)]
struct HostListQuery {
    limit: Option<u32>,
    search: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MetricsQuery {
    hours: Option<u32>,
}

async fn list_hosts(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    Query(q): Query<HostListQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let client = client_for(&state, &code)?;
    let mut hosts = client
        .list_hosts(q.limit.unwrap_or(100), q.search.as_deref())
        .await?;
    if let Some(pool) = state.db.as_ref() {
        enrich_hosts_with_links(pool, &code, &mut hosts).await?;
    }
    Ok(Json(json!({ "code": 0, "data": hosts })))
}

async fn enrich_hosts_with_links(
    pool: &crate::db::DbPool,
    instance_code: &str,
    hosts: &mut [serde_json::Value],
) -> Result<(), AppError> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT host_id, ci_id, agent_version, agent_available FROM zbx_host_links WHERE instance_code = ?",
    )
    .bind(instance_code)
    .fetch_all(pool)
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;
    let mut map = std::collections::HashMap::new();
    for r in rows {
        let host_id: String = r.try_get("host_id").unwrap_or_default();
        let ci_id: Option<String> = r.try_get("ci_id").ok().flatten();
        map.insert(host_id, ci_id);
    }
    for h in hosts.iter_mut() {
        let id = h["hostId"].as_str().unwrap_or("").to_string();
        let linked = map.get(&id).cloned().flatten();
        h["ciId"] = json!(linked);
        h["cmdbLinked"] = json!(h["ciId"].as_str().map(|s| !s.is_empty()).unwrap_or(false));
    }
    Ok(())
}

async fn host_problems(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let client = client_for(&state, &code)?;
    let problems = client.host_problems(&host_id).await?;
    Ok(Json(json!({ "code": 0, "data": problems })))
}

async fn host_metrics(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
    Query(q): Query<MetricsQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let client = client_for(&state, &code)?;
    let series = client
        .host_metrics(&host_id, q.hours.unwrap_or(24))
        .await?;
    Ok(Json(json!({ "code": 0, "data": series })))
}

async fn sync_hosts(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let pool = state
        .db
        .as_ref()
        .ok_or_else(|| AppError::bad("数据库未配置，无法同步主机对照"))?;
    let n = worker::sync_host_links(pool, &state, &code)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(Json(json!({ "code": 0, "data": { "synced": n } })))
}

async fn governance(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == code)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("未找到 Zabbix 实例 {code}")))?;
    let client = ZabbixClient::new(state.http.clone(), instance.clone())?;
    let data = client
        .governance(
            &instance.standard_templates,
            instance.proxy_stale_secs,
            instance.queue_warn,
        )
        .await?;
    Ok(Json(json!({ "code": 0, "data": data })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateTaskRequest {
    instance_code: String,
    action: String,
    host_ids: Vec<String>,
    target_version: Option<String>,
    concurrency: Option<i32>,
    requested_by: Option<String>,
}

async fn create_task(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Json(req): Json<CreateTaskRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let pool = state
        .db
        .as_ref()
        .ok_or_else(|| AppError::bad("数据库未配置，无法创建任务"))?;
    let id = worker::create_task(
        pool,
        &state.config,
        &req.instance_code,
        &req.action,
        &req.host_ids,
        req.target_version.as_deref(),
        req.concurrency.unwrap_or(10),
        req.requested_by.as_deref().unwrap_or(""),
    )
    .await
    .map_err(|e| AppError::bad(e.to_string()))?;
    Ok(Json(json!({ "code": 0, "data": { "id": id } })))
}

async fn get_task(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let pool = state
        .db
        .as_ref()
        .ok_or_else(|| AppError::bad("数据库未配置"))?;
    let task = worker::get_task(pool, &id)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound(format!("任务不存在: {id}")))?;
    Ok(Json(json!({ "code": 0, "data": task })))
}

fn client_for(state: &AppState, code: &str) -> Result<ZabbixClient, AppError> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == code)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("未找到 Zabbix 实例 {code}")))?;
    ZabbixClient::new(state.http.clone(), instance)
}
