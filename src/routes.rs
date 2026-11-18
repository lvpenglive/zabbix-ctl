use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::auth::ServiceAuth;
use crate::designer;
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
        .route(
            "/api/instances/:code/hosts/:host_id/items",
            get(host_items),
        )
        .route(
            "/api/instances/:code/hosts/:host_id/triggers",
            get(host_triggers_list),
        )
        .route("/api/instances/:code/sync", post(sync_hosts))
        .route("/api/instances/:code/governance", get(governance))
        .route(
            "/api/instances/:code/template-groups",
            get(template_groups).post(designer::create_template_group),
        )
        .route(
            "/api/instances/:code/host-groups",
            get(host_groups).post(designer::create_host_group),
        )
        .route(
            "/api/instances/:code/host-groups/:group_id/hosts",
            get(host_group_hosts),
        )
        .route(
            "/api/instances/:code/templates",
            get(templates).post(designer::create_template),
        )
        .route(
            "/api/instances/:code/hosts/:host_id/linkage",
            get(host_linkage),
        )
        .route(
            "/api/instances/:code/hosts/:host_id/templates",
            post(link_templates),
        )
        .route(
            "/api/instances/:code/hosts/:host_id/groups",
            post(link_groups),
        )
        .route(
            "/api/instances/:code/hosts/:host_id/macros",
            get(host_macros).put(save_host_macros),
        )
        .route("/api/instances/:code/hosts", post(register_host))
        .route("/api/instances/:code/events/:event_id", get(event_hosts))
        .route(
            "/api/instances/:code/events/:event_id/ack",
            post(ack_event),
        )
        .route("/api/tasks", post(create_task))
        .route("/api/tasks/:id", get(get_task))
        .merge(designer::routes())
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
                "protected": z.protected,
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
#[serde(rename_all = "camelCase")]
struct HostListQuery {
    search: Option<String>,
    page: Option<u32>,
    page_size: Option<u32>,
    group_id: Option<String>,
    /// online / offline / unknown
    agent: Option<String>,
    /// linked / unlinked
    cmdb: Option<String>,
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
    let mut hosts = client.enabled_hosts_light().await?;
    if let Some(pool) = state.db.as_ref() {
        enrich_hosts_with_links(pool, &code, &mut hosts).await?;
    }
    let total = hosts.len();
    let groups = group_counts(&hosts);
    let matched_rows = filter_roster(&hosts, &q);
    let matched = matched_rows.len();
    let page_size = q.page_size.unwrap_or(20).clamp(1, 100);
    let page = q.page.unwrap_or(1).max(1);
    let start = ((page - 1) as usize).saturating_mul(page_size as usize);
    let mut items = if start >= matched {
        Vec::new()
    } else {
        let end = (start + page_size as usize).min(matched);
        matched_rows[start..end].to_vec()
    };
    client.attach_agent_versions(&mut items).await?;
    Ok(Json(json!({
        "code": 0,
        "data": {
            "total": total,
            "matched": matched,
            "page": page,
            "pageSize": page_size,
            "groups": groups,
            "items": items,
        }
    })))
}

fn group_counts(hosts: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut counts: std::collections::HashMap<String, (String, usize)> = std::collections::HashMap::new();
    for host in hosts {
        let Some(groups) = host["groups"].as_array() else {
            continue;
        };
        for group in groups {
            let id = group["groupId"].as_str().unwrap_or("").trim();
            if id.is_empty() {
                continue;
            }
            let name = group["name"].as_str().unwrap_or("").to_string();
            let entry = counts.entry(id.to_string()).or_insert((name, 0));
            entry.1 += 1;
        }
    }
    let mut rows: Vec<_> = counts
        .into_iter()
        .filter(|(_, (_, n))| *n > 0)
        .map(|(id, (name, n))| (id, name, n))
        .collect();
    rows.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.1.cmp(&b.1)));
    rows.into_iter()
        .map(|(id, name, n)| json!({ "groupId": id, "name": name, "hostCount": n }))
        .collect()
}

fn filter_roster(hosts: &[serde_json::Value], q: &HostListQuery) -> Vec<serde_json::Value> {
    let search = q
        .search
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase());
    let group_id = q.group_id.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let agent = q.agent.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let cmdb = q.cmdb.as_deref().map(str::trim).filter(|s| !s.is_empty());
    hosts
        .iter()
        .filter(|host| {
            if let Some(keyword) = &search {
                let blob = format!(
                    "{} {} {}",
                    host["displayName"].as_str().unwrap_or(""),
                    host["hostname"].as_str().unwrap_or(""),
                    host["ip"].as_str().unwrap_or("")
                );
                if !blob.to_lowercase().contains(keyword) {
                    return false;
                }
            }
            if let Some(gid) = group_id {
                let hit = host["groups"].as_array().is_some_and(|groups| {
                    groups.iter().any(|g| g["groupId"].as_str() == Some(gid))
                });
                if !hit {
                    return false;
                }
            }
            if let Some(flag) = agent {
                let available = host["agentAvailable"].as_bool();
                let ok = match flag {
                    "online" => available == Some(true),
                    "offline" => available == Some(false),
                    "unknown" => available.is_none(),
                    _ => true,
                };
                if !ok {
                    return false;
                }
            }
            if let Some(flag) = cmdb {
                let linked = host["cmdbLinked"].as_bool().unwrap_or(false);
                let ok = match flag {
                    "linked" => linked,
                    "unlinked" => !linked,
                    _ => true,
                };
                if !ok {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect()
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

#[derive(Debug, Deserialize)]
struct HostItemsQuery {
    search: Option<String>,
}

async fn host_items(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
    Query(q): Query<HostItemsQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let client = client_for(&state, &code)?;
    let items = client
        .host_items(&host_id, q.search.as_deref())
        .await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn host_triggers_list(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let client = client_for(&state, &code)?;
    let triggers = client.host_triggers(&host_id).await?;
    Ok(Json(json!({ "code": 0, "data": triggers })))
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
    #[serde(default)]
    host_ids: Vec<String>,
    target_version: Option<String>,
    concurrency: Option<i32>,
    requested_by: Option<String>,
    #[serde(default)]
    ci_ids: Vec<String>,
    #[serde(default)]
    group_ids: Vec<String>,
    #[serde(default)]
    template_ids: Vec<String>,
    server: Option<String>,
    server_active: Option<String>,
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
    let requested_by = req.requested_by.as_deref().unwrap_or("");
    let concurrency = req.concurrency.unwrap_or(10);
    let id = if req.action == "install" {
        worker::create_install_task(
            pool,
            &state.config,
            &req.instance_code,
            &req.ci_ids,
            &req.group_ids,
            &req.template_ids,
            req.server.as_deref().unwrap_or(""),
            req.server_active.as_deref().unwrap_or(""),
            concurrency,
            requested_by,
        )
        .await
    } else {
        worker::create_task(
            pool,
            &state.config,
            &req.instance_code,
            &req.action,
            &req.host_ids,
            req.target_version.as_deref(),
            concurrency,
            requested_by,
        )
        .await
    }
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

#[derive(Debug, Deserialize)]
struct TemplateQuery {
    #[serde(rename = "groupId")]
    group_id: Option<String>,
}

async fn template_groups(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let items = client_for(&state, &code)?.list_template_groups().await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn host_groups(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let items = client_for(&state, &code)?.list_host_groups().await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn host_group_hosts(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, group_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let items = client_for(&state, &code)?.hosts_in_group(&group_id).await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn templates(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    Query(q): Query<TemplateQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let items = client_for(&state, &code)?
        .list_templates(q.group_id.as_deref())
        .await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn host_linkage(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let data = client_for(&state, &code)?.host_linkage(&host_id).await?;
    Ok(Json(json!({ "code": 0, "data": data })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinkRequest {
    action: String,
    template_ids: Option<Vec<String>>,
    group_ids: Option<Vec<String>>,
}

async fn link_templates(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
    Json(req): Json<LinkRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let data = client_for_write(&state, &code)?
        .link_templates(
            &host_id,
            req.template_ids.as_deref().unwrap_or(&[]),
            &req.action,
        )
        .await?;
    Ok(Json(json!({ "code": 0, "data": data })))
}

async fn link_groups(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
    Json(req): Json<LinkRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let data = client_for_write(&state, &code)?
        .link_groups(
            &host_id,
            req.group_ids.as_deref().unwrap_or(&[]),
            &req.action,
        )
        .await?;
    Ok(Json(json!({ "code": 0, "data": data })))
}

async fn host_macros(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let items = client_for(&state, &code)?.host_macros(&host_id).await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

#[derive(Debug, Deserialize)]
struct MacroListRequest {
    macros: Vec<serde_json::Value>,
}

async fn save_host_macros(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, host_id)): Path<(String, String)>,
    Json(req): Json<MacroListRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let items = client_for_write(&state, &code)?
        .save_host_macros(&host_id, &req.macros)
        .await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn register_host(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, AppError> {
    let data = client_for_write(&state, &code)?.register_host(&body).await?;
    Ok(Json(json!({ "code": 0, "data": data })))
}

async fn event_hosts(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, event_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, AppError> {
    let host_ids = client_for(&state, &code)?.event_hosts(&event_id).await?;
    Ok(Json(json!({ "code": 0, "data": { "hostIds": host_ids } })))
}

async fn ack_event(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, event_id)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, AppError> {
    let data = client_for_write(&state, &code)?
        .acknowledge_event(&event_id, &body)
        .await?;
    Ok(Json(json!({ "code": 0, "data": data })))
}

fn client_for_write(state: &AppState, code: &str) -> Result<ZabbixClient, AppError> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == code)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("未找到 Zabbix 实例 {code}")))?;
    ZabbixClient::for_write(state.http.clone(), instance)
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
