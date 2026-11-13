//! 第 3 期模板设计器。表单字段以 catalog/zabbix-7.0.json 为准，写入前核对主版本。

use std::sync::OnceLock;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::auth::ServiceAuth;
use crate::error::AppError;
use crate::state::AppState;
use crate::zabbix::ZabbixClient;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/catalog", get(catalog_route))
        .route(
            "/api/instances/:code/template-groups/:group_id",
            put(rename_template_group).delete(delete_template_group),
        )
        .route(
            "/api/instances/:code/host-groups/:group_id",
            put(rename_host_group).delete(delete_host_group),
        )
        .route(
            "/api/instances/:code/templates/export",
            post(export_templates),
        )
        .route(
            "/api/instances/:code/templates/import",
            post(import_templates),
        )
        .route(
            "/api/instances/:code/templates/:template_id",
            get(get_template).put(update_template).delete(delete_template),
        )
        .route(
            "/api/instances/:code/templates/:template_id/items",
            get(list_items).post(create_item),
        )
        .route(
            "/api/instances/:code/templates/:template_id/items/:item_id",
            put(update_item).delete(delete_item),
        )
        .route(
            "/api/instances/:code/templates/:template_id/triggers",
            get(list_triggers).post(create_trigger),
        )
        .route(
            "/api/instances/:code/templates/:template_id/triggers/:trigger_id",
            put(update_trigger).delete(delete_trigger),
        )
        .route(
            "/api/instances/:code/templates/:template_id/macros",
            get(template_macros).put(save_template_macros),
        )
        .merge(crate::lld::routes())
}

async fn catalog_route(_auth: ServiceAuth) -> Json<Value> {
    Json(json!({ "code": 0, "data": catalog() }))
}

fn catalog() -> &'static Value {
    static CATALOG: OnceLock<Value> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("../catalog/zabbix-7.0.json"))
            .expect("catalog/zabbix-7.0.json")
    })
}

pub(crate) fn write_client(state: &AppState, code: &str) -> Result<ZabbixClient, AppError> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == code)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("未找到 Zabbix 实例 {code}")))?;
    ZabbixClient::for_write(state.http.clone(), instance)
}

pub(crate) fn read_client(state: &AppState, code: &str) -> Result<ZabbixClient, AppError> {
    let instance = state
        .config
        .zabbix
        .iter()
        .find(|z| z.enabled && z.code == code)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("未找到 Zabbix 实例 {code}")))?;
    ZabbixClient::new(state.http.clone(), instance)
}

pub(crate) fn approved(headers: &HeaderMap) -> bool {
    headers
        .get("x-monitor-approved")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

async fn ensure_version(client: &ZabbixClient) -> Result<(), AppError> {
    let version = client.version().await?;
    let prefix = version.split('.').take(2).collect::<Vec<_>>().join(".");
    if prefix != "7.0" {
        return Err(AppError::bad(format!(
            "模板设计器只写入 Zabbix 7.0，当前版本 {version}"
        )));
    }
    Ok(())
}

pub(crate) async fn ensure_write(client: &ZabbixClient, ok: bool) -> Result<(), AppError> {
    if client.protected() && !ok {
        return Err(AppError::bad(
            "该实例已保护，模板变更需先提交标准变更工单，审批关闭后再执行",
        ));
    }
    ensure_version(client).await
}

pub(crate) fn require_id(id: &str) -> Result<(), AppError> {
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
        return Err(AppError::bad("标识无效"));
    }
    Ok(())
}

pub(crate) fn clean_name(raw: &str) -> Result<String, AppError> {
    let name = raw.trim();
    if name.is_empty() || name.chars().count() > 255 {
        return Err(AppError::bad("名称不能为空且不超过 255 字"));
    }
    if name.chars().any(|c| c.is_control() || matches!(c, '"' | '\'' | '\\' | ';')) {
        return Err(AppError::bad("名称含有不允许的字符"));
    }
    Ok(name.to_string())
}

fn clean_host(raw: &str) -> Result<String, AppError> {
    let host = raw.trim();
    if host.is_empty() || host.len() > 128 {
        return Err(AppError::bad("技术名不能为空且不超过 128 字符"));
    }
    if !host
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ' '))
    {
        return Err(AppError::bad("技术名只能包含字母、数字、空格、点、下划线和短横线"));
    }
    Ok(host.to_string())
}

pub(crate) fn text_field(raw: &str, limit: usize, label: &str) -> Result<String, AppError> {
    let value = raw.trim();
    if value.chars().count() > limit || value.chars().any(|c| c == '\n' || c == '\r' || c == '\0') {
        return Err(AppError::bad(format!("{label}过长或含有换行")));
    }
    Ok(value.to_string())
}

pub(crate) fn first_id(result: &Value, key: &str) -> Result<String, AppError> {
    result[key]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| AppError::internal("Zabbix 未返回标识"))
}

fn count_of(value: &Value) -> Result<i64, AppError> {
    match value {
        Value::Array(rows) => Ok(rows.len() as i64),
        Value::String(text) => text
            .parse()
            .map_err(|_| AppError::bad("无法确认组是否为空，已停止删除")),
        Value::Number(number) => Ok(number.as_i64().unwrap_or(0)),
        _ => Err(AppError::bad("无法确认组是否为空，已停止删除")),
    }
}

pub(crate) fn num(value: &Value) -> i64 {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0)
}

fn ids_from(body: &Value, key: &str) -> Result<Vec<String>, AppError> {
    let ids = body[key]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    for id in &ids {
        require_id(id)?;
    }
    Ok(ids)
}

pub(crate) fn type_name(kind: i64) -> String {
    catalog()["itemTypes"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["type"].as_i64() == Some(kind)))
        .and_then(|row| row["name"].as_str())
        .unwrap_or("其他类型")
        .to_string()
}

pub(crate) fn editable_type(kind: i64) -> bool {
    catalog()["itemTypes"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["type"].as_i64() == Some(kind)))
        .and_then(|row| row["editable"].as_bool())
        .unwrap_or(false)
}

fn preprocessing_allowed(kind: i64) -> bool {
    catalog()["preprocessing"]
        .as_array()
        .map(|rows| rows.iter().any(|row| row["type"].as_i64() == Some(kind)))
        .unwrap_or(false)
}

pub async fn create_template_group(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let name = clean_name(body["name"].as_str().unwrap_or(""))?;
    let result = client
        .call("templategroup.create", json!([{ "name": name }]))
        .await?;
    let group_id = first_id(&result, "groupids")?;
    Ok(Json(json!({ "code": 0, "data": { "groupId": group_id, "name": name } })))
}

pub async fn create_host_group(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let name = clean_name(body["name"].as_str().unwrap_or(""))?;
    let result = client
        .call("hostgroup.create", json!([{ "name": name }]))
        .await?;
    let group_id = first_id(&result, "groupids")?;
    Ok(Json(json!({ "code": 0, "data": { "groupId": group_id, "name": name } })))
}

async fn rename_template_group(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, group_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    rename_group(&state, &code, &group_id, &body, &headers, true).await
}

async fn rename_host_group(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, group_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    rename_group(&state, &code, &group_id, &body, &headers, false).await
}

async fn rename_group(
    state: &AppState,
    code: &str,
    group_id: &str,
    body: &Value,
    headers: &HeaderMap,
    template_group: bool,
) -> Result<Json<Value>, AppError> {
    require_id(group_id)?;
    let client = write_client(state, code)?;
    ensure_write(&client, approved(headers)).await?;
    let name = clean_name(body["name"].as_str().unwrap_or(""))?;
    let method = if template_group {
        "templategroup.update"
    } else {
        "hostgroup.update"
    };
    client
        .call(method, json!({ "groupid": group_id, "name": name }))
        .await?;
    Ok(Json(json!({ "code": 0, "data": { "groupId": group_id, "name": name } })))
}

async fn delete_template_group(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, group_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    delete_group(&state, &code, &group_id, &headers, true).await
}

async fn delete_host_group(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, group_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    delete_group(&state, &code, &group_id, &headers, false).await
}

async fn delete_group(
    state: &AppState,
    code: &str,
    group_id: &str,
    headers: &HeaderMap,
    template_group: bool,
) -> Result<Json<Value>, AppError> {
    require_id(group_id)?;
    let client = write_client(state, code)?;
    ensure_write(&client, approved(headers)).await?;
    let (method_get, select_key, method_del, label) = if template_group {
        ("templategroup.get", "selectTemplates", "templategroup.delete", "模板")
    } else {
        ("hostgroup.get", "selectHosts", "hostgroup.delete", "主机")
    };
    let found = client
        .call(
            method_get,
            json!({
                "output": ["groupid"],
                "groupids": [group_id],
                select_key: "count",
            }),
        )
        .await?;
    let row = found
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| AppError::NotFound("组不存在".to_string()))?;
    let count_key = if template_group { "templates" } else { "hosts" };
    if count_of(&row[count_key])? > 0 {
        return Err(AppError::bad(format!("组内仍有{label}，不能删除")));
    }
    client.call(method_del, json!([group_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "groupId": group_id } })))
}

pub async fn create_template(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let params = template_params(&body, None)?;
    let result = client.call("template.create", json!([params])).await?;
    let template_id = first_id(&result, "templateids")?;
    Ok(Json(json!({ "code": 0, "data": { "templateId": template_id } })))
}

async fn get_template(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = read_client(&state, &code)?;
    let row = load_template(&client, &template_id).await?;
    Ok(Json(json!({ "code": 0, "data": row })))
}

async fn update_template(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let params = template_params(&body, Some(&template_id))?;
    client.call("template.update", params).await?;
    let row = load_template(&client, &template_id).await?;
    Ok(Json(json!({ "code": 0, "data": row })))
}

async fn delete_template(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    client.call("template.delete", json!([template_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "templateId": template_id } })))
}

fn template_params(body: &Value, template_id: Option<&str>) -> Result<Value, AppError> {
    let host = clean_host(body["host"].as_str().unwrap_or(""))?;
    let name = clean_name(body["name"].as_str().unwrap_or(""))?;
    let group_ids = ids_from(body, "groupIds")?;
    if group_ids.is_empty() {
        return Err(AppError::bad("模板至少属于一个模板组"));
    }
    let parent_ids = ids_from(body, "parentTemplateIds")?;
    let mut params = json!({
        "host": host,
        "name": name,
        "groups": group_ids.iter().map(|id| json!({"groupid": id})).collect::<Vec<_>>(),
        "templates": parent_ids.iter().map(|id| json!({"templateid": id})).collect::<Vec<_>>(),
    });
    if let Some(id) = template_id {
        params["templateid"] = json!(id);
    }
    Ok(params)
}

pub(crate) async fn load_template(client: &ZabbixClient, template_id: &str) -> Result<Value, AppError> {
    let result = client
        .call(
            "template.get",
            json!({
                "output": ["templateid", "host", "name"],
                "templateids": [template_id],
                "selectTemplateGroups": ["groupid", "name"],
                "selectParentTemplates": ["templateid", "name"],
            }),
        )
        .await?;
    let row = result
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| AppError::NotFound("模板不存在".to_string()))?;
    Ok(json!({
        "templateId": row["templateid"].as_str().unwrap_or(""),
        "host": row["host"].as_str().unwrap_or(""),
        "name": row["name"].as_str().unwrap_or(""),
        "groups": map_refs(&row["templategroups"], "groupid", "groupId"),
        "parentTemplates": map_refs(&row["parentTemplates"], "templateid", "templateId"),
    }))
}

fn map_refs(value: &Value, id_key: &str, out_key: &str) -> Vec<Value> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|item| json!({ out_key: item[id_key].as_str().unwrap_or(""), "name": item["name"].as_str().unwrap_or("") }))
        .collect()
}

async fn list_items(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = read_client(&state, &code)?;
    let result = client
        .call(
            "item.get",
            json!({
                "output": ["itemid", "name", "key_", "type", "value_type", "delay", "units", "status", "description", "templateid", "master_itemid", "snmp_oid", "url", "request_method", "status_codes", "params"],
                "templateids": [template_id],
                "selectPreprocessing": ["type", "params", "error_handler", "error_handler_params"],
                "sortfield": "name",
            }),
        )
        .await?;
    let items = result
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| map_item(&row))
        .collect::<Vec<_>>();
    Ok(Json(json!({ "code": 0, "data": items })))
}

pub(crate) fn map_item(row: &Value) -> Value {
    let kind = num(&row["type"]);
    let inherited = inherited_id(&row["templateid"]);
    json!({
        "itemId": row["itemid"].as_str().unwrap_or(""),
        "name": row["name"].as_str().unwrap_or(""),
        "key": row["key_"].as_str().unwrap_or(""),
        "type": kind,
        "typeName": type_name(kind),
        "valueType": num(&row["value_type"]),
        "delay": row["delay"].as_str().unwrap_or(""),
        "units": row["units"].as_str().unwrap_or(""),
        "status": num(&row["status"]),
        "description": row["description"].as_str().unwrap_or(""),
        "inherited": inherited,
        "editable": !inherited && editable_type(kind),
        "masterItemId": row["master_itemid"].as_str().unwrap_or("0"),
        "snmpOid": row["snmp_oid"].as_str().unwrap_or(""),
        "url": row["url"].as_str().unwrap_or(""),
        "requestMethod": num(&row["request_method"]),
        "statusCodes": row["status_codes"].as_str().unwrap_or(""),
        "params": row["params"].as_str().unwrap_or(""),
        "preprocessing": map_preprocessing(&row["preprocessing"]),
    })
}

pub(crate) fn inherited_id(value: &Value) -> bool {
    let id = value.as_str().unwrap_or("0");
    !id.is_empty() && id != "0"
}

fn map_preprocessing(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|step| {
            json!({
                "type": num(&step["type"]),
                "params": step["params"].as_str().unwrap_or(""),
                "errorHandler": num(&step["error_handler"]),
                "errorHandlerParams": step["error_handler_params"].as_str().unwrap_or(""),
            })
        })
        .collect()
}

async fn create_item(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let params = build_item(&template_id, &body, None)?;
    let result = client.call("item.create", json!([params])).await?;
    let item_id = first_id(&result, "itemids")?;
    Ok(Json(json!({ "code": 0, "data": { "itemId": item_id } })))
}

async fn update_item(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, item_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    require_id(&item_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_item(&client, &template_id, &item_id).await?;
    if current["inherited"].as_bool() == Some(true) || current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad(format!(
            "该监控项为{}，设计器只读，避免保存时丢掉原有配置",
            current["typeName"].as_str().unwrap_or("不可编辑类型")
        )));
    }
    let mut params = build_item(&template_id, &body, Some(&current["preprocessing"]))?;
    if let Some(map) = params.as_object_mut() {
        map.remove("hostid");
    }
    params["itemid"] = json!(item_id);
    client.call("item.update", params).await?;
    Ok(Json(json!({ "code": 0, "data": { "itemId": item_id } })))
}

async fn delete_item(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, item_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    require_id(&item_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_item(&client, &template_id, &item_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad("该监控项只读，不能删除"));
    }
    client.call("item.delete", json!([item_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "itemId": item_id } })))
}

async fn one_item(client: &ZabbixClient, template_id: &str, item_id: &str) -> Result<Value, AppError> {
    let result = client
        .call(
            "item.get",
            json!({
                "output": ["itemid", "name", "key_", "type", "templateid"],
                "itemids": [item_id],
                "templateids": [template_id],
                "selectPreprocessing": ["type", "params", "error_handler", "error_handler_params"],
            }),
        )
        .await?;
    result
        .as_array()
        .and_then(|rows| rows.first())
        .map(map_item)
        .ok_or_else(|| AppError::NotFound("监控项不存在".to_string()))
}

pub(crate) fn build_item(template_id: &str, body: &Value, existing_steps: Option<&Value>) -> Result<Value, AppError> {
    let kind = body["type"].as_i64().unwrap_or(-1);
    if !editable_type(kind) {
        return Err(AppError::bad(format!(
            "{}不在设计器可编辑范围内",
            type_name(kind)
        )));
    }
    let name = clean_name(body["name"].as_str().unwrap_or(""))?;
    let key = text_field(body["key"].as_str().unwrap_or(""), 2048, "键值")?;
    if key.is_empty() {
        return Err(AppError::bad("键值不能为空"));
    }
    let value_type = body["valueType"].as_i64().unwrap_or(3);
    if !matches!(value_type, 0 | 1 | 3 | 4) {
        return Err(AppError::bad("值类型无效"));
    }
    let units = text_field(body["units"].as_str().unwrap_or(""), 255, "单位")?;
    let delay = if kind == 2 || kind == 18 || kind == 15 {
        "0".to_string()
    } else {
        let delay = text_field(body["delay"].as_str().unwrap_or("1m"), 64, "更新间隔")?;
        if delay.is_empty() {
            return Err(AppError::bad("更新间隔不能为空"));
        }
        delay
    };
    let mut params = json!({
        "hostid": template_id,
        "name": name,
        "key_": key,
        "type": kind,
        "value_type": value_type,
        "delay": delay,
        "units": units,
        "status": if body["status"].as_i64() == Some(1) { 1 } else { 0 },
        "preprocessing": merge_preprocessing(body, existing_steps)?,
    });
    match kind {
        20 => {
            let oid = text_field(body["snmpOid"].as_str().unwrap_or(""), 512, "SNMP OID")?;
            if oid.is_empty() {
                return Err(AppError::bad("SNMP OID 不能为空"));
            }
            params["snmp_oid"] = json!(oid);
        }
        18 => {
            let master = body["masterItemId"].as_str().unwrap_or("");
            require_id(master)?;
            params["master_itemid"] = json!(master);
        }
        15 => {
            let formula = body["params"].as_str().unwrap_or("").trim();
            if formula.is_empty() || formula.len() > 4096 {
                return Err(AppError::bad("计算公式不能为空且不超过 4096 字符"));
            }
            params["params"] = json!(formula);
        }
        19 => {
            let url = text_field(body["url"].as_str().unwrap_or(""), 2048, "URL")?;
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(AppError::bad("HTTP Agent 的 URL 需以 http:// 或 https:// 开头"));
            }
            let method = body["requestMethod"].as_i64().unwrap_or(0);
            if !(0..=3).contains(&method) {
                return Err(AppError::bad("HTTP 方法无效"));
            }
            params["url"] = json!(url);
            params["request_method"] = json!(method);
            params["status_codes"] = json!(text_field(body["statusCodes"].as_str().unwrap_or("200"), 255, "状态码")?);
        }
        _ => {}
    }
    Ok(params)
}

fn merge_preprocessing(body: &Value, existing_steps: Option<&Value>) -> Result<Vec<Value>, AppError> {
    let mut steps = Vec::new();
    for step in body["preprocessing"].as_array().cloned().unwrap_or_default() {
        let kind = step["type"].as_i64().unwrap_or(-1);
        if !preprocessing_allowed(kind) {
            return Err(AppError::bad("预处理步骤不在 7.0 目录中"));
        }
        let params = step["params"].as_str().unwrap_or("");
        if params.len() > 4096 || params.contains('\0') {
            return Err(AppError::bad("预处理参数无效"));
        }
        steps.push(json!({
            "type": kind,
            "params": params,
            "error_handler": step["errorHandler"].as_i64().unwrap_or(0),
            "error_handler_params": step["errorHandlerParams"].as_str().unwrap_or(""),
        }));
    }
    if let Some(existing) = existing_steps.and_then(|v| v.as_array()) {
        for step in existing {
            let kind = step["type"].as_i64().unwrap_or(-1);
            if !preprocessing_allowed(kind) {
                steps.push(json!({
                    "type": kind,
                    "params": step["params"].as_str().unwrap_or(""),
                    "error_handler": step["errorHandler"].as_i64().unwrap_or(0),
                    "error_handler_params": step["errorHandlerParams"].as_str().unwrap_or(""),
                }));
            }
        }
    }
    Ok(steps)
}

async fn list_triggers(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = read_client(&state, &code)?;
    let result = client
        .call(
            "trigger.get",
            json!({
                "output": ["triggerid", "description", "expression", "priority", "status", "recovery_mode", "recovery_expression", "templateid"],
                "templateids": [template_id],
                "expandExpression": false,
                "sortfield": "description",
            }),
        )
        .await?;
    let rows = result
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| map_trigger(&row))
        .collect::<Vec<_>>();
    Ok(Json(json!({ "code": 0, "data": rows })))
}

pub(crate) fn map_trigger(row: &Value) -> Value {
    let inherited = inherited_id(&row["templateid"]);
    json!({
        "triggerId": row["triggerid"].as_str().unwrap_or(""),
        "description": row["description"].as_str().unwrap_or(""),
        "expression": row["expression"].as_str().unwrap_or(""),
        "priority": num(&row["priority"]),
        "status": num(&row["status"]),
        "recoveryMode": num(&row["recovery_mode"]),
        "recoveryExpression": row["recovery_expression"].as_str().unwrap_or(""),
        "inherited": inherited,
        "editable": !inherited,
    })
}

async fn create_trigger(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let _ = load_template(&client, &template_id).await?;
    let params = build_trigger(&body)?;
    let result = client.call("trigger.create", json!([params])).await?;
    let trigger_id = first_id(&result, "triggerids")?;
    Ok(Json(json!({ "code": 0, "data": { "triggerId": trigger_id } })))
}

async fn update_trigger(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, trigger_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    require_id(&trigger_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_trigger(&client, &template_id, &trigger_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad("继承来的触发器请到上级模板修改"));
    }
    let mut params = build_trigger(&body)?;
    params["triggerid"] = json!(trigger_id);
    client.call("trigger.update", params).await?;
    Ok(Json(json!({ "code": 0, "data": { "triggerId": trigger_id } })))
}

async fn delete_trigger(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, trigger_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    require_id(&trigger_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_trigger(&client, &template_id, &trigger_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad("继承来的触发器不能在这里删除"));
    }
    client.call("trigger.delete", json!([trigger_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "triggerId": trigger_id } })))
}

async fn one_trigger(client: &ZabbixClient, template_id: &str, trigger_id: &str) -> Result<Value, AppError> {
    let result = client
        .call(
            "trigger.get",
            json!({
                "output": ["triggerid", "description", "expression", "priority", "status", "recovery_mode", "recovery_expression", "templateid"],
                "triggerids": [trigger_id],
                "templateids": [template_id],
            }),
        )
        .await?;
    result
        .as_array()
        .and_then(|rows| rows.first())
        .map(map_trigger)
        .ok_or_else(|| AppError::NotFound("触发器不存在".to_string()))
}

pub(crate) fn build_trigger(body: &Value) -> Result<Value, AppError> {
    let description = clean_name(body["description"].as_str().unwrap_or(""))?;
    let expression = body["expression"].as_str().unwrap_or("").trim();
    if expression.is_empty() || expression.len() > 2048 || expression.contains('\0') {
        return Err(AppError::bad("表达式不能为空且不超过 2048 字符"));
    }
    let priority = body["priority"].as_i64().unwrap_or(0);
    if !(0..=5).contains(&priority) {
        return Err(AppError::bad("严重级别无效"));
    }
    let recovery_mode = body["recoveryMode"].as_i64().unwrap_or(0);
    if !matches!(recovery_mode, 0 | 1 | 2) {
        return Err(AppError::bad("恢复模式无效"));
    }
    let recovery = body["recoveryExpression"].as_str().unwrap_or("").trim();
    if recovery_mode == 1 && recovery.is_empty() {
        return Err(AppError::bad("恢复表达式不能为空"));
    }
    if recovery.len() > 2048 {
        return Err(AppError::bad("恢复表达式过长"));
    }
    Ok(json!({
        "description": description,
        "expression": expression,
        "priority": priority,
        "status": if body["status"].as_i64() == Some(1) { 1 } else { 0 },
        "recovery_mode": recovery_mode,
        "recovery_expression": recovery,
    }))
}

async fn template_macros(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = read_client(&state, &code)?;
    let _ = load_template(&client, &template_id).await?;
    let items = macro_list(&client, &template_id).await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn save_template_macros(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let _ = load_template(&client, &template_id).await?;
    let macros = body["macros"].as_array().cloned().unwrap_or_default();
    let existing = macro_list(&client, &template_id).await?;
    let mut by_name = std::collections::HashMap::new();
    for item in &existing {
        if let Some(name) = item["macro"].as_str() {
            by_name.insert(name.to_string(), item.clone());
        }
    }
    for item in &macros {
        let name = item["macro"].as_str().unwrap_or("").trim();
        if !name.starts_with("{$") || !name.ends_with('}') || name.len() > 255 {
            return Err(AppError::bad("宏名需形如 {$NAME}"));
        }
        let value = item["value"].as_str().unwrap_or("");
        if value.contains('\n') || value.contains('\0') || value.len() > 2048 {
            return Err(AppError::bad("宏值无效"));
        }
        let description = text_field(item["description"].as_str().unwrap_or(""), 255, "宏说明")?;
        if let Some(old) = by_name.get(name) {
            if old["secret"].as_bool() == Some(true) && (value.is_empty() || value == "******") {
                continue;
            }
            client
                .call(
                    "usermacro.update",
                    json!({
                        "hostmacroid": old["hostMacroId"].as_str().unwrap_or(""),
                        "value": value,
                        "description": description,
                    }),
                )
                .await?;
        } else if !value.is_empty() {
            client
                .call(
                    "usermacro.create",
                    json!({
                        "hostid": template_id,
                        "macro": name,
                        "value": value,
                        "description": description,
                    }),
                )
                .await?;
        }
    }
    let items = macro_list(&client, &template_id).await?;
    Ok(Json(json!({ "code": 0, "data": items })))
}

async fn macro_list(client: &ZabbixClient, template_id: &str) -> Result<Vec<Value>, AppError> {
    let result = client
        .call(
            "usermacro.get",
            json!({
                "output": ["hostmacroid", "macro", "value", "description", "type"],
                "hostids": [template_id],
                "sortfield": "macro",
            }),
        )
        .await?;
    Ok(result
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            json!({
                "hostMacroId": row["hostmacroid"].as_str().unwrap_or(""),
                "macro": row["macro"].as_str().unwrap_or(""),
                "value": row["value"].as_str().unwrap_or(""),
                "description": row["description"].as_str().unwrap_or(""),
                "secret": row["type"].as_str() == Some("1"),
            })
        })
        .collect())
}

async fn export_templates(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let client = read_client(&state, &code)?;
    ensure_version(&client).await?;
    let ids = ids_from(&body, "templateIds")?;
    if ids.is_empty() || ids.len() > 20 {
        return Err(AppError::bad("请选择 1 到 20 个模板"));
    }
    let result = client
        .call(
            "configuration.export",
            json!({
                "format": "json",
                "options": { "templates": ids },
            }),
        )
        .await?;
    let source = result
        .as_str()
        .ok_or_else(|| AppError::internal("Zabbix 未返回导出内容"))?;
    Ok(Json(json!({ "code": 0, "data": { "source": source } })))
}

async fn import_templates(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path(code): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let source = body["source"].as_str().unwrap_or("").trim();
    if source.is_empty() || source.len() > 2_000_000 {
        return Err(AppError::bad("导入内容为空或超过 2MB"));
    }
    if serde_json::from_str::<Value>(source).is_err() {
        return Err(AppError::bad("导入内容不是 JSON"));
    }
    client
        .call(
            "configuration.import",
            json!({
                "format": "json",
                "source": source,
                "rules": {
                    "templates": { "createMissing": true, "updateExisting": true },
                    "items": { "createMissing": true, "updateExisting": true, "deleteMissing": false },
                    "triggers": { "createMissing": true, "updateExisting": true, "deleteMissing": false },
                    "templateLinkage": { "createMissing": true },
                    "templateGroups": { "createMissing": true },
                    "discoveryRules": { "createMissing": true, "updateExisting": true, "deleteMissing": false }
                }
            }),
        )
        .await?;
    Ok(Json(json!({ "code": 0, "data": { "imported": true } })))
}
