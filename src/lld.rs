//! 第 4 期低级发现：发现规则、过滤、监控项原型、触发器原型、覆盖规则。

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, put};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::auth::ServiceAuth;
use crate::designer::{
    approved, build_item, build_trigger, clean_name, editable_type, ensure_write, first_id,
    inherited_id, load_template, map_item, map_trigger, num, read_client, require_id, text_field,
    type_name, write_client,
};
use crate::error::AppError;
use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/instances/:code/templates/:template_id/discovery-rules",
            get(list_rules).post(create_rule),
        )
        .route(
            "/api/instances/:code/templates/:template_id/discovery-rules/:rule_id",
            put(update_rule).delete(delete_rule),
        )
        .route(
            "/api/instances/:code/templates/:template_id/discovery-rules/:rule_id/items",
            get(list_item_prototypes).post(create_item_prototype),
        )
        .route(
            "/api/instances/:code/templates/:template_id/discovery-rules/:rule_id/items/:item_id",
            put(update_item_prototype).delete(delete_item_prototype),
        )
        .route(
            "/api/instances/:code/templates/:template_id/discovery-rules/:rule_id/triggers",
            get(list_trigger_prototypes).post(create_trigger_prototype),
        )
        .route(
            "/api/instances/:code/templates/:template_id/discovery-rules/:rule_id/triggers/:trigger_id",
            put(update_trigger_prototype).delete(delete_trigger_prototype),
        )
}

async fn list_rules(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_id(&template_id)?;
    let client = read_client(&state, &code)?;
    let result = client
        .call(
            "discoveryrule.get",
            json!({
                "output": ["itemid", "name", "key_", "type", "delay", "status", "templateid", "lifetime", "lifetime_type", "description"],
                "templateids": [template_id],
                "selectFilter": "extend",
                "selectOverrides": "extend",
                "sortfield": "name",
            }),
        )
        .await?;
    let rows = result
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| map_rule(&row))
        .collect::<Vec<_>>();
    Ok(Json(json!({ "code": 0, "data": rows })))
}

async fn create_rule(
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
    let mut params = rule_params(&body)?;
    params["hostid"] = json!(template_id);
    let result = client
        .call("discoveryrule.create", json!([params]))
        .await?;
    let rule_id = first_id(&result, "itemids")?;
    if body.get("overrides").is_some() {
        save_overrides(&client, &template_id, &rule_id, &body).await?;
    }
    Ok(Json(json!({ "code": 0, "data": { "ruleId": rule_id } })))
}

async fn update_rule(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = load_rule(&client, &template_id, &rule_id).await?;
    ensure_editable(&current)?;
    let mut params = rule_params(&body)?;
    params["itemid"] = json!(rule_id);
    client.call("discoveryrule.update", params).await?;
    if body.get("overrides").is_some() {
        save_overrides(&client, &template_id, &rule_id, &body).await?;
    }
    let row = load_rule(&client, &template_id, &rule_id).await?;
    Ok(Json(json!({ "code": 0, "data": row })))
}

async fn delete_rule(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = load_rule(&client, &template_id, &rule_id).await?;
    ensure_editable(&current)?;
    client.call("discoveryrule.delete", json!([rule_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "ruleId": rule_id } })))
}

async fn list_item_prototypes(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    let client = read_client(&state, &code)?;
    let _ = load_rule(&client, &template_id, &rule_id).await?;
    let result = client
        .call(
            "itemprototype.get",
            json!({
                "output": ["itemid", "name", "key_", "type", "value_type", "delay", "units", "status", "description", "templateid", "master_itemid", "snmp_oid", "url", "request_method", "status_codes", "params"],
                "discoveryids": [rule_id],
                "selectPreprocessing": ["type", "params", "error_handler", "error_handler_params"],
                "sortfield": "name",
            }),
        )
        .await?;
    let rows = result.as_array().cloned().unwrap_or_default().into_iter().map(|row| map_item(&row)).collect::<Vec<_>>();
    Ok(Json(json!({ "code": 0, "data": rows })))
}

async fn create_item_prototype(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let rule = load_rule(&client, &template_id, &rule_id).await?;
    ensure_editable(&rule)?;
    require_lld_key(body["key"].as_str().unwrap_or(""))?;
    let mut params = build_item(&template_id, &body, None)?;
    params["ruleid"] = json!(rule_id);
    let result = client.call("itemprototype.create", json!([params])).await?;
    let item_id = first_id(&result, "itemids")?;
    Ok(Json(json!({ "code": 0, "data": { "itemId": item_id } })))
}

async fn update_item_prototype(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id, item_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    require_id(&item_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_item_prototype(&client, &template_id, &rule_id, &item_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad(format!(
            "该监控项原型为{}，设计器只读",
            current["typeName"].as_str().unwrap_or("不可编辑类型")
        )));
    }
    require_lld_key(body["key"].as_str().unwrap_or(""))?;
    let mut params = build_item(&template_id, &body, Some(&current["preprocessing"]))?;
    if let Some(map) = params.as_object_mut() {
        map.remove("hostid");
        map.remove("ruleid");
    }
    params["itemid"] = json!(item_id);
    client.call("itemprototype.update", params).await?;
    Ok(Json(json!({ "code": 0, "data": { "itemId": item_id } })))
}

async fn delete_item_prototype(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id, item_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    require_id(&item_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_item_prototype(&client, &template_id, &rule_id, &item_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad("该监控项原型只读，不能删除"));
    }
    client.call("itemprototype.delete", json!([item_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "itemId": item_id } })))
}

async fn list_trigger_prototypes(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    let client = read_client(&state, &code)?;
    let _ = load_rule(&client, &template_id, &rule_id).await?;
    let result = client
        .call(
            "triggerprototype.get",
            json!({
                "output": ["triggerid", "description", "expression", "priority", "status", "recovery_mode", "recovery_expression", "templateid"],
                "discoveryids": [rule_id],
                "expandExpression": false,
                "sortfield": "description",
            }),
        )
        .await?;
    let rows = result.as_array().cloned().unwrap_or_default().into_iter().map(|row| map_trigger(&row)).collect::<Vec<_>>();
    Ok(Json(json!({ "code": 0, "data": rows })))
}

async fn create_trigger_prototype(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let rule = load_rule(&client, &template_id, &rule_id).await?;
    ensure_editable(&rule)?;
    let params = build_trigger(&body)?;
    let result = client.call("triggerprototype.create", json!([params])).await?;
    let trigger_id = first_id(&result, "triggerids")?;
    Ok(Json(json!({ "code": 0, "data": { "triggerId": trigger_id } })))
}

async fn update_trigger_prototype(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id, trigger_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    require_id(&trigger_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_trigger_prototype(&client, &template_id, &rule_id, &trigger_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad("继承来的触发器原型请到上级模板修改"));
    }
    let mut params = build_trigger(&body)?;
    params["triggerid"] = json!(trigger_id);
    client.call("triggerprototype.update", params).await?;
    Ok(Json(json!({ "code": 0, "data": { "triggerId": trigger_id } })))
}

async fn delete_trigger_prototype(
    State(state): State<AppState>,
    _auth: ServiceAuth,
    Path((code, template_id, rule_id, trigger_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_pair(&template_id, &rule_id)?;
    require_id(&trigger_id)?;
    let client = write_client(&state, &code)?;
    ensure_write(&client, approved(&headers)).await?;
    let current = one_trigger_prototype(&client, &template_id, &rule_id, &trigger_id).await?;
    if current["editable"].as_bool() != Some(true) {
        return Err(AppError::bad("继承来的触发器原型不能在这里删除"));
    }
    client.call("triggerprototype.delete", json!([trigger_id])).await?;
    Ok(Json(json!({ "code": 0, "data": { "triggerId": trigger_id } })))
}

fn require_pair(template_id: &str, rule_id: &str) -> Result<(), AppError> {
    require_id(template_id)?;
    require_id(rule_id)
}

fn ensure_editable(rule: &Value) -> Result<(), AppError> {
    if rule["editable"].as_bool() == Some(true) {
        Ok(())
    } else {
        Err(AppError::bad("继承来的发现规则请到上级模板修改"))
    }
}

fn require_lld_key(key: &str) -> Result<(), AppError> {
    if key.contains("{#") {
        Ok(())
    } else {
        Err(AppError::bad("原型键值需要包含低级发现宏，例如 {#IFNAME}"))
    }
}

fn rule_params(body: &Value) -> Result<Value, AppError> {
    let kind = body["type"].as_i64().unwrap_or(-1);
    if !editable_type(kind) {
        return Err(AppError::bad(format!("{}不能作为发现规则", type_name(kind))));
    }
    let name = clean_name(body["name"].as_str().unwrap_or(""))?;
    let key = text_field(body["key"].as_str().unwrap_or(""), 2048, "键值")?;
    if key.is_empty() {
        return Err(AppError::bad("键值不能为空"));
    }
    let delay = text_field(body["delay"].as_str().unwrap_or("1h"), 64, "更新间隔")?;
    if delay.is_empty() {
        return Err(AppError::bad("更新间隔不能为空"));
    }
    let lifetime_type = body["lifetimeType"].as_i64().unwrap_or(0);
    if !matches!(lifetime_type, 0 | 1 | 2) {
        return Err(AppError::bad("保留期限类型无效"));
    }
    let lifetime = text_field(body["lifetime"].as_str().unwrap_or("7d"), 64, "保留期限")?;
    if lifetime_type == 0 && lifetime.is_empty() {
        return Err(AppError::bad("保留期限不能为空"));
    }
    Ok(json!({
        "name": name,
        "key_": key,
        "type": kind,
        "delay": delay,
        "status": if body["status"].as_i64() == Some(1) { 1 } else { 0 },
        "filter": build_filter(body)?,
        "lifetime_type": lifetime_type,
        "lifetime": if lifetime.is_empty() { "7d" } else { lifetime.as_str() },
    }))
}

fn build_filter(body: &Value) -> Result<Value, AppError> {
    let evaltype = body["filterEvalType"].as_i64().unwrap_or(0);
    if !matches!(evaltype, 0 | 1 | 2) {
        return Err(AppError::bad("过滤组合只支持与/或、并且、或者"));
    }
    let mut conditions = Vec::new();
    for condition in body["filter"].as_array().cloned().unwrap_or_default() {
        let macro_name = condition["macro"].as_str().unwrap_or("").trim();
        if !valid_macro(macro_name) {
            return Err(AppError::bad("过滤宏需形如 {#NAME}"));
        }
        let operator = condition["operator"].as_i64().unwrap_or(8);
        if !matches!(operator, 8 | 9) {
            return Err(AppError::bad("过滤运算符只支持匹配或不匹配"));
        }
        let value = text_field(condition["value"].as_str().unwrap_or(""), 255, "过滤值")?;
        conditions.push(json!({ "macro": macro_name, "operator": operator, "value": value }));
    }
    Ok(json!({ "evaltype": evaltype, "conditions": conditions }))
}

fn valid_macro(value: &str) -> bool {
    let Some(inner) = value.strip_prefix("{#").and_then(|s| s.strip_suffix('}')) else {
        return false;
    };
    !inner.is_empty()
        && inner.len() <= 64
        && inner.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

fn map_rule(row: &Value) -> Value {
    let kind = num(&row["type"]);
    let inherited = inherited_id(&row["templateid"]);
    let filter = map_conditions(&row["filter"]);
    json!({
        "ruleId": row["itemid"].as_str().unwrap_or(""),
        "name": row["name"].as_str().unwrap_or(""),
        "key": row["key_"].as_str().unwrap_or(""),
        "type": kind,
        "typeName": type_name(kind),
        "delay": row["delay"].as_str().unwrap_or(""),
        "status": num(&row["status"]),
        "lifetimeType": num(&row["lifetime_type"]),
        "lifetime": row["lifetime"].as_str().unwrap_or(""),
        "description": row["description"].as_str().unwrap_or(""),
        "inherited": inherited,
        "editable": !inherited && editable_type(kind),
        "filterEvalType": filter.0,
        "filter": filter.1,
        "overrides": map_overrides(&row["overrides"]),
    })
}

fn map_conditions(filter: &Value) -> (i64, Vec<Value>) {
    let conditions = filter["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            json!({
                "macro": row["macro"].as_str().unwrap_or(""),
                "operator": num(&row["operator"]),
                "value": row["value"].as_str().unwrap_or(""),
            })
        })
        .collect();
    (num(&filter["evaltype"]), conditions)
}

fn map_overrides(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            let filter = map_conditions(&row["filter"]);
            let operations = row["operations"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|op| num(&op["operationobject"]) <= 1)
                .map(|op| {
                    json!({
                        "operationObject": num(&op["operationobject"]),
                        "operator": num(&op["operator"]),
                        "value": op["value"].as_str().unwrap_or(""),
                        "status": op.get("opstatus").map(|s| num(&s["status"])),
                        "discover": op.get("opdiscover").map(|s| num(&s["discover"])),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "name": row["name"].as_str().unwrap_or(""),
                "step": num(&row["step"]),
                "stop": num(&row["stop"]) == 1,
                "filterEvalType": filter.0,
                "filter": filter.1,
                "operations": operations,
            })
        })
        .collect()
}

async fn load_rule(client: &crate::zabbix::ZabbixClient, template_id: &str, rule_id: &str) -> Result<Value, AppError> {
    let result = client
        .call(
            "discoveryrule.get",
            json!({
                "output": ["itemid", "name", "key_", "type", "delay", "status", "templateid", "lifetime", "lifetime_type", "description"],
                "itemids": [rule_id],
                "templateids": [template_id],
                "selectFilter": "extend",
                "selectOverrides": "extend",
            }),
        )
        .await?;
    result
        .as_array()
        .and_then(|rows| rows.first())
        .map(map_rule)
        .ok_or_else(|| AppError::NotFound("发现规则不存在".to_string()))
}

async fn save_overrides(
    client: &crate::zabbix::ZabbixClient,
    template_id: &str,
    rule_id: &str,
    body: &Value,
) -> Result<(), AppError> {
    let current = client
        .call(
            "discoveryrule.get",
            json!({
                "output": ["itemid"],
                "itemids": [rule_id],
                "templateids": [template_id],
                "selectOverrides": "extend",
            }),
        )
        .await?;
    let existing = current
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| AppError::NotFound("发现规则不存在".to_string()))?;
    let overrides = build_overrides(body, &existing["overrides"])?;
    client
        .call(
            "discoveryrule.update",
            json!({ "itemid": rule_id, "overrides": overrides }),
        )
        .await?;
    Ok(())
}

fn build_overrides(body: &Value, existing: &Value) -> Result<Vec<Value>, AppError> {
    let incoming = body["overrides"].as_array().cloned().unwrap_or_default();
    if incoming.len() > 30 {
        return Err(AppError::bad("覆盖规则最多 30 条"));
    }
    let mut steps = Vec::new();
    let mut out = Vec::new();
    for row in &incoming {
        let name = clean_name(row["name"].as_str().unwrap_or(""))?;
        let step = row["step"].as_i64().unwrap_or(0);
        if !(1..=99).contains(&step) || steps.contains(&step) {
            return Err(AppError::bad("覆盖步骤需是 1 到 99 且不能重复"));
        }
        steps.push(step);
        let filter_body = json!({
            "filterEvalType": row["filterEvalType"],
            "filter": row["filter"],
        });
        let filter = build_filter(&filter_body)?;
        if filter["conditions"].as_array().map(|rows| rows.is_empty()).unwrap_or(true) {
            return Err(AppError::bad("每条覆盖至少要有一个过滤条件"));
        }
        let mut operations = build_operations(row)?;
        if let Some(old) = existing.as_array().and_then(|rows| {
            rows.iter().find(|item| num(&item["step"]) == step || item["name"].as_str() == Some(name.as_str()))
        }) {
            for op in old["operations"].as_array().cloned().unwrap_or_default() {
                if num(&op["operationobject"]) >= 2 {
                    operations.push(op);
                }
            }
        }
        if operations.is_empty() {
            return Err(AppError::bad("每条覆盖至少要有一个动作"));
        }
        out.push(json!({
            "name": name,
            "step": step.to_string(),
            "stop": if row["stop"].as_bool() == Some(true) { 1 } else { 0 },
            "filter": filter,
            "operations": operations,
        }));
    }
    Ok(out)
}

fn build_operations(row: &Value) -> Result<Vec<Value>, AppError> {
    let mut operations = Vec::new();
    for op in row["operations"].as_array().cloned().unwrap_or_default() {
        let object = op["operationObject"].as_i64().unwrap_or(-1);
        if !matches!(object, 0 | 1) {
            return Err(AppError::bad("覆盖对象只支持监控项原型或触发器原型"));
        }
        let operator = op["operator"].as_i64().unwrap_or(8);
        if !matches!(operator, 0 | 1 | 2 | 3 | 8 | 9) {
            return Err(AppError::bad("覆盖运算符无效"));
        }
        let value = text_field(op["value"].as_str().unwrap_or(""), 255, "覆盖匹配值")?;
        if value.is_empty() {
            return Err(AppError::bad("覆盖匹配值不能为空"));
        }
        let mut mapped = json!({
            "operationobject": object,
            "operator": operator,
            "value": value,
        });
        let has_status = op.get("status").map(|v| !v.is_null()).unwrap_or(false);
        let has_discover = op.get("discover").map(|v| !v.is_null()).unwrap_or(false);
        if !has_status && !has_discover {
            return Err(AppError::bad("覆盖动作需要指定启用状态或不发现"));
        }
        if has_status {
            let status = op["status"].as_i64().unwrap_or(-1);
            if !matches!(status, 0 | 1) {
                return Err(AppError::bad("覆盖状态无效"));
            }
            mapped["opstatus"] = json!({ "status": status });
        }
        if has_discover {
            let discover = op["discover"].as_i64().unwrap_or(-1);
            if !matches!(discover, 0 | 1) {
                return Err(AppError::bad("发现标记无效"));
            }
            mapped["opdiscover"] = json!({ "discover": discover });
        }
        operations.push(mapped);
    }
    Ok(operations)
}

async fn one_item_prototype(
    client: &crate::zabbix::ZabbixClient,
    template_id: &str,
    rule_id: &str,
    item_id: &str,
) -> Result<Value, AppError> {
    let _ = load_rule(client, template_id, rule_id).await?;
    let result = client
        .call(
            "itemprototype.get",
            json!({
                "output": ["itemid", "name", "key_", "type", "templateid"],
                "itemids": [item_id],
                "discoveryids": [rule_id],
                "selectPreprocessing": ["type", "params", "error_handler", "error_handler_params"],
            }),
        )
        .await?;
    result
        .as_array()
        .and_then(|rows| rows.first())
        .map(map_item)
        .ok_or_else(|| AppError::NotFound("监控项原型不存在".to_string()))
}

async fn one_trigger_prototype(
    client: &crate::zabbix::ZabbixClient,
    template_id: &str,
    rule_id: &str,
    trigger_id: &str,
) -> Result<Value, AppError> {
    let _ = load_rule(client, template_id, rule_id).await?;
    let result = client
        .call(
            "triggerprototype.get",
            json!({
                "output": ["triggerid", "description", "expression", "priority", "status", "recovery_mode", "recovery_expression", "templateid"],
                "triggerids": [trigger_id],
                "discoveryids": [rule_id],
            }),
        )
        .await?;
    result
        .as_array()
        .and_then(|rows| rows.first())
        .map(map_trigger)
        .ok_or_else(|| AppError::NotFound("触发器原型不存在".to_string()))
}
