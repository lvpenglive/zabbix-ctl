//! Zabbix JSON-RPC 客户端。历史数据仍留在 Zabbix，这里只现查。

use std::sync::OnceLock;

use serde_json::{json, Value};

use crate::config::ZabbixInstanceConfig;
use crate::error::AppError;

/// 可选 limit 的安全上限（仅当调用方显式传 limit 时生效）。
const HOST_LIMIT_MAX: u32 = 10_000;
const LIST_AGENT_KEYS: &[&str] = &["agent.version", "agent.ping"];
const ITEM_GET_HOST_CHUNK: usize = 500;

/// 只允许这些监控项键，避免把 Zabbix 查询暴露成任意键接口。
const ITEM_KEYS: &[&str] = &[
    "agent.version",
    "agent.ping",
    "system.cpu.util",
    "vm.memory.utilization",
    "vm.memory.size[pused]",
    "vfs.fs.dependent.size[/,pused]",
    "vfs.fs.size[/,pused]",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthMode {
    /// Authorization: Bearer（Zabbix 6.4+ 推荐，7.2+ 唯一）
    Header,
    /// JSON-RPC body 的 auth 字段（7.2 以下可用；反代剥 Header 时必需）
    Body,
}

pub struct ZabbixClient {
    http: reqwest::Client,
    instance: ZabbixInstanceConfig,
    token: String,
    auth_mode: OnceLock<AuthMode>,
}

impl ZabbixClient {
    pub fn new(http: reqwest::Client, instance: ZabbixInstanceConfig) -> Result<Self, AppError> {
        let token = instance.resolved_api_token().ok_or_else(|| {
            AppError::bad(format!(
                "未配置 Zabbix API 令牌（toml [[zabbix]].api_token 或环境变量 {}）",
                instance.token_env_key()
            ))
        })?;
        Ok(Self {
            http,
            instance,
            token,
            auth_mode: OnceLock::new(),
        })
    }

    pub async fn version(&self) -> Result<String, AppError> {
        let result = self.call("apiinfo.version", json!({})).await?;
        result
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::internal("Zabbix 未返回版本"))
    }

    /// 列出已启用主机。`limit = None` 表示全量（Zabbix host.get 不传 limit）。
    pub async fn list_hosts(
        &self,
        limit: Option<u32>,
        search: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        let mut params = json!({
            "output": ["hostid", "host", "name", "status"],
            "selectInterfaces": ["ip", "dns", "type", "main", "available"],
            "filter": { "status": 0 },
            "sortfield": "host",
        });
        if let Some(n) = limit.filter(|&n| n > 0) {
            params["limit"] = json!(n.clamp(1, HOST_LIMIT_MAX));
        }
        if let Some(q) = search.map(str::trim).filter(|s| !s.is_empty()) {
            params["search"] = json!({ "host": q, "name": q });
            params["searchByAny"] = json!(true);
        }
        let hosts = self.call("host.get", params).await?;
        let hosts = hosts.as_array().cloned().unwrap_or_default();
        let host_ids: Vec<String> = hosts
            .iter()
            .filter_map(|h| h["hostid"].as_str().map(|s| s.to_string()))
            .collect();
        // 名册只需通断/版本，不拉 CPU 等指标键
        let items = self
            .items_for_hosts_with_keys(&host_ids, LIST_AGENT_KEYS)
            .await?;
        Ok(hosts
            .into_iter()
            .map(|h| map_host(&h, &items))
            .collect())
    }

    pub async fn host_problems(&self, host_id: &str) -> Result<Vec<Value>, AppError> {
        self.ensure_host(host_id).await?;
        let result = self
            .call(
                "problem.get",
                json!({
                    "output": ["eventid", "name", "severity", "clock"],
                    "hostids": [host_id],
                    "recent": false,
                    "sortfield": ["eventid"],
                    "sortorder": "DESC",
                    "limit": 50,
                }),
            )
            .await?;
        let problems = result
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                json!({
                    "eventId": p["eventid"].as_str().unwrap_or(""),
                    "name": p["name"].as_str().unwrap_or(""),
                    "severity": p["severity"].as_str().unwrap_or("0"),
                    "clock": p["clock"].as_str().unwrap_or(""),
                })
            })
            .collect();
        Ok(problems)
    }

    pub async fn host_metrics(&self, host_id: &str, hours: u32) -> Result<Vec<Value>, AppError> {
        self.ensure_host(host_id).await?;
        let hours = hours.clamp(1, 168);
        let items = self.items_for_hosts(&[host_id.to_string()]).await?;
        let now = chrono_now();
        let from = now.saturating_sub(hours as i64 * 3600);
        let mut series = Vec::new();
        for (name, keys) in [
            ("cpu", &["system.cpu.util"][..]),
            (
                "memory",
                &["vm.memory.utilization", "vm.memory.size[pused]"][..],
            ),
            (
                "disk",
                &[
                    "vfs.fs.dependent.size[/,pused]",
                    "vfs.fs.size[/,pused]",
                ][..],
            ),
        ] {
            let Some(item) = pick_item(&items, host_id, keys) else {
                series.push(json!({ "name": name, "key": Value::Null, "points": [] }));
                continue;
            };
            let points = self.series_points(&item, from, hours).await?;
            series.push(json!({
                "name": name,
                "key": item["key_"].as_str().unwrap_or(""),
                "points": points,
            }));
        }
        Ok(series)
    }

    async fn ensure_host(&self, host_id: &str) -> Result<(), AppError> {
        if !host_id.chars().all(|c| c.is_ascii_digit()) {
            return Err(AppError::bad("hostId 无效"));
        }
        let result = self
            .call(
                "host.get",
                json!({
                    "output": ["hostid"],
                    "hostids": [host_id],
                    "filter": { "status": 0 },
                    "limit": 1,
                }),
            )
            .await?;
        let empty = result.as_array().map(|a| a.is_empty()).unwrap_or(true);
        if empty {
            return Err(AppError::NotFound(format!("未找到主机 {host_id}")));
        }
        Ok(())
    }

    async fn items_for_hosts(&self, host_ids: &[String]) -> Result<Vec<Value>, AppError> {
        self.items_for_hosts_with_keys(host_ids, ITEM_KEYS).await
    }

    async fn items_for_hosts_with_keys(
        &self,
        host_ids: &[String],
        keys: &[&str],
    ) -> Result<Vec<Value>, AppError> {
        if host_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all = Vec::new();
        for chunk in host_ids.chunks(ITEM_GET_HOST_CHUNK) {
            let result = self
                .call(
                    "item.get",
                    json!({
                        "output": ["itemid", "hostid", "key_", "lastvalue", "value_type"],
                        "hostids": chunk,
                        "monitored": true,
                        "filter": { "key_": keys },
                    }),
                )
                .await?;
            if let Some(arr) = result.as_array() {
                all.extend(arr.iter().cloned());
            }
        }
        Ok(all)
    }

    async fn series_points(&self, item: &Value, from: i64, hours: u32) -> Result<Vec<Value>, AppError> {
        let item_id = item["itemid"].as_str().unwrap_or("");
        let value_type = item["value_type"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        if hours <= 24 {
            let result = self
                .call(
                    "history.get",
                    json!({
                        "output": ["clock", "value"],
                        "history": value_type,
                        "itemids": [item_id],
                        "time_from": from,
                        "sortfield": "clock",
                        "sortorder": "ASC",
                        "limit": 500,
                    }),
                )
                .await?;
            return Ok(points_from(&result, "value"));
        }
        let result = self
            .call(
                "trend.get",
                json!({
                    "output": ["clock", "value_avg"],
                    "itemids": [item_id],
                    "time_from": from,
                    "sortfield": "clock",
                    "sortorder": "ASC",
                    "limit": 500,
                }),
            )
            .await?;
        Ok(points_from(&result, "value_avg"))
    }

    pub async fn agent_version(&self, host_id: &str) -> Result<Option<String>, AppError> {
        let items = self.items_for_hosts(&[host_id.to_string()]).await?;
        Ok(last_value(&items, host_id, &["agent.version"]))
    }

    /// 创建主机维护期。返回 maintenanceid。
    pub async fn create_maintenance(
        &self,
        name: &str,
        host_ids: &[String],
        period_secs: i64,
    ) -> Result<String, AppError> {
        let now = chrono_now();
        let till = now + period_secs;
        let result = self
            .call(
                "maintenance.create",
                json!({
                    "name": name,
                    "active_since": now,
                    "active_till": till,
                    "maintenance_type": 0,
                    "hostids": host_ids,
                    "timeperiods": [{
                        "timeperiod_type": 0,
                        "start_date": now,
                        "period": period_secs,
                    }],
                }),
            )
            .await?;
        result["maintenanceids"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::internal("创建维护期未返回 ID"))
    }

    pub async fn delete_maintenance(&self, maintenance_id: &str) -> Result<(), AppError> {
        let _ = self
            .call("maintenance.delete", json!([maintenance_id]))
            .await?;
        Ok(())
    }

    /// Proxy 列表与在线状态。
    pub async fn list_proxies(&self, stale_secs: i64) -> Result<Vec<Value>, AppError> {
        let result = self
            .call(
                "proxy.get",
                json!({
                    "output": ["proxyid", "host", "status", "lastaccess", "description"],
                }),
            )
            .await;
        let result = match result {
            Ok(v) => v,
            Err(e) => {
                // 部分版本字段名不同，退化为最少字段
                tracing::warn!(error = %e, "proxy.get 扩展字段失败，尝试精简输出");
                self.call(
                    "proxy.get",
                    json!({ "output": ["proxyid", "host", "lastaccess"] }),
                )
                .await?
            }
        };
        let now = chrono_now();
        let stale = stale_secs.max(60);
        Ok(result
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                let last = p["lastaccess"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| p["lastaccess"].as_i64())
                    .unwrap_or(0);
                let age = if last > 0 { now - last } else { i64::MAX };
                let online = last > 0 && age <= stale;
                json!({
                    "proxyId": p["proxyid"].as_str().unwrap_or(""),
                    "name": p["host"].as_str().unwrap_or(""),
                    "lastAccess": last,
                    "ageSecs": if age == i64::MAX { Value::Null } else { json!(age) },
                    "online": online,
                })
            })
            .collect())
    }

    /// 模板偏离：对照标准模板名。standard 为空时只返回主机已挂模板。
    pub async fn template_drift(
        &self,
        standard: &[String],
        limit: u32,
    ) -> Result<Vec<Value>, AppError> {
        let limit = limit.clamp(1, 500);
        let result = self
            .call(
                "host.get",
                json!({
                    "output": ["hostid", "host", "name"],
                    "selectParentTemplates": ["templateid", "name"],
                    "filter": { "status": 0 },
                    "sortfield": "host",
                    "limit": limit,
                }),
            )
            .await?;
        let standard_set: std::collections::BTreeSet<String> =
            standard.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        Ok(result
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|h| {
                let templates: Vec<String> = h["parentTemplates"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
                    .collect();
                let have: std::collections::BTreeSet<_> = templates.iter().cloned().collect();
                let missing: Vec<String> = if standard_set.is_empty() {
                    Vec::new()
                } else {
                    standard_set.difference(&have).cloned().collect()
                };
                let extra: Vec<String> = if standard_set.is_empty() {
                    Vec::new()
                } else {
                    have.difference(&standard_set).cloned().collect()
                };
                let drifted = !missing.is_empty() || templates.is_empty();
                // 未配置标准时返回全部；配置了标准时只返回偏离项
                if !standard_set.is_empty() && !drifted {
                    return None;
                }
                Some(json!({
                    "hostId": h["hostid"].as_str().unwrap_or(""),
                    "hostname": h["host"].as_str().unwrap_or(""),
                    "displayName": h["name"].as_str().unwrap_or(""),
                    "templates": templates,
                    "missing": missing,
                    "extra": extra,
                    "drifted": drifted || (!standard_set.is_empty() && !missing.is_empty()),
                }))
            })
            .collect())
    }

    /// 采集队列积压概况。
    /// Zabbix JSON-RPC 没有 queue.*（前端队列走 server 10051 协议）；
    /// 这里读内部监控项 `zabbix[queue]`（Template App Zabbix Server 通常自带）。
    pub async fn queue_overview(&self, warn_count: u32) -> Result<Value, AppError> {
        match self.queue_from_internal_item(warn_count).await {
            Ok(v) => Ok(v),
            Err(e) => Ok(json!({
                "available": false,
                "count": 0,
                "warn": warn_count,
                "backedUp": false,
                "source": "unavailable",
                "message": format!(
                    "未找到监控项 zabbix[queue]，请在 Zabbix Server 主机启用该内部指标（{e}）"
                ),
            })),
        }
    }

    async fn queue_from_internal_item(&self, warn_count: u32) -> Result<Value, AppError> {
        let items = self
            .call(
                "item.get",
                json!({
                    "output": ["itemid", "name", "key_", "lastvalue", "status", "state", "hostid"],
                    "filter": { "key_": ["zabbix[queue]"], "status": 0 },
                    "limit": 20,
                }),
            )
            .await?;
        let arr = items.as_array().cloned().unwrap_or_default();
        if arr.is_empty() {
            return Err(AppError::internal("item.get 无匹配项"));
        }
        // 多节点时取最大值，避免漏报积压
        let mut count: u32 = 0;
        for item in &arr {
            let v = item
                .get("lastvalue")
                .and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<u32>().ok())
                        .or_else(|| v.as_u64().map(|n| n as u32))
                })
                .unwrap_or(0);
            count = count.max(v);
        }
        Ok(json!({
            "available": true,
            "count": count,
            "warn": warn_count,
            "backedUp": count >= warn_count,
            "source": "zabbix[queue]",
        }))
    }

    pub async fn governance(
        &self,
        standard_templates: &[String],
        proxy_stale_secs: i64,
        queue_warn: u32,
    ) -> Result<Value, AppError> {
        let proxies = self.list_proxies(proxy_stale_secs).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "读取 Proxy 失败");
            Vec::new()
        });
        let template_drift = self
            .template_drift(standard_templates, 200)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "读取模板偏离失败");
                Vec::new()
            });
        let queue = self.queue_overview(queue_warn).await.unwrap_or_else(|e| {
            json!({
                "available": false,
                "count": 0,
                "warn": queue_warn,
                "backedUp": false,
                "message": e.to_string(),
            })
        });
        let offline_proxies = proxies
            .iter()
            .filter(|p| p["online"].as_bool() != Some(true))
            .count();
        Ok(json!({
            "proxies": proxies,
            "offlineProxyCount": offline_proxies,
            "templateDrift": template_drift,
            "driftCount": template_drift.len(),
            "queue": queue,
            "standardTemplates": standard_templates,
        }))
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, AppError> {
        let needs_auth = method != "apiinfo.version";
        let auth = if needs_auth {
            Some(self.resolve_auth_mode().await?)
        } else {
            None
        };
        self.call_with_auth(method, params, auth).await
    }

    async fn call_with_auth(
        &self,
        method: &str,
        params: Value,
        auth: Option<AuthMode>,
    ) -> Result<Value, AppError> {
        let mut body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });
        let mut request = self
            .http
            .post(&self.instance.api_url)
            .header("Content-Type", "application/json");

        match auth {
            Some(AuthMode::Header) => {
                request = request.header("Authorization", format!("Bearer {}", self.token));
            }
            Some(AuthMode::Body) => {
                body["auth"] = json!(self.token);
            }
            None => {}
        }

        let response = request
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::internal(format!("调用 Zabbix 失败: {e}")))?;
        if !response.status().is_success() {
            return Err(AppError::internal(format!(
                "Zabbix HTTP {}",
                response.status()
            )));
        }
        let payload: Value = response
            .json()
            .await
            .map_err(|e| AppError::internal(format!("解析 Zabbix 响应失败: {e}")))?;
        if let Some(err) = payload.get("error") {
            return Err(AppError::internal(format!("Zabbix API 错误: {err}")));
        }
        Ok(payload.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn resolve_auth_mode(&self) -> Result<AuthMode, AppError> {
        if let Some(mode) = self.auth_mode.get() {
            return Ok(*mode);
        }
        let mode = match self.instance.api_auth.trim().to_ascii_lowercase().as_str() {
            "header" | "bearer" => AuthMode::Header,
            "body" | "auth" => AuthMode::Body,
            _ => {
                // auto：无鉴权取版本；7.2 以下用 body auth（兼容反代剥 Header）
                let ver = self
                    .call_with_auth("apiinfo.version", json!({}), None)
                    .await?;
                let ver = ver.as_str().unwrap_or("");
                if zabbix_version_lt_7_2(ver) {
                    tracing::info!(version = ver, "Zabbix < 7.2，API 鉴权使用 body auth");
                    AuthMode::Body
                } else {
                    tracing::info!(version = ver, "Zabbix >= 7.2，API 鉴权使用 Authorization Bearer");
                    AuthMode::Header
                }
            }
        };
        let _ = self.auth_mode.set(mode);
        Ok(mode)
    }
}

/// 解析 x.y.z；无法解析时当作旧版，走 body auth。
fn zabbix_version_lt_7_2(version: &str) -> bool {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
    major < 7 || (major == 7 && minor < 2)
}

fn map_host(host: &Value, items: &[Value]) -> Value {
    let host_id = host["hostid"].as_str().unwrap_or("");
    let ip = primary_ip(host);
    let version = last_value(items, host_id, &["agent.version"]);
    let ping = last_value(items, host_id, &["agent.ping"]);
    let available = interface_available(host).or_else(|| match ping.as_deref() {
        Some("1") => Some(true),
        Some("0") => Some(false),
        _ => None,
    });
    json!({
        "hostId": host_id,
        "hostname": host["host"].as_str().unwrap_or(""),
        "displayName": host["name"].as_str().unwrap_or(""),
        "ip": ip,
        "agentAvailable": available,
        "agentVersion": version,
    })
}

fn primary_ip(host: &Value) -> String {
    let Some(list) = host["interfaces"].as_array() else {
        return String::new();
    };
    let agent = list.iter().find(|i| i["type"].as_str() == Some("1"));
    let chosen = agent
        .or_else(|| list.iter().find(|i| i["main"].as_str() == Some("1")))
        .or_else(|| list.first());
    chosen
        .and_then(|i| i["ip"].as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| chosen.and_then(|i| i["dns"].as_str()))
        .unwrap_or("")
        .to_string()
}

fn interface_available(host: &Value) -> Option<bool> {
    let list = host["interfaces"].as_array()?;
    let agent = list.iter().find(|i| i["type"].as_str() == Some("1"))?;
    match agent["available"].as_str() {
        Some("1") => Some(true),
        Some("2") => Some(false),
        _ => None,
    }
}

fn last_value(items: &[Value], host_id: &str, keys: &[&str]) -> Option<String> {
    pick_item(items, host_id, keys).and_then(|item| {
        item["lastvalue"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    })
}

fn pick_item<'a>(items: &'a [Value], host_id: &str, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| {
        items.iter().find(|item| {
            item["hostid"].as_str() == Some(host_id) && item["key_"].as_str() == Some(*key)
        })
    })
}

fn points_from(result: &Value, value_field: &str) -> Vec<Value> {
    result
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            json!({
                "clock": p["clock"].as_str().unwrap_or(""),
                "value": p[value_field].as_str().unwrap_or(""),
            })
        })
        .collect()
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
