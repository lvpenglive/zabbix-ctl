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
    // Zabbix 7 Linux 模板用 vm.memory.util；旧模板常见 utilization / pused
    "vm.memory.util",
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
            "selectHostGroups": ["groupid", "name"],
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

    /// 已启用主机的轻量名册。不查监控项；Agent 通断只看接口 available。
    pub async fn enabled_hosts_light(&self) -> Result<Vec<Value>, AppError> {
        let result = self
            .call(
                "host.get",
                json!({
                    "output": ["hostid", "host", "name", "status"],
                    "selectInterfaces": ["ip", "dns", "type", "main", "available"],
                    "selectHostGroups": ["groupid", "name"],
                    "filter": { "status": 0 },
                    "sortfield": "host",
                }),
            )
            .await?;
        let hosts = result.as_array().cloned().unwrap_or_default();
        Ok(hosts.iter().map(|h| map_host(h, &[])).collect())
    }

    /// 只给当前页补 agent.version，避免全量 item.get。
    pub async fn attach_agent_versions(&self, hosts: &mut [Value]) -> Result<(), AppError> {
        let ids: Vec<String> = hosts
            .iter()
            .filter_map(|h| h["hostId"].as_str().map(|s| s.to_string()))
            .filter(|s| !s.is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let items = self
            .items_for_hosts_with_keys(&ids, &["agent.version"])
            .await?;
        for host in hosts.iter_mut() {
            let id = host["hostId"].as_str().unwrap_or("");
            if let Some(version) = last_value(&items, id, &["agent.version"]) {
                host["agentVersion"] = json!(version);
            }
        }
        Ok(())
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
                &[
                    "vm.memory.util",
                    "vm.memory.utilization",
                    "vm.memory.size[pused]",
                ][..],
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

    /// 主机监控项只读列表，含最新值。不限制 key，按名称/键搜索可选。
    pub async fn host_items(
        &self,
        host_id: &str,
        search: Option<&str>,
    ) -> Result<Vec<Value>, AppError> {
        self.ensure_host(host_id).await?;
        let mut params = json!({
            "output": [
                "itemid", "name", "key_", "lastvalue", "lastclock",
                "status", "state", "units", "value_type", "templateid", "error"
            ],
            "hostids": [host_id],
            "sortfield": "name",
            "limit": 500,
        });
        if let Some(q) = search.map(str::trim).filter(|s| !s.is_empty()) {
            params["search"] = json!({ "name": q, "key_": q });
            params["searchByAny"] = json!(true);
        }
        let result = self.call("item.get", params).await?;
        let rows = result.as_array().cloned().unwrap_or_default();
        let parent_ids: Vec<String> = rows
            .iter()
            .filter_map(|item| {
                let id = item["templateid"].as_str().unwrap_or("0");
                if id == "0" || id.is_empty() {
                    None
                } else {
                    Some(id.to_string())
                }
            })
            .collect();
        let parents = self.templates_of_parent_items(&parent_ids).await?;
        Ok(rows
            .into_iter()
            .map(|item| {
                let parent_id = item["templateid"].as_str().unwrap_or("0");
                let inherited = parent_id != "0" && !parent_id.is_empty();
                let (template_id, template_name) = parents
                    .get(parent_id)
                    .cloned()
                    .unwrap_or_default();
                json!({
                    "itemId": item["itemid"].as_str().unwrap_or(""),
                    "name": item["name"].as_str().unwrap_or(""),
                    "key": item["key_"].as_str().unwrap_or(""),
                    "lastValue": item["lastvalue"].as_str().unwrap_or(""),
                    "lastClock": item["lastclock"].as_str().unwrap_or("0"),
                    "units": item["units"].as_str().unwrap_or(""),
                    "status": item["status"].as_str().unwrap_or("0"),
                    "state": item["state"].as_str().unwrap_or("0"),
                    "valueType": item["value_type"].as_str().unwrap_or(""),
                    "inherited": inherited,
                    "templateId": template_id,
                    "templateName": template_name,
                    "error": item["error"].as_str().unwrap_or(""),
                })
            })
            .collect())
    }

    /// 主机触发器只读列表。
    pub async fn host_triggers(&self, host_id: &str) -> Result<Vec<Value>, AppError> {
        self.ensure_host(host_id).await?;
        let result = self
            .call(
                "trigger.get",
                json!({
                    "output": [
                        "triggerid", "description", "priority", "value",
                        "status", "state", "lastchange", "templateid", "error"
                    ],
                    "hostids": [host_id],
                    "sortfield": "priority",
                    "sortorder": "DESC",
                    "limit": 500,
                }),
            )
            .await?;
        let rows = result.as_array().cloned().unwrap_or_default();
        let parent_ids: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                let id = row["templateid"].as_str().unwrap_or("0");
                if id == "0" || id.is_empty() {
                    None
                } else {
                    Some(id.to_string())
                }
            })
            .collect();
        let parents = self.templates_of_parent_triggers(&parent_ids).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let parent_id = row["templateid"].as_str().unwrap_or("0");
                let inherited = parent_id != "0" && !parent_id.is_empty();
                let (template_id, template_name) = parents
                    .get(parent_id)
                    .cloned()
                    .unwrap_or_default();
                json!({
                    "triggerId": row["triggerid"].as_str().unwrap_or(""),
                    "name": row["description"].as_str().unwrap_or(""),
                    "severity": row["priority"].as_str().unwrap_or("0"),
                    "value": row["value"].as_str().unwrap_or("0"),
                    "status": row["status"].as_str().unwrap_or("0"),
                    "state": row["state"].as_str().unwrap_or("0"),
                    "lastChange": row["lastchange"].as_str().unwrap_or("0"),
                    "inherited": inherited,
                    "templateId": template_id,
                    "templateName": template_name,
                    "error": row["error"].as_str().unwrap_or(""),
                })
            })
            .collect())
    }

    /// 父监控项 ID → (模板 ID, 模板名)。`templateid` 指向父监控项，再取该监控项所属模板。
    async fn templates_of_parent_items(
        &self,
        parent_item_ids: &[String],
    ) -> Result<std::collections::HashMap<String, (String, String)>, AppError> {
        let mut map = std::collections::HashMap::new();
        let mut uniq = parent_item_ids.to_vec();
        uniq.sort();
        uniq.dedup();
        if uniq.is_empty() {
            return Ok(map);
        }
        for chunk in uniq.chunks(ITEM_GET_HOST_CHUNK) {
            let result = self
                .call(
                    "item.get",
                    json!({
                        "output": ["itemid"],
                        "itemids": chunk,
                        "selectHosts": ["hostid", "name", "host"],
                    }),
                )
                .await?;
            for item in result.as_array().cloned().unwrap_or_default() {
                let parent_id = item["itemid"].as_str().unwrap_or("").to_string();
                let host = item["hosts"]
                    .as_array()
                    .and_then(|a| a.first())
                    .cloned()
                    .unwrap_or(json!({}));
                let template_id = host["hostid"].as_str().unwrap_or("").to_string();
                let template_name = host["name"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .or_else(|| host["host"].as_str())
                    .unwrap_or("")
                    .to_string();
                if !parent_id.is_empty() {
                    map.insert(parent_id, (template_id, template_name));
                }
            }
        }
        Ok(map)
    }

    async fn templates_of_parent_triggers(
        &self,
        parent_trigger_ids: &[String],
    ) -> Result<std::collections::HashMap<String, (String, String)>, AppError> {
        let mut map = std::collections::HashMap::new();
        let mut uniq = parent_trigger_ids.to_vec();
        uniq.sort();
        uniq.dedup();
        if uniq.is_empty() {
            return Ok(map);
        }
        for chunk in uniq.chunks(ITEM_GET_HOST_CHUNK) {
            let result = self
                .call(
                    "trigger.get",
                    json!({
                        "output": ["triggerid"],
                        "triggerids": chunk,
                        "selectHosts": ["hostid", "name", "host"],
                    }),
                )
                .await?;
            for row in result.as_array().cloned().unwrap_or_default() {
                let parent_id = row["triggerid"].as_str().unwrap_or("").to_string();
                let host = row["hosts"]
                    .as_array()
                    .and_then(|a| a.first())
                    .cloned()
                    .unwrap_or(json!({}));
                let template_id = host["hostid"].as_str().unwrap_or("").to_string();
                let template_name = host["name"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .or_else(|| host["host"].as_str())
                    .unwrap_or("")
                    .to_string();
                if !parent_id.is_empty() {
                    map.insert(parent_id, (template_id, template_name));
                }
            }
        }
        Ok(map)
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

    pub async fn agent_ping_up(&self, host_id: &str) -> Result<bool, AppError> {
        let items = self.items_for_hosts(&[host_id.to_string()]).await?;
        Ok(last_value(&items, host_id, &["agent.ping"]).as_deref() == Some("1"))
    }

    pub async fn find_existing_host(&self, hostname: &str, ip: &str) -> Result<Option<String>, AppError> {
        let by_name = if hostname.is_empty() {
            None
        } else {
            self.find_host_id_by_name(hostname).await?
        };
        let by_ip = if ip.is_empty() {
            None
        } else {
            self.find_host_id_by_ip(ip).await?
        };
        if let (Some(a), Some(b)) = (&by_name, &by_ip) {
            if a != b {
                return Err(AppError::bad("同名主机与同 IP 主机不是同一台"));
            }
        }
        Ok(by_name.or(by_ip))
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

    pub fn for_write(http: reqwest::Client, instance: ZabbixInstanceConfig) -> Result<Self, AppError> {
        let token = instance.resolved_write_token().ok_or_else(|| {
            AppError::bad(format!(
                "未配置 Zabbix 写令牌（toml [[zabbix]].api_token_write 或环境变量 {}）",
                instance.write_token_env_key()
            ))
        })?;
        Ok(Self {
            http,
            instance,
            token,
            auth_mode: OnceLock::new(),
        })
    }

    pub async fn list_template_groups(&self) -> Result<Vec<Value>, AppError> {
        let result = self
            .call(
                "templategroup.get",
                json!({
                    "output": ["groupid", "name"],
                    "sortfield": "name",
                }),
            )
            .await?;
        Ok(map_id_names(&result, "groupid", "groupId"))
    }

    pub async fn list_host_groups(&self) -> Result<Vec<Value>, AppError> {
        let result = self
            .call(
                "hostgroup.get",
                json!({
                    "output": ["groupid", "name"],
                    "sortfield": "name",
                }),
            )
            .await?;
        Ok(map_id_names(&result, "groupid", "groupId"))
    }

    pub async fn hosts_in_group(&self, group_id: &str) -> Result<Vec<Value>, AppError> {
        require_zbx_id(group_id)?;
        let result = self
            .call(
                "host.get",
                json!({
                    "output": ["hostid", "host", "name"],
                    "groupids": [group_id],
                    "selectInterfaces": ["ip", "dns", "type", "main"],
                    "selectHostGroups": ["groupid", "name"],
                    "filter": { "status": 0 },
                    "sortfield": "host",
                }),
            )
            .await?;
        let hosts = result.as_array().cloned().unwrap_or_default();
        Ok(hosts
            .iter()
            .map(|h| {
                json!({
                    "hostId": h["hostid"].as_str().unwrap_or(""),
                    "hostname": h["host"].as_str().unwrap_or(""),
                    "displayName": h["name"].as_str().unwrap_or(""),
                    "ip": primary_ip(h),
                    "groups": map_id_names(&h["hostgroups"], "groupid", "groupId"),
                })
            })
            .collect())
    }

    pub async fn list_templates(&self, group_id: Option<&str>) -> Result<Vec<Value>, AppError> {
        let mut params = json!({
            "output": ["templateid", "host", "name"],
            "selectTemplateGroups": ["groupid", "name"],
            "selectParentTemplates": ["templateid", "name"],
            "sortfield": "name",
        });
        if let Some(gid) = group_id.map(str::trim).filter(|s| !s.is_empty()) {
            require_zbx_id(gid)?;
            params["groupids"] = json!([gid]);
        }
        let result = self.call("template.get", params).await?;
        Ok(result
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(map_template)
            .collect())
    }

    pub async fn host_linkage(&self, host_id: &str) -> Result<Value, AppError> {
        self.ensure_host(host_id).await?;
        let result = self
            .call(
                "host.get",
                json!({
                    "output": ["hostid"],
                    "hostids": [host_id],
                    "selectHostGroups": ["groupid", "name"],
                    "selectParentTemplates": ["templateid", "name"],
                }),
            )
            .await?;
        let host = result
            .as_array()
            .and_then(|a| a.first())
            .cloned()
            .unwrap_or_else(|| json!({}));
        Ok(json!({
            "groups": map_id_names(&host["hostgroups"], "groupid", "groupId"),
            "templates": map_id_names(&host["parentTemplates"], "templateid", "templateId"),
        }))
    }

    pub async fn link_templates(
        &self,
        host_id: &str,
        template_ids: &[String],
        action: &str,
    ) -> Result<Value, AppError> {
        self.ensure_host(host_id).await?;
        for id in template_ids {
            require_zbx_id(id)?;
        }
        if template_ids.is_empty() {
            return Err(AppError::bad("模板列表为空"));
        }
        let linkage = self.host_linkage(host_id).await?;
        let mut ids = ids_of(&linkage["templates"], "templateId");
        apply_link(&mut ids, template_ids, action)?;
        self.call(
            "host.update",
            json!({
                "hostid": host_id,
                "templates": ids.iter().map(|id| json!({"templateid": id})).collect::<Vec<_>>(),
            }),
        )
        .await?;
        self.host_linkage(host_id).await
    }

    pub async fn link_groups(
        &self,
        host_id: &str,
        group_ids: &[String],
        action: &str,
    ) -> Result<Value, AppError> {
        self.ensure_host(host_id).await?;
        for id in group_ids {
            require_zbx_id(id)?;
        }
        if group_ids.is_empty() {
            return Err(AppError::bad("主机组列表为空"));
        }
        let linkage = self.host_linkage(host_id).await?;
        let mut ids = ids_of(&linkage["groups"], "groupId");
        apply_link(&mut ids, group_ids, action)?;
        if ids.is_empty() {
            return Err(AppError::bad("主机至少要属于一个主机组"));
        }
        self.call(
            "host.update",
            json!({
                "hostid": host_id,
                "groups": ids.iter().map(|id| json!({"groupid": id})).collect::<Vec<_>>(),
            }),
        )
        .await?;
        self.host_linkage(host_id).await
    }

    pub async fn host_macros(&self, host_id: &str) -> Result<Vec<Value>, AppError> {
        self.ensure_host(host_id).await?;
        let result = self
            .call(
                "usermacro.get",
                json!({
                    "output": ["hostmacroid", "macro", "value", "description", "type"],
                    "hostids": [host_id],
                    "sortfield": "macro",
                }),
            )
            .await?;
        Ok(result
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|m| {
                json!({
                    "hostMacroId": m["hostmacroid"].as_str().unwrap_or(""),
                    "macro": m["macro"].as_str().unwrap_or(""),
                    "value": m["value"].as_str().unwrap_or(""),
                    "description": m["description"].as_str().unwrap_or(""),
                    "secret": m["type"].as_str() == Some("1"),
                })
            })
            .collect())
    }

    pub async fn save_host_macros(&self, host_id: &str, macros: &[Value]) -> Result<Vec<Value>, AppError> {
        self.ensure_host(host_id).await?;
        let existing = self.host_macros(host_id).await?;
        let mut by_name = std::collections::HashMap::new();
        for item in &existing {
            if let Some(name) = item["macro"].as_str() {
                by_name.insert(name.to_string(), item.clone());
            }
        }
        for item in macros {
            let name = item["macro"].as_str().unwrap_or("").trim();
            require_macro(name)?;
            let value = item["value"].as_str().unwrap_or("");
            let description = item["description"].as_str().unwrap_or("");
            if let Some(old) = by_name.get(name) {
                if old["secret"].as_bool() == Some(true) && (value.is_empty() || value == "******") {
                    continue;
                }
                let id = old["hostMacroId"].as_str().unwrap_or("");
                self.call(
                    "usermacro.update",
                    json!({
                        "hostmacroid": id,
                        "value": value,
                        "description": description,
                    }),
                )
                .await?;
            } else {
                self.call(
                    "usermacro.create",
                    json!({
                        "hostid": host_id,
                        "macro": name,
                        "value": value,
                        "description": description,
                    }),
                )
                .await?;
            }
        }
        self.host_macros(host_id).await
    }

    pub async fn register_host(&self, body: &Value) -> Result<Value, AppError> {
        let hostname = body["hostname"].as_str().unwrap_or("").trim();
        let display = body["displayName"].as_str().unwrap_or(hostname).trim();
        let ip = body["ip"].as_str().unwrap_or("").trim();
        if hostname.is_empty() || ip.is_empty() {
            return Err(AppError::bad("主机名和管理 IP 不能为空"));
        }
        let group_ids = string_list(&body["groupIds"]);
        let template_ids = string_list(&body["templateIds"]);
        if group_ids.is_empty() {
            return Err(AppError::bad("请选择主机组"));
        }
        for id in group_ids.iter().chain(template_ids.iter()) {
            require_zbx_id(id)?;
        }
        let by_name = self.find_host_id_by_name(hostname).await?;
        let by_ip = self.find_host_id_by_ip(ip).await?;
        if let (Some(a), Some(b)) = (&by_name, &by_ip) {
            if a != b {
                return Err(AppError::bad("同名主机与同 IP 主机不是同一台，未修改"));
            }
        }
        if let Some(host_id) = by_name.or(by_ip) {
            self.link_groups(&host_id, &group_ids, "link").await?;
            if !template_ids.is_empty() {
                self.link_templates(&host_id, &template_ids, "link").await?;
            }
            return Ok(json!({ "hostId": host_id, "created": false }));
        }
        let created = self
            .call(
                "host.create",
                json!({
                    "host": hostname,
                    "name": display,
                    "groups": group_ids.iter().map(|id| json!({"groupid": id})).collect::<Vec<_>>(),
                    "templates": template_ids.iter().map(|id| json!({"templateid": id})).collect::<Vec<_>>(),
                    "interfaces": [{
                        "type": 1,
                        "main": 1,
                        "useip": 1,
                        "ip": ip,
                        "dns": "",
                        "port": "10050"
                    }],
                }),
            )
            .await?;
        let host_id = created["hostids"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if host_id.is_empty() {
            return Err(AppError::internal("Zabbix 未返回新主机 ID"));
        }
        Ok(json!({ "hostId": host_id, "created": true }))
    }

    pub async fn event_hosts(&self, event_id: &str) -> Result<Vec<String>, AppError> {
        require_zbx_id(event_id)?;
        let result = self
            .call(
                "event.get",
                json!({
                    "output": ["eventid"],
                    "eventids": [event_id],
                    "selectHosts": ["hostid"],
                }),
            )
            .await?;
        let event = result
            .as_array()
            .and_then(|a| a.first())
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("未找到事件 {event_id}")))?;
        let ids = event["hosts"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|h| h["hostid"].as_str().map(|s| s.to_string()))
            .collect();
        Ok(ids)
    }

    pub async fn acknowledge_event(&self, event_id: &str, body: &Value) -> Result<Value, AppError> {
        require_zbx_id(event_id)?;
        let mut action = 2;
        let message = body["message"].as_str().unwrap_or("").trim().to_string();
        if !message.is_empty() {
            action |= 4;
        }
        let mut params = json!({
            "eventids": [event_id],
            "action": action,
        });
        if !message.is_empty() {
            params["message"] = json!(message);
        }
        if let Some(severity) = body["severity"].as_u64() {
            if severity > 5 {
                return Err(AppError::bad("严重级别必须是 0 到 5"));
            }
            action |= 8;
            params["action"] = json!(action);
            params["severity"] = json!(severity);
        }
        if body["close"].as_bool() == Some(true) {
            action |= 1;
            params["action"] = json!(action);
        }
        self.call("event.acknowledge", params).await?;
        Ok(json!({ "eventId": event_id }))
    }

    async fn find_host_id_by_name(&self, hostname: &str) -> Result<Option<String>, AppError> {
        let result = self
            .call(
                "host.get",
                json!({
                    "output": ["hostid"],
                    "filter": { "host": [hostname] },
                    "limit": 1,
                }),
            )
            .await?;
        Ok(result
            .as_array()
            .and_then(|a| a.first())
            .and_then(|h| h["hostid"].as_str().map(|s| s.to_string())))
    }

    async fn find_host_id_by_ip(&self, ip: &str) -> Result<Option<String>, AppError> {
        let result = self
            .call(
                "hostinterface.get",
                json!({
                    "output": ["hostid", "ip"],
                    "filter": { "ip": [ip] },
                    "limit": 1,
                }),
            )
            .await?;
        Ok(result
            .as_array()
            .and_then(|a| a.first())
            .and_then(|h| h["hostid"].as_str().map(|s| s.to_string())))
    }

    pub fn protected(&self) -> bool {
        self.instance.protected
    }

    pub(crate) async fn call(&self, method: &str, params: Value) -> Result<Value, AppError> {
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
            return Err(AppError::bad(zabbix_error_message(err)));
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

fn require_zbx_id(id: &str) -> Result<(), AppError> {
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
        return Err(AppError::bad("标识无效"));
    }
    Ok(())
}

fn require_macro(name: &str) -> Result<(), AppError> {
    let ok = name.starts_with("{$")
        && name.ends_with('}')
        && name.len() > 3
        && name.len() <= 255
        && name.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '{' | '}' | '$' | '_' | '.' | ':')
        });
    if !ok {
        return Err(AppError::bad(format!("宏格式无效: {name}")));
    }
    Ok(())
}

fn apply_link(current: &mut Vec<String>, incoming: &[String], action: &str) -> Result<(), AppError> {
    match action {
        "link" => {
            for id in incoming {
                if !current.iter().any(|x| x == id) {
                    current.push(id.clone());
                }
            }
            Ok(())
        }
        "unlink" => {
            current.retain(|id| !incoming.iter().any(|x| x == id));
            Ok(())
        }
        _ => Err(AppError::bad("action 只能是 link 或 unlink")),
    }
}

fn ids_of(value: &Value, key: &str) -> Vec<String> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| item[key].as_str().map(|s| s.to_string()))
        .collect()
}

fn string_list(value: &Value) -> Vec<String> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
        .filter(|s| !s.is_empty())
        .collect()
}

fn map_template(template: &Value) -> Value {
    json!({
        "templateId": template["templateid"].as_str().unwrap_or(""),
        "host": template["host"].as_str().unwrap_or(""),
        "name": template["name"].as_str().unwrap_or(""),
        "groups": map_id_names(&template["templategroups"], "groupid", "groupId"),
        "parentTemplates": map_id_names(&template["parentTemplates"], "templateid", "templateId"),
    })
}

fn map_id_names(value: &Value, id_key: &str, out_key: &str) -> Vec<Value> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|item| {
            json!({
                out_key: item[id_key].as_str().unwrap_or(""),
                "name": item["name"].as_str().unwrap_or(""),
            })
        })
        .collect()
}

fn zabbix_error_message(err: &Value) -> String {
    err.get("data")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| err.get("message").and_then(|v| v.as_str()))
        .unwrap_or("Zabbix API 错误")
        .to_string()
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
        "groups": map_id_names(&host["hostgroups"], "groupid", "groupId"),
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
