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
            "output": self.host_list_output().await,
            "selectInterfaces": [
                "ip", "dns", "port", "type", "main",
                "available", "error", "errors_from", "disable_until"
            ],
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

    /// 主机轻量名册（含启用/禁用）。不查监控项；通断先看接口 available，列表分页后再用 agent.ping 覆盖。
    pub async fn enabled_hosts_light(&self) -> Result<Vec<Value>, AppError> {
        self.hosts_light(None).await
    }

    /// `status_filter`: None=全部，Some(0)=启用，Some(1)=禁用。
    pub async fn hosts_light(&self, status_filter: Option<i64>) -> Result<Vec<Value>, AppError> {
        let mut params = json!({
            "output": self.host_list_output().await,
            "selectInterfaces": [
                "ip", "dns", "port", "type", "main",
                "available", "error", "errors_from", "disable_until"
            ],
            "selectHostGroups": ["groupid", "name"],
            "sortfield": "host",
        });
        if let Some(status) = status_filter {
            params["filter"] = json!({ "status": status });
        }
        let result = self.call("host.get", params).await?;
        let hosts = result.as_array().cloned().unwrap_or_default();
        Ok(hosts.iter().map(|h| map_host(h, &[])).collect())
    }

    /// host.get 名册字段：含 Proxy 通道，便于同步写入对照表。
    async fn host_list_output(&self) -> Vec<&'static str> {
        let ver = self.version().await.unwrap_or_default();
        let mut output = vec!["hostid", "host", "name", "status", "proxy_hostid"];
        if !zabbix_version_lt_7_0(&ver) {
            output.extend(["monitored_by", "proxyid", "proxy_groupid"]);
        }
        output
    }

    /// 只给当前页补 agent.version / agent.ping，避免全量 item.get。
    /// Agent 通断：看 ping 项 state/lastvalue，且 ZBX 接口不可用时强制为断（避免 lastvalue 残留 1）。
    /// 接口「可用性」徽章仍用 availability.*，不在此改。
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
            .items_for_hosts_with_keys(&ids, &["agent.version", "agent.ping"])
            .await?;
        for host in hosts.iter_mut() {
            let id = host["hostId"].as_str().unwrap_or("").to_string();
            let version = last_value(&items, &id, &["agent.version"]);
            if let Some(version) = version {
                host["agentVersion"] = json!(version);
            }
            let ping_item = pick_item(&items, &id, &["agent.ping"]);
            let zbx_iface = mapped_zbx_iface_available(host);
            if let Some(available) = resolve_agent_available(ping_item, zbx_iface) {
                host["agentAvailable"] = json!(available);
            }
            host["agentStatus"] = json!(agent_status_code(
                mapped_has_agent_iface(host),
                host["agentAvailable"].as_bool(),
            ));
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
                        "output": [
                            "itemid", "hostid", "key_", "lastvalue", "lastclock",
                            "value_type", "state", "status", "error"
                        ],
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

    /// Proxy 列表与在线状态；尽量带回 Agent 可达地址。
    /// Zabbix 7+ 用 name/operating_mode/address；6.x 用 host/status。
    pub async fn list_proxies(&self, stale_secs: i64) -> Result<Vec<Value>, AppError> {
        let result = match self
            .call(
                "proxy.get",
                json!({
                    "output": [
                        "proxyid", "name", "operating_mode", "lastaccess", "description",
                        "address", "proxy_groupid"
                    ],
                }),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "proxy.get(7.x 字段)失败，尝试 6.x 字段");
                match self
                    .call(
                        "proxy.get",
                        json!({
                            "output": [
                                "proxyid", "host", "status", "lastaccess", "description",
                                "proxy_address"
                            ],
                        }),
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e2) => {
                        tracing::warn!(error = %e2, "proxy.get 中等字段失败，尝试最少字段");
                        match self
                            .call(
                                "proxy.get",
                                json!({ "output": ["proxyid", "name", "lastaccess"] }),
                            )
                            .await
                        {
                            Ok(v) => v,
                            Err(_) => {
                                self.call(
                                    "proxy.get",
                                    json!({ "output": ["proxyid", "host", "lastaccess"] }),
                                )
                                .await?
                            }
                        }
                    }
                }
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
                let name = p["name"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .or_else(|| p["host"].as_str().filter(|s| !s.is_empty()))
                    .unwrap_or("");
                let address = p["address"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .or_else(|| p["proxy_address"].as_str().filter(|s| !s.trim().is_empty()))
                    .unwrap_or("");
                json!({
                    "proxyId": p["proxyid"].as_str().unwrap_or(""),
                    "name": name,
                    "address": address,
                    "lastAccess": last,
                    "ageSecs": if age == i64::MAX { Value::Null } else { json!(age) },
                    "online": online,
                })
            })
            .collect())
    }

    /// Zabbix 7+ Proxy Group 列表（含成员地址，供 ServerActive 分号拼装）。
    pub async fn list_proxy_groups(&self, stale_secs: i64) -> Result<Vec<Value>, AppError> {
        let ver = self.version().await.unwrap_or_default();
        if zabbix_version_lt_7_0(&ver) {
            return Ok(vec![]);
        }
        let result = match self
            .call(
                "proxygroup.get",
                json!({
                    "output": [
                        "proxy_groupid", "name", "description", "state",
                        "failover_delay", "min_online"
                    ],
                    "selectProxies": [
                        "proxyid", "name", "address", "lastaccess"
                    ],
                }),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "proxygroup.get 失败，可能未启用 Proxy Group");
                return Ok(vec![]);
            }
        };
        let now = chrono_now();
        let stale = stale_secs.max(60);
        Ok(result
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|g| {
                let proxies = g["proxies"]
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
                        let name = p["name"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .or_else(|| p["host"].as_str())
                            .unwrap_or("");
                        let address = p["address"]
                            .as_str()
                            .filter(|s| !s.trim().is_empty())
                            .or_else(|| {
                                p["proxy_address"]
                                    .as_str()
                                    .filter(|s| !s.trim().is_empty())
                            })
                            .unwrap_or("");
                        json!({
                            "proxyId": p["proxyid"].as_str().unwrap_or(""),
                            "name": name,
                            "address": address,
                            "online": online,
                        })
                    })
                    .collect::<Vec<_>>();
                let addresses: Vec<String> = proxies
                    .iter()
                    .filter_map(|p| {
                        p["address"]
                            .as_str()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                    })
                    .collect();
                let state = g["state"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .or_else(|| g["state"].as_i64())
                    .unwrap_or(0);
                // 3 = Online（见 Proxy group object）
                let online = state == 3;
                json!({
                    "proxyGroupId": g["proxy_groupid"].as_str().unwrap_or(""),
                    "name": g["name"].as_str().unwrap_or(""),
                    "state": state,
                    "online": online,
                    "proxies": proxies,
                    "addresses": addresses,
                    "serverCsv": addresses.join(","),
                    "serverActiveSemicolon": addresses.join(";"),
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
                    "selectInterfaces": ["ip", "dns", "port", "type", "main"],
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
                    "port": primary_port(h),
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
        if hostname.is_empty() {
            return Err(AppError::bad("主机名不能为空"));
        }
        let group_ids = string_list(&body["groupIds"]);
        let template_ids = string_list(&body["templateIds"]);
        if group_ids.is_empty() {
            return Err(AppError::bad("请选择主机组"));
        }
        for id in group_ids.iter().chain(template_ids.iter()) {
            require_zbx_id(id)?;
        }
        let proxy_id = body["proxyId"].as_str().unwrap_or("").trim();
        let proxy_group_id = body["proxyGroupId"].as_str().unwrap_or("").trim();
        if !proxy_id.is_empty() && !proxy_group_id.is_empty() {
            return Err(AppError::bad("不能同时指定 Proxy 与 Proxy Group"));
        }
        if !proxy_id.is_empty() {
            require_zbx_id(proxy_id)?;
        }
        if !proxy_group_id.is_empty() {
            require_zbx_id(proxy_group_id)?;
        }

        let interfaces = build_host_interfaces(body)?;
        let primary_ip = interfaces
            .iter()
            .find(|i| i["main"].as_i64() == Some(1) || i["main"].as_str() == Some("1"))
            .or_else(|| interfaces.first())
            .and_then(|i| {
                let useip = i["useip"].as_i64().unwrap_or(1) == 1
                    || i["useip"].as_str() == Some("1");
                if useip {
                    i["ip"].as_str()
                } else {
                    i["dns"].as_str()
                }
            })
            .unwrap_or("")
            .to_string();

        // 仅按主机名去重，允许同 IP 多主机（如 _icmp / _agent）
        if let Some(host_id) = self.find_host_id_by_name(hostname).await? {
            self.link_groups(&host_id, &group_ids, "link").await?;
            if !template_ids.is_empty() {
                self.link_templates(&host_id, &template_ids, "link").await?;
            }
            if !proxy_group_id.is_empty() {
                self.assign_host_proxy_group(&host_id, proxy_group_id).await?;
            } else if !proxy_id.is_empty() {
                self.assign_host_proxy(&host_id, proxy_id).await?;
            }
            if let Some(iface) = interfaces.first() {
                let port = iface["port"].as_str().unwrap_or("10050");
                let ip = iface["ip"].as_str().unwrap_or(primary_ip.as_str());
                let _ = self.update_agent_interface_port(&host_id, ip, port).await;
            }
            return Ok(json!({ "hostId": host_id, "created": false, "ip": primary_ip }));
        }

        let mut params = json!({
            "host": hostname,
            "name": if display.is_empty() { hostname } else { display },
            "groups": group_ids.iter().map(|id| json!({"groupid": id})).collect::<Vec<_>>(),
            "templates": template_ids.iter().map(|id| json!({"templateid": id})).collect::<Vec<_>>(),
            "interfaces": interfaces,
        });
        if let Some(status) = parse_host_status(body.get("status")) {
            params["status"] = json!(status);
        }
        if let Some(macros) = build_host_macros(body) {
            params["macros"] = json!(macros);
        }
        if let Some(tags) = build_host_tags(body) {
            params["tags"] = json!(tags);
        }
        if let Some(mode) = body.get("inventoryMode").or_else(|| body.get("inventory_mode")) {
            if let Some(n) = mode.as_i64().or_else(|| mode.as_str().and_then(|s| s.parse().ok())) {
                params["inventory_mode"] = json!(n);
            }
        }
        if let Some(inv) = body.get("inventory").filter(|v| v.is_object()) {
            if !inv.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                params["inventory"] = inv.clone();
                if params.get("inventory_mode").is_none() {
                    params["inventory_mode"] = json!(0); // manual
                }
            }
        }
        if !proxy_group_id.is_empty() {
            self.merge_proxy_group_fields(&mut params, proxy_group_id)?;
        } else if !proxy_id.is_empty() {
            self.merge_proxy_fields(&mut params, proxy_id).await?;
        }
        let created = self.call("host.create", params).await?;
        let host_id = created["hostids"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if host_id.is_empty() {
            return Err(AppError::internal("Zabbix 未返回新主机 ID"));
        }
        Ok(json!({ "hostId": host_id, "created": true, "ip": primary_ip }))
    }

    pub async fn get_host_detail(&self, host_id: &str) -> Result<Value, AppError> {
        self.ensure_host(host_id).await?;
        let ver = self.version().await.unwrap_or_default();
        let mut output = vec![
            "hostid",
            "host",
            "name",
            "status",
            "description",
            "inventory_mode",
            "proxy_hostid",
        ];
        if !zabbix_version_lt_7_0(&ver) {
            output.extend(["monitored_by", "proxyid", "proxy_groupid"]);
        }
        let result = self
            .call(
                "host.get",
                json!({
                    "output": output,
                    "hostids": [host_id],
                    "selectInterfaces": "extend",
                    "selectHostGroups": ["groupid", "name"],
                    "selectParentTemplates": ["templateid", "name"],
                    "selectTags": "extend",
                    "selectInventory": "extend",
                    "limit": 1,
                }),
            )
            .await?;
        let host = result
            .as_array()
            .and_then(|a| a.first())
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("未找到主机 {host_id}")))?;

        let (channel, proxy_id, proxy_group_id) =
            host_monitoring_channel(&host, zabbix_version_lt_7_0(&ver));

        let interfaces = host["interfaces"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|i| map_host_interface(&i))
            .collect::<Vec<_>>();

        let inventory_mode = host["inventory_mode"]
            .as_i64()
            .or_else(|| {
                host["inventory_mode"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(-1);
        let inventory = if host["inventory"].is_object() {
            host["inventory"].clone()
        } else {
            json!({})
        };
        let macros = self.host_macros(host_id).await?;
        let groups = map_id_names(&host["hostgroups"], "groupid", "groupId");
        let templates = map_id_names(&host["parentTemplates"], "templateid", "templateId");
        let group_ids: Vec<String> = groups
            .iter()
            .filter_map(|g| g["groupId"].as_str().map(|s| s.to_string()))
            .collect();
        let template_ids: Vec<String> = templates
            .iter()
            .filter_map(|t| t["templateId"].as_str().map(|s| s.to_string()))
            .collect();

        Ok(json!({
            "hostId": host_id,
            "hostname": host["host"].as_str().unwrap_or(""),
            "displayName": host["name"].as_str().unwrap_or(""),
            "status": host["status"].as_str()
                .map(|s| s.to_string())
                .or_else(|| host["status"].as_i64().map(|n| n.to_string()))
                .unwrap_or_else(|| "0".to_string()),
            "description": host["description"].as_str().unwrap_or(""),
            "groupIds": group_ids,
            "groups": groups,
            "templateIds": template_ids,
            "templates": templates,
            "channel": channel,
            "proxyId": proxy_id,
            "proxyGroupId": proxy_group_id,
            "interfaces": interfaces,
            "tags": host["tags"].as_array().cloned().unwrap_or_default().into_iter().map(|t| json!({
                "tag": t["tag"].as_str().unwrap_or(""),
                "value": t["value"].as_str().unwrap_or(""),
            })).collect::<Vec<_>>(),
            "inventoryMode": inventory_mode,
            "inventory": inventory,
            "macros": macros,
        }))
    }

    pub async fn update_host(&self, host_id: &str, body: &Value) -> Result<Value, AppError> {
        self.ensure_host(host_id).await?;
        let hostname = body["hostname"].as_str().unwrap_or("").trim();
        let display = body["displayName"].as_str().unwrap_or("").trim();
        let group_ids = string_list(&body["groupIds"]);
        let template_ids = string_list(&body["templateIds"]);
        if !hostname.is_empty() {
            // ok
        } else if body.get("hostname").is_some() {
            return Err(AppError::bad("主机名不能为空"));
        }
        if body.get("groupIds").is_some() && group_ids.is_empty() {
            return Err(AppError::bad("主机至少要属于一个主机组"));
        }
        for id in group_ids.iter().chain(template_ids.iter()) {
            require_zbx_id(id)?;
        }

        let mut params = json!({ "hostid": host_id });
        if !hostname.is_empty() {
            params["host"] = json!(hostname);
        }
        if body.get("displayName").is_some() {
            params["name"] = json!(if display.is_empty() { hostname } else { display });
        }
        if let Some(status) = parse_host_status(body.get("status")) {
            params["status"] = json!(status);
        }
        if !group_ids.is_empty() || body.get("groupIds").is_some() {
            params["groups"] = json!(group_ids
                .iter()
                .map(|id| json!({ "groupid": id }))
                .collect::<Vec<_>>());
        }
        if body.get("templateIds").is_some() {
            params["templates"] = json!(template_ids
                .iter()
                .map(|id| json!({ "templateid": id }))
                .collect::<Vec<_>>());
        }
        if body.get("tags").is_some() {
            params["tags"] = json!(build_host_tags(body).unwrap_or_default());
        }
        if let Some(mode) = body.get("inventoryMode").or_else(|| body.get("inventory_mode")) {
            if let Some(n) = mode.as_i64().or_else(|| mode.as_str().and_then(|s| s.parse().ok())) {
                params["inventory_mode"] = json!(n);
            }
        }
        if let Some(inv) = body.get("inventory").filter(|v| v.is_object()) {
            params["inventory"] = inv.clone();
        }
        if let Some(desc) = body["description"].as_str() {
            params["description"] = json!(desc);
        }

        let proxy_id = body["proxyId"].as_str().unwrap_or("").trim();
        let proxy_group_id = body["proxyGroupId"].as_str().unwrap_or("").trim();
        let channel = body["channel"].as_str().unwrap_or("").trim();
        if !proxy_id.is_empty() && !proxy_group_id.is_empty() {
            return Err(AppError::bad("不能同时指定 Proxy 与 Proxy Group"));
        }
        if !proxy_id.is_empty() {
            require_zbx_id(proxy_id)?;
        }
        if !proxy_group_id.is_empty() {
            require_zbx_id(proxy_group_id)?;
        }

        // 通道变更在 massupdate / assign 里做，避免 host.update 与版本字段冲突
        let apply_proxy = channel == "proxy" || !proxy_id.is_empty();
        let apply_proxy_group = channel == "proxyGroup" || !proxy_group_id.is_empty();
        let clear_proxy = channel == "direct";

        self.call("host.update", params).await?;

        if body.get("interfaces").is_some() {
            let interfaces = build_host_interfaces(body)?;
            self.sync_host_interfaces(host_id, &interfaces, body["interfaces"].as_array())
                .await?;
        }

        if clear_proxy {
            self.clear_host_proxy(host_id).await?;
        } else if apply_proxy_group && !proxy_group_id.is_empty() {
            self.assign_host_proxy_group(host_id, proxy_group_id).await?;
        } else if apply_proxy && !proxy_id.is_empty() {
            self.assign_host_proxy(host_id, proxy_id).await?;
        }

        if let Some(macros) = body.get("macros").and_then(|v| v.as_array()) {
            self.save_host_macros(host_id, macros).await?;
        }

        self.get_host_detail(host_id).await
    }

    pub async fn delete_hosts(&self, host_ids: &[String]) -> Result<Value, AppError> {
        if host_ids.is_empty() {
            return Err(AppError::bad("请选择主机"));
        }
        for id in host_ids {
            require_zbx_id(id)?;
            self.ensure_host(id).await?;
        }
        self.call("host.delete", json!(host_ids)).await?;
        Ok(json!({ "hostIds": host_ids, "deleted": host_ids.len() }))
    }

    pub async fn mass_update_hosts(&self, body: &Value) -> Result<Value, AppError> {
        let host_ids = string_list(&body["hostIds"]);
        if host_ids.is_empty() {
            return Err(AppError::bad("请选择主机"));
        }
        for id in &host_ids {
            require_zbx_id(id)?;
            self.ensure_host(id).await?;
        }

        let mut params = json!({
            "hosts": host_ids.iter().map(|id| json!({ "hostid": id })).collect::<Vec<_>>(),
        });
        let mut touched = false;

        if let Some(status) = parse_host_status(body.get("status")) {
            params["status"] = json!(status);
            touched = true;
        }
        if body.get("groupIds").is_some() {
            let group_ids = string_list(&body["groupIds"]);
            if group_ids.is_empty() {
                return Err(AppError::bad("主机至少要属于一个主机组"));
            }
            for id in &group_ids {
                require_zbx_id(id)?;
            }
            params["groups"] = json!(group_ids
                .iter()
                .map(|id| json!({ "groupid": id }))
                .collect::<Vec<_>>());
            touched = true;
        }
        if body.get("templateIds").is_some() {
            let template_ids = string_list(&body["templateIds"]);
            for id in &template_ids {
                require_zbx_id(id)?;
            }
            let action = body["templateAction"].as_str().unwrap_or("replace");
            match action {
                "link" => {
                    params["templates"] = json!(template_ids
                        .iter()
                        .map(|id| json!({ "templateid": id }))
                        .collect::<Vec<_>>());
                }
                "unlink" => {
                    params["templates_clear"] = json!(template_ids
                        .iter()
                        .map(|id| json!({ "templateid": id }))
                        .collect::<Vec<_>>());
                }
                _ => {
                    params["templates"] = json!(template_ids
                        .iter()
                        .map(|id| json!({ "templateid": id }))
                        .collect::<Vec<_>>());
                }
            }
            touched = true;
        }
        if body.get("tags").is_some() {
            params["tags"] = json!(build_host_tags(body).unwrap_or_default());
            touched = true;
        }
        if let Some(mode) = body.get("inventoryMode").or_else(|| body.get("inventory_mode")) {
            if let Some(n) = mode.as_i64().or_else(|| mode.as_str().and_then(|s| s.parse().ok())) {
                params["inventory_mode"] = json!(n);
                touched = true;
            }
        }
        if let Some(inv) = body.get("inventory").filter(|v| v.is_object()) {
            params["inventory"] = inv.clone();
            touched = true;
        }

        let proxy_id = body["proxyId"].as_str().unwrap_or("").trim();
        let proxy_group_id = body["proxyGroupId"].as_str().unwrap_or("").trim();
        let channel = body["channel"].as_str().unwrap_or("").trim();
        if !proxy_id.is_empty() && !proxy_group_id.is_empty() {
            return Err(AppError::bad("不能同时指定 Proxy 与 Proxy Group"));
        }
        if channel == "direct" {
            let ver = self.version().await.unwrap_or_default();
            if zabbix_version_lt_7_0(&ver) {
                params["proxy_hostid"] = json!("0");
            } else {
                params["monitored_by"] = json!(0);
                params["proxyid"] = json!("0");
                params["proxy_groupid"] = json!("0");
            }
            touched = true;
        } else if channel == "proxyGroup" || !proxy_group_id.is_empty() {
            if proxy_group_id.is_empty() {
                return Err(AppError::bad("请选择 Proxy Group"));
            }
            require_zbx_id(proxy_group_id)?;
            params["monitored_by"] = json!(2);
            params["proxy_groupid"] = json!(proxy_group_id);
            touched = true;
        } else if channel == "proxy" || !proxy_id.is_empty() {
            if proxy_id.is_empty() {
                return Err(AppError::bad("请选择 Proxy"));
            }
            require_zbx_id(proxy_id)?;
            let ver = self.version().await.unwrap_or_default();
            if zabbix_version_lt_7_0(&ver) {
                params["proxy_hostid"] = json!(proxy_id);
            } else {
                params["monitored_by"] = json!(1);
                params["proxyid"] = json!(proxy_id);
            }
            touched = true;
        }

        if !touched {
            return Err(AppError::bad("请至少勾选一项要更新的内容"));
        }
        self.call("host.massupdate", params).await?;
        Ok(json!({ "hostIds": host_ids, "updated": host_ids.len() }))
    }

    async fn sync_host_interfaces(
        &self,
        host_id: &str,
        built: &[Value],
        raw_list: Option<&Vec<Value>>,
    ) -> Result<(), AppError> {
        let current = self
            .call(
                "hostinterface.get",
                json!({
                    "output": ["interfaceid", "type", "main", "useip", "ip", "dns", "port"],
                    "hostids": [host_id],
                }),
            )
            .await?;
        let current_ids: Vec<String> = current
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|i| i["interfaceid"].as_str().map(|s| s.to_string()))
            .collect();
        let mut keep = std::collections::HashSet::new();
        let raws = raw_list.cloned().unwrap_or_default();
        for (idx, iface) in built.iter().enumerate() {
            let iface_id = raws
                .get(idx)
                .and_then(|r| {
                    r["interfaceId"]
                        .as_str()
                        .or_else(|| r["interfaceid"].as_str())
                })
                .unwrap_or("")
                .trim();
            if !iface_id.is_empty() && current_ids.iter().any(|id| id == iface_id) {
                let mut params = iface.clone();
                params["interfaceid"] = json!(iface_id);
                // hostinterface.update 不接受 hostid
                if let Some(obj) = params.as_object_mut() {
                    obj.remove("hostid");
                }
                self.call("hostinterface.update", params).await?;
                keep.insert(iface_id.to_string());
            } else {
                let mut params = iface.clone();
                params["hostid"] = json!(host_id);
                self.call("hostinterface.create", params).await?;
            }
        }
        let to_delete: Vec<String> = current_ids
            .into_iter()
            .filter(|id| !keep.contains(id))
            .collect();
        if !to_delete.is_empty() {
            // 不能删光；若会删光则跳过删除
            let remaining = keep.len();
            if remaining > 0 {
                self.call("hostinterface.delete", json!(to_delete)).await?;
            }
        }
        Ok(())
    }

    async fn clear_host_proxy(&self, host_id: &str) -> Result<(), AppError> {
        let ver = self.version().await.unwrap_or_default();
        if zabbix_version_lt_7_0(&ver) {
            self.call(
                "host.update",
                json!({ "hostid": host_id, "proxy_hostid": "0" }),
            )
            .await?;
        } else {
            self.call(
                "host.massupdate",
                json!({
                    "hosts": [{ "hostid": host_id }],
                    "monitored_by": 0,
                    "proxyid": "0",
                    "proxy_groupid": "0",
                }),
            )
            .await?;
        }
        Ok(())
    }

    /// 按 Zabbix 版本把主机挂到 Proxy（6.x proxy_hostid；7+ monitored_by+proxyid）。
    async fn merge_proxy_fields(&self, params: &mut Value, proxy_id: &str) -> Result<(), AppError> {
        let ver = self.version().await.unwrap_or_default();
        if zabbix_version_lt_7_0(&ver) {
            params["proxy_hostid"] = json!(proxy_id);
        } else {
            params["monitored_by"] = json!(1);
            params["proxyid"] = json!(proxy_id);
        }
        Ok(())
    }

    fn merge_proxy_group_fields(
        &self,
        params: &mut Value,
        proxy_group_id: &str,
    ) -> Result<(), AppError> {
        params["monitored_by"] = json!(2);
        params["proxy_groupid"] = json!(proxy_group_id);
        Ok(())
    }

    async fn assign_host_proxy(&self, host_id: &str, proxy_id: &str) -> Result<(), AppError> {
        let ver = self.version().await.unwrap_or_default();
        if zabbix_version_lt_7_0(&ver) {
            self.call(
                "host.update",
                json!({ "hostid": host_id, "proxy_hostid": proxy_id }),
            )
            .await?;
        } else {
            // 7.0+：host.update 的旧 proxy_hostid 可能无效，用 massupdate
            self.call(
                "host.massupdate",
                json!({
                    "hosts": [{ "hostid": host_id }],
                    "monitored_by": 1,
                    "proxyid": proxy_id,
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn assign_host_proxy_group(
        &self,
        host_id: &str,
        proxy_group_id: &str,
    ) -> Result<(), AppError> {
        self.call(
            "host.massupdate",
            json!({
                "hosts": [{ "hostid": host_id }],
                "monitored_by": 2,
                "proxy_groupid": proxy_group_id,
            }),
        )
        .await?;
        Ok(())
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

    /// 更新已有主机的 Agent 接口端口（type=1）。
    async fn update_agent_interface_port(
        &self,
        host_id: &str,
        ip: &str,
        port: &str,
    ) -> Result<(), AppError> {
        let result = self
            .call(
                "hostinterface.get",
                json!({
                    "output": ["interfaceid", "ip", "port", "type", "main"],
                    "hostids": [host_id],
                    "filter": { "type": 1 },
                }),
            )
            .await?;
        let Some(list) = result.as_array() else {
            return Ok(());
        };
        let iface = list
            .iter()
            .find(|i| i["main"].as_str() == Some("1") || i["main"].as_i64() == Some(1))
            .or_else(|| list.first());
        let Some(iface) = iface else {
            return Ok(());
        };
        let Some(interface_id) = iface["interfaceid"].as_str() else {
            return Ok(());
        };
        let mut params = json!({
            "interfaceid": interface_id,
            "port": port,
        });
        if !ip.is_empty() {
            params["ip"] = json!(ip);
            params["useip"] = json!(1);
        }
        self.call("hostinterface.update", params).await?;
        Ok(())
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

fn zabbix_version_lt_7_0(version: &str) -> bool {
    let major = version
        .split('.')
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    major < 7
}

fn normalize_listen_port(value: Option<&Value>) -> String {
    let raw = value
        .and_then(|v| {
            v.as_str()
                .map(|s| s.trim().to_string())
                .or_else(|| v.as_u64().map(|n| n.to_string()))
                .or_else(|| v.as_i64().map(|n| n.to_string()))
        })
        .unwrap_or_default();
    if let Ok(n) = raw.parse::<u16>() {
        if n > 0 {
            return n.to_string();
        }
    }
    "10050".to_string()
}

fn iface_type_num(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_u64().map(|n| n as i64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn iface_bool01(v: Option<&Value>, default: i64) -> i64 {
    match v {
        Some(Value::Bool(true)) => 1,
        Some(Value::Bool(false)) => 0,
        Some(Value::Number(n)) => {
            if n.as_i64().unwrap_or(0) != 0 {
                1
            } else {
                0
            }
        }
        Some(Value::String(s)) => {
            if matches!(s.as_str(), "1" | "true" | "yes") {
                1
            } else {
                0
            }
        }
        _ => default,
    }
}

/// 从请求组装 interfaces；兼容旧字段 ip/port（单 Agent 接口）。
fn build_host_interfaces(body: &Value) -> Result<Vec<Value>, AppError> {
    if let Some(list) = body["interfaces"].as_array() {
        if list.is_empty() {
            return Err(AppError::bad("请至少配置一个接口"));
        }
        let mut out = Vec::new();
        let mut main_count = 0i32;
        for (idx, raw) in list.iter().enumerate() {
            let ty = iface_type_num(&raw["type"]).unwrap_or(1);
            if !(1..=4).contains(&ty) {
                return Err(AppError::bad(&format!("接口 #{} 类型无效", idx + 1)));
            }
            let main = iface_bool01(raw.get("main"), if idx == 0 { 1 } else { 0 });
            if main == 1 {
                main_count += 1;
            }
            let useip = iface_bool01(raw.get("useip"), 1);
            let ip = raw["ip"].as_str().unwrap_or("").trim();
            let dns = raw["dns"].as_str().unwrap_or("").trim();
            if useip == 1 && ip.is_empty() {
                return Err(AppError::bad(&format!("接口 #{} 请填写 IP", idx + 1)));
            }
            if useip == 0 && dns.is_empty() {
                return Err(AppError::bad(&format!("接口 #{} 请填写 DNS", idx + 1)));
            }
            let default_port = match ty {
                2 => "161",
                3 => "623",
                4 => "12345",
                _ => "10050",
            };
            let port_raw = raw.get("port");
            let port_empty = port_raw.is_none()
                || port_raw
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(false)
                    && port_raw.and_then(|v| v.as_u64()).is_none()
                    && port_raw.and_then(|v| v.as_i64()).is_none();
            let port = if port_empty {
                default_port.to_string()
            } else {
                normalize_listen_port(port_raw)
            };
            let mut iface = json!({
                "type": ty,
                "main": main,
                "useip": useip,
                "ip": ip,
                "dns": dns,
                "port": port,
            });
            if ty == 2 {
                let details = build_snmp_details(raw)?;
                iface["details"] = details;
            }
            out.push(iface);
        }
        if main_count == 0 {
            if let Some(first) = out.first_mut() {
                first["main"] = json!(1);
            }
        } else if main_count > 1 {
            // 只保留第一个 main=1
            let mut seen = false;
            for iface in &mut out {
                if iface["main"].as_i64() == Some(1) {
                    if seen {
                        iface["main"] = json!(0);
                    } else {
                        seen = true;
                    }
                }
            }
        }
        return Ok(out);
    }

    let ip = body["ip"].as_str().unwrap_or("").trim();
    if ip.is_empty() {
        return Err(AppError::bad("请配置接口，或填写管理 IP"));
    }
    let port = normalize_listen_port(body.get("port").or_else(|| body.get("agentPort")));
    Ok(vec![json!({
        "type": 1,
        "main": 1,
        "useip": 1,
        "ip": ip,
        "dns": "",
        "port": port,
    })])
}

fn build_snmp_details(raw: &Value) -> Result<Value, AppError> {
    let src = raw.get("details").cloned().unwrap_or(json!({}));
    let version = iface_type_num(&src["version"])
        .or_else(|| iface_type_num(&raw["snmpVersion"]))
        .unwrap_or(2);
    if !(1..=3).contains(&version) {
        return Err(AppError::bad("SNMP 版本无效"));
    }
    let mut details = json!({
        "version": version,
        "bulk": iface_bool01(src.get("bulk"), 1),
    });
    if version == 3 {
        details["contextname"] = json!(src["contextname"]
            .as_str()
            .or_else(|| src["contextName"].as_str())
            .unwrap_or(""));
        details["securityname"] = json!(src["securityname"]
            .as_str()
            .or_else(|| src["securityName"].as_str())
            .unwrap_or(""));
        details["securitylevel"] = json!(iface_type_num(&src["securitylevel"])
            .or_else(|| iface_type_num(&src["securityLevel"]))
            .unwrap_or(0));
        details["authprotocol"] = json!(iface_type_num(&src["authprotocol"])
            .or_else(|| iface_type_num(&src["authProtocol"]))
            .unwrap_or(0));
        details["authpassphrase"] = json!(src["authpassphrase"]
            .as_str()
            .or_else(|| src["authPassphrase"].as_str())
            .unwrap_or(""));
        details["privprotocol"] = json!(iface_type_num(&src["privprotocol"])
            .or_else(|| iface_type_num(&src["privProtocol"]))
            .unwrap_or(0));
        details["privpassphrase"] = json!(src["privpassphrase"]
            .as_str()
            .or_else(|| src["privPassphrase"].as_str())
            .unwrap_or(""));
    } else {
        let community = src["community"]
            .as_str()
            .or_else(|| raw["community"].as_str())
            .unwrap_or("{$SNMP_COMMUNITY}");
        if community.trim().is_empty() {
            return Err(AppError::bad("SNMP community 不能为空"));
        }
        details["community"] = json!(community);
    }
    if let Some(n) = iface_type_num(&src["max_repetitions"])
        .or_else(|| iface_type_num(&src["maxRepetitions"]))
    {
        details["max_repetitions"] = json!(n);
    }
    Ok(details)
}

fn parse_host_status(value: Option<&Value>) -> Option<i64> {
    let v = value?;
    let n = v
        .as_i64()
        .or_else(|| v.as_u64().map(|n| n as i64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))?;
    if n == 0 || n == 1 {
        Some(n)
    } else {
        None
    }
}

fn map_host_interface(iface: &Value) -> Value {
    let ty = iface_type_num(&iface["type"]).unwrap_or(1);
    let details = iface.get("details").cloned().unwrap_or(json!({}));
    let snmp_version = iface_type_num(&details["version"]).unwrap_or(2);
    json!({
        "interfaceId": iface["interfaceid"].as_str().unwrap_or(""),
        "type": ty,
        "main": iface_bool01(iface.get("main"), 0) == 1,
        "useip": iface_bool01(iface.get("useip"), 1),
        "ip": iface["ip"].as_str().unwrap_or(""),
        "dns": iface["dns"].as_str().unwrap_or(""),
        "port": iface["port"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| iface["port"].as_u64().map(|n| n.to_string()))
            .unwrap_or_default(),
        "snmpVersion": snmp_version,
        "community": details["community"].as_str().unwrap_or("{$SNMP_COMMUNITY}"),
        "contextName": details["contextname"].as_str().unwrap_or(""),
        "securityName": details["securityname"].as_str().unwrap_or(""),
        "securityLevel": iface_type_num(&details["securitylevel"]).unwrap_or(0),
        "authPassphrase": details["authpassphrase"].as_str().unwrap_or(""),
        "privPassphrase": details["privpassphrase"].as_str().unwrap_or(""),
        "details": details,
    })
}

fn build_host_macros(body: &Value) -> Option<Vec<Value>> {
    let list = body["macros"].as_array()?;
    let mut out = Vec::new();
    for m in list {
        let macro_name = m["macro"].as_str().unwrap_or("").trim();
        if macro_name.is_empty() {
            continue;
        }
        let mut item = json!({
            "macro": macro_name,
            "value": m["value"].as_str().unwrap_or(""),
        });
        if let Some(t) = iface_type_num(&m["type"]) {
            item["type"] = json!(t);
        }
        if let Some(d) = m["description"].as_str() {
            item["description"] = json!(d);
        }
        out.push(item);
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn build_host_tags(body: &Value) -> Option<Vec<Value>> {
    let list = body["tags"].as_array()?;
    let mut out = Vec::new();
    for t in list {
        let tag = t["tag"].as_str().unwrap_or("").trim();
        if tag.is_empty() {
            continue;
        }
        out.push(json!({
            "tag": tag,
            "value": t["value"].as_str().unwrap_or(""),
        }));
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
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
    let port = primary_port(host);
    let version = last_value(items, host_id, &["agent.version"]);
    let ping_item = pick_item(items, host_id, &["agent.ping"]);
    // 可用性：按接口类型给出 Zabbix 同款状态码（0 未知 / 1 可用 / 2 不可用；无该类型则为 null）
    let availability = host_availability_by_type(host);
    let interface_available = primary_interface_available(host);
    // Agent 通断：ping 项 state/lastvalue + ZBX 接口；接口已红时不因 lastvalue=1 仍显示通
    let agent_available =
        resolve_agent_available(ping_item, agent_interface_available(host));
    let has_agent_iface = agent_interface_available(host).is_some()
        || host["interfaces"]
            .as_array()
            .is_some_and(|list| list.iter().any(|i| iface_type_is(i, 1)));
    let agent_status = agent_status_code(has_agent_iface, agent_available);
    let status = host["status"]
        .as_str()
        .map(|s| s.to_string())
        .or_else(|| host["status"].as_i64().map(|n| n.to_string()))
        .unwrap_or_else(|| "0".to_string());
    // 有 monitored_by 按 7.x 解析；否则按 6.x proxy_hostid
    let ver_lt_7 = !host
        .as_object()
        .map(|o| o.contains_key("monitored_by"))
        .unwrap_or(false);
    let (channel, proxy_id, proxy_group_id) = host_monitoring_channel(host, ver_lt_7);
    json!({
        "hostId": host_id,
        "hostname": host["host"].as_str().unwrap_or(""),
        "displayName": host["name"].as_str().unwrap_or(""),
        "status": status,
        "ip": ip,
        "port": port,
        "agentAvailable": agent_available,
        "agentStatus": agent_status,
        "interfaceAvailable": interface_available,
        "availability": availability,
        "agentVersion": version,
        "channel": channel,
        "proxyId": proxy_id,
        "proxyGroupId": proxy_group_id,
        "groups": map_id_names(&host["hostgroups"], "groupid", "groupId"),
    })
}

/// 解析 Zabbix 主机监控通道 → (channel, proxy_id, proxy_group_id)。
/// `ver_lt_7=true` 用 proxy_hostid；否则用 monitored_by / proxyid / proxy_groupid。
fn host_monitoring_channel(host: &Value, ver_lt_7: bool) -> (String, String, String) {
    let mut channel = "direct".to_string();
    let mut proxy_id = String::new();
    let mut proxy_group_id = String::new();
    if !ver_lt_7 {
        let monitored_by = host["monitored_by"]
            .as_i64()
            .or_else(|| host["monitored_by"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0);
        match monitored_by {
            1 => {
                channel = "proxy".into();
                proxy_id = host["proxyid"].as_str().unwrap_or("").to_string();
            }
            2 => {
                channel = "proxyGroup".into();
                proxy_group_id = host["proxy_groupid"].as_str().unwrap_or("").to_string();
            }
            _ => {}
        }
    } else {
        let ph = host["proxy_hostid"].as_str().unwrap_or("0");
        if ph != "0" && !ph.is_empty() {
            channel = "proxy".into();
            proxy_id = ph.to_string();
        }
    }
    if proxy_id == "0" {
        proxy_id.clear();
    }
    if proxy_group_id == "0" {
        proxy_group_id.clear();
    }
    (channel, proxy_id, proxy_group_id)
}

fn primary_agent_iface<'a>(host: &'a Value) -> Option<&'a Value> {
    let list = host["interfaces"].as_array()?;
    list.iter()
        .find(|i| i["type"].as_str() == Some("1") || i["type"].as_i64() == Some(1))
        .or_else(|| {
            list.iter()
                .find(|i| i["main"].as_str() == Some("1") || i["main"].as_i64() == Some(1))
        })
        .or_else(|| list.first())
}

fn primary_ip(host: &Value) -> String {
    let Some(chosen) = primary_agent_iface(host) else {
        return String::new();
    };
    chosen
        .get("ip")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| chosen.get("dns").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string()
}

fn primary_port(host: &Value) -> String {
    primary_agent_iface(host)
        .and_then(|i| {
            i.get("port")
                .and_then(|v| v.as_str().map(|s| s.to_string()).or_else(|| {
                    v.as_u64()
                        .map(|n| n.to_string())
                        .or_else(|| v.as_i64().map(|n| n.to_string()))
                }))
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_default()
}

fn iface_available_code(iface: &Value) -> i64 {
    let Some(raw) = iface.get("available") else {
        return 0;
    };
    let code = raw
        .as_i64()
        .or_else(|| raw.as_u64().map(|n| n as i64))
        .or_else(|| raw.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0);
    match code {
        1 | 2 => code,
        _ => 0,
    }
}

fn iface_available_flag(iface: &Value) -> Option<bool> {
    match iface_available_code(iface) {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    }
}

fn iface_ts(iface: &Value, key: &str) -> i64 {
    iface
        .get(key)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_u64().map(|n| n as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

fn iface_availability_detail(iface: &Value) -> Value {
    json!({
        "status": iface_available_code(iface),
        "error": iface["error"].as_str().unwrap_or(""),
        "errorsFrom": iface_ts(iface, "errors_from"),
        "disableUntil": iface_ts(iface, "disable_until"),
    })
}

/// 与 Zabbix 前端一致：按类型取主接口（无主则取该类型第一张）的 available + error。
fn host_availability_by_type(host: &Value) -> Value {
    let empty = json!({
        "zbx": Value::Null,
        "snmp": Value::Null,
        "ipmi": Value::Null,
        "jmx": Value::Null,
    });
    let Some(list) = host["interfaces"].as_array() else {
        return empty;
    };
    let pick = |ty: i64| -> Value {
        let iface = list
            .iter()
            .filter(|i| iface_type_is(i, ty))
            .find(|i| iface_is_main(i))
            .or_else(|| list.iter().find(|i| iface_type_is(i, ty)));
        match iface {
            Some(i) => iface_availability_detail(i),
            None => Value::Null,
        }
    };
    json!({
        "zbx": pick(1),
        "snmp": pick(2),
        "ipmi": pick(3),
        "jmx": pick(4),
    })
}

fn iface_is_main(iface: &Value) -> bool {
    iface["main"].as_str() == Some("1") || iface["main"].as_i64() == Some(1)
}

fn iface_type_is(iface: &Value, ty: i64) -> bool {
    iface["type"].as_i64() == Some(ty)
        || iface["type"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            == Some(ty)
}

/// Zabbix 主机「可用性」：优先主接口，否则第一张接口（含 SNMP/IPMI/JMX）。
fn primary_interface_available(host: &Value) -> Option<bool> {
    let list = host["interfaces"].as_array()?;
    let iface = list
        .iter()
        .find(|i| iface_is_main(i))
        .or_else(|| list.first())?;
    iface_available_flag(iface)
}

fn agent_interface_available(host: &Value) -> Option<bool> {
    let list = host["interfaces"].as_array()?;
    let agent = list.iter().find(|i| iface_type_is(i, 1))?;
    iface_available_flag(agent)
}

/// 名册已映射主机上的 ZBX 接口通断（status 1/2）。
fn mapped_zbx_iface_available(host: &Value) -> Option<bool> {
    let node = &host["availability"]["zbx"];
    let code = node["status"]
        .as_i64()
        .or_else(|| node.as_i64())
        .or_else(|| node.as_u64().map(|n| n as i64))?;
    match code {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    }
}

fn mapped_has_agent_iface(host: &Value) -> bool {
    !host["availability"]["zbx"].is_null()
}

/// 代理生命周期状态（一期）：
/// - absent：无 Zabbix Agent 接口
/// - running：有接口且通
/// - stopped：有接口但不通（含停服与链路不可达，二期再拆）
/// - unknown：有接口但通断未知
fn agent_status_code(has_agent_iface: bool, available: Option<bool>) -> &'static str {
    if !has_agent_iface {
        return "absent";
    }
    match available {
        Some(true) => "running",
        Some(false) => "stopped",
        None => "unknown",
    }
}

fn item_state_code(item: &Value) -> i64 {
    item.get("state")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_u64().map(|n| n as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

/// Agent 通断判定：
/// 1) ZBX Agent 接口不可用 → 断（与可用性列一致，避免 lastvalue 残留）
/// 2) agent.ping 监控项 state=1（不支持）→ 断
/// 3) 否则看 lastvalue 1/0；都没有则回退接口状态
fn resolve_agent_available(
    ping_item: Option<&Value>,
    zbx_iface_available: Option<bool>,
) -> Option<bool> {
    if zbx_iface_available == Some(false) {
        return Some(false);
    }
    if let Some(item) = ping_item {
        if item_state_code(item) == 1 {
            return Some(false);
        }
        match item["lastvalue"].as_str() {
            Some("1") => return Some(true),
            Some("0") => return Some(false),
            _ => {}
        }
    }
    zbx_iface_available
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
