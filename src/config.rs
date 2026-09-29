use std::env;
use std::fs;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub meridianops: MeridianOpsConfig,
    #[serde(default)]
    pub zabbix: Vec<ZabbixInstanceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_bind() -> String {
    "127.0.0.1:8090".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "mysql://root:password@127.0.0.1:3306/meridianops".to_string(),
            max_connections: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MeridianOpsConfig {
    pub base_url: String,
    pub job_start: i64,
    pub job_stop: i64,
    pub job_restart: i64,
    pub job_upgrade: i64,
    pub job_rollback: i64,
    /// 为 0 时按名称从 job_definitions 解析
    pub job_start_name: String,
    pub job_stop_name: String,
    pub job_restart_name: String,
    pub job_upgrade_name: String,
    pub job_rollback_name: String,
}

impl Default for MeridianOpsConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:8000".to_string(),
            job_start: 0,
            job_stop: 0,
            job_restart: 0,
            job_upgrade: 0,
            job_rollback: 0,
            job_start_name: "Zabbix Agent 启动".to_string(),
            job_stop_name: "Zabbix Agent 停止".to_string(),
            job_restart_name: "Zabbix Agent 重启".to_string(),
            job_upgrade_name: "Zabbix Agent 升级".to_string(),
            job_rollback_name: "Zabbix Agent 回滚".to_string(),
        }
    }
}

impl MeridianOpsConfig {
    pub fn job_id_for(&self, action: &str) -> Option<i64> {
        let id = match action {
            "start" => self.job_start,
            "stop" => self.job_stop,
            "restart" => self.job_restart,
            "upgrade" => self.job_upgrade,
            "rollback" => self.job_rollback,
            _ => 0,
        };
        if id > 0 {
            Some(id)
        } else {
            None
        }
    }

    pub fn job_name_for(&self, action: &str) -> Option<&str> {
        let name = match action {
            "start" => self.job_start_name.as_str(),
            "stop" => self.job_stop_name.as_str(),
            "restart" => self.job_restart_name.as_str(),
            "upgrade" => self.job_upgrade_name.as_str(),
            "rollback" => self.job_rollback_name.as_str(),
            _ => "",
        };
        if name.is_empty() {
            None
        } else {
            Some(name)
        }
    }

    pub fn job_token_from_env() -> Option<String> {
        env::var("MERIDIANOPS_JOB_TOKEN")
            .ok()
            .filter(|v| !v.is_empty())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ZabbixInstanceConfig {
    pub code: String,
    pub name: String,
    pub api_url: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 标准模板名称列表。主机缺少其中任一则记为偏离。为空则只列出模板、不判偏离。
    #[serde(default)]
    pub standard_templates: Vec<String>,
    /// Proxy 超过多少秒未上报视为离线，默认 600。
    #[serde(default = "default_proxy_stale_secs")]
    pub proxy_stale_secs: i64,
    /// 采集队列条数超过此值视为积压，默认 1000。
    #[serde(default = "default_queue_warn")]
    pub queue_warn: u32,
}

fn default_true() -> bool {
    true
}

fn default_proxy_stale_secs() -> i64 {
    600
}

fn default_queue_warn() -> u32 {
    1000
}

impl ZabbixInstanceConfig {
    pub fn token_env_key(&self) -> String {
        format!("ZBX_TOKEN_{}", self.code.to_uppercase())
    }

    pub fn token_from_env(&self) -> Option<String> {
        env::var(self.token_env_key()).ok().filter(|v| !v.is_empty())
    }
}

pub fn load(path: &Path) -> anyhow::Result<AppConfig> {
    let raw = fs::read_to_string(path)?;
    let mut cfg: AppConfig = toml::from_str(&raw)?;
    if let Ok(url) = env::var("MERIDIANOPS_DB_URL") {
        if !url.is_empty() {
            cfg.database.url = url;
        }
    }
    Ok(cfg)
}
