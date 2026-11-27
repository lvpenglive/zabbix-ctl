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
    /// Gateway 调用本服务时使用的 Bearer。环境变量 ZABBIX_CTL_SERVICE_TOKEN 可覆盖。
    #[serde(default)]
    pub service_token: String,
}

fn default_bind() -> String {
    "127.0.0.1:8090".to_string()
}

impl ServerConfig {
    pub fn resolved_service_token(&self) -> String {
        env::var("ZABBIX_CTL_SERVICE_TOKEN")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.service_token.clone())
    }
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

/// 未配置 `default_install_path` 或配置为空时使用的 Zabbix Agent 安装目录。
pub const DEFAULT_INSTALL_PATH: &str = "/opt/ITM/IIM/zabbix_agentd";

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MeridianOpsConfig {
    pub base_url: String,
    /// 调 Gateway 作业接口的 API Token（mk- 前缀）。环境变量 MERIDIANOPS_JOB_TOKEN 可覆盖。
    pub job_token: String,
    /// Zabbix Agent 默认安装目录。主机对照或安装任务未指定 install_path 时使用；
    /// 留空则回退内置默认值 DEFAULT_INSTALL_PATH。
    #[serde(default)]
    pub default_install_path: String,
    pub job_start: i64,
    pub job_stop: i64,
    pub job_restart: i64,
    pub job_upgrade: i64,
    pub job_rollback: i64,
    #[serde(default)]
    pub job_install: i64,
    /// 为 0 时按名称从 job_definitions 解析
    pub job_start_name: String,
    pub job_stop_name: String,
    pub job_restart_name: String,
    pub job_upgrade_name: String,
    pub job_rollback_name: String,
    #[serde(default = "default_install_job_name")]
    pub job_install_name: String,
}

fn default_install_job_name() -> String {
    "Zabbix Agent 安装".to_string()
}

impl Default for MeridianOpsConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:8800".to_string(),
            job_token: String::new(),
            default_install_path: String::new(),
            job_start: 0,
            job_stop: 0,
            job_restart: 0,
            job_upgrade: 0,
            job_rollback: 0,
            job_install: 0,
            job_start_name: "Zabbix Agent 启动".to_string(),
            job_stop_name: "Zabbix Agent 停止".to_string(),
            job_restart_name: "Zabbix Agent 重启".to_string(),
            job_upgrade_name: "Zabbix Agent 升级".to_string(),
            job_rollback_name: "Zabbix Agent 回滚".to_string(),
            job_install_name: default_install_job_name(),
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
            "install" => self.job_install,
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
            "install" => self.job_install_name.as_str(),
            _ => "",
        };
        if name.is_empty() {
            None
        } else {
            Some(name)
        }
    }

    /// 全局默认安装目录；未配置或为空时回退内置默认值。
    pub fn resolved_default_install_path(&self) -> &str {
        if self.default_install_path.is_empty() {
            DEFAULT_INSTALL_PATH
        } else {
            self.default_install_path.as_str()
        }
    }

    pub fn resolved_job_token(&self) -> Option<String> {
        env::var("MERIDIANOPS_JOB_TOKEN")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                if self.job_token.is_empty() {
                    None
                } else {
                    Some(self.job_token.clone())
                }
            })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ZabbixInstanceConfig {
    pub code: String,
    pub name: String,
    pub api_url: String,
    /// 浏览器可打开的 Zabbix 前端根地址（含子路径，如 `https://zabbix.example/zabbix`）。
    /// 用于门户「在 Zabbix 打开」深链；勿填 API 地址（:10051 / api_jsonrpc.php）。
    #[serde(default)]
    pub frontend_url: String,
    /// Zabbix API 令牌。环境变量 ZBX_TOKEN_{CODE} 可覆盖。
    #[serde(default)]
    pub api_token: String,
    /// 改主机、模板关联、宏、确认问题用的写令牌。环境变量 ZBX_TOKEN_{CODE}_WRITE 可覆盖。
    /// 为空时回退到读令牌，便于现有单令牌环境先跑起来。
    #[serde(default)]
    pub api_token_write: String,
    /// 鉴权方式：`auto`（默认，按版本选择）、`header`（Bearer）、`body`（JSON auth，适合 7.0 及反代剥 Header）。
    #[serde(default = "default_api_auth")]
    pub api_auth: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 生产实例。为 true 时，模板设计器写入必须带已审批标记。
    #[serde(default)]
    pub protected: bool,
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

fn default_api_auth() -> String {
    "auto".to_string()
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

    pub fn resolved_api_token(&self) -> Option<String> {
        env::var(self.token_env_key())
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                if self.api_token.is_empty() {
                    None
                } else {
                    Some(self.api_token.clone())
                }
            })
    }

    pub fn write_token_env_key(&self) -> String {
        format!("ZBX_TOKEN_{}_WRITE", self.code.to_uppercase())
    }

    /// 写操作优先用写令牌，未配置时回退读令牌。
    pub fn resolved_write_token(&self) -> Option<String> {
        env::var(self.write_token_env_key())
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                if self.api_token_write.is_empty() {
                    None
                } else {
                    Some(self.api_token_write.clone())
                }
            })
            .or_else(|| self.resolved_api_token())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_install_path_falls_back_to_builtin() {
        let cfg: AppConfig = toml::from_str("[server]").unwrap();
        assert_eq!(
            cfg.meridianops.resolved_default_install_path(),
            DEFAULT_INSTALL_PATH
        );

        let cfg: AppConfig =
            toml::from_str("[server]\n[meridianops]\ndefault_install_path = \"\"").unwrap();
        assert_eq!(
            cfg.meridianops.resolved_default_install_path(),
            DEFAULT_INSTALL_PATH
        );

        let cfg: AppConfig = toml::from_str(
            "[server]\n[meridianops]\ndefault_install_path = \"/opt/custom/zabbix\"",
        )
        .unwrap();
        assert_eq!(
            cfg.meridianops.resolved_default_install_path(),
            "/opt/custom/zabbix"
        );
    }
}

