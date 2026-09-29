//! 调用 MeridianOps Gateway 作业执行接口。

use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

use crate::config::MeridianOpsConfig;
use crate::error::AppError;

#[derive(Clone)]
pub struct JobClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl JobClient {
    pub fn from_config(http: reqwest::Client, cfg: &MeridianOpsConfig) -> Result<Self, AppError> {
        let token = MeridianOpsConfig::job_token_from_env()
            .ok_or_else(|| AppError::bad("未配置环境变量 MERIDIANOPS_JOB_TOKEN"))?;
        Ok(Self {
            http,
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    pub async fn execute(&self, job_id: i64, asset_id: &str) -> Result<i64, AppError> {
        let url = format!("{}/api/jobs/{}/execute", self.base_url, job_id);
        let response = self
            .http
            .post(url)
            .header("Authorization", format!("Bearer {}", self.token))
            .json(&json!({ "assetIds": [asset_id] }))
            .send()
            .await
            .map_err(|e| AppError::internal(format!("调用作业执行失败: {e}")))?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|e| AppError::internal(format!("解析作业响应失败: {e}")))?;
        if !status.is_success() || body["code"].as_i64().unwrap_or(-1) != 0 {
            let msg = body["message"].as_str().unwrap_or("作业执行被拒绝");
            return Err(AppError::bad(msg));
        }
        body["data"]["jobRunId"]
            .as_i64()
            .ok_or_else(|| AppError::internal("作业未返回 jobRunId"))
    }

    /// 轮询直到完成或超时。返回 overall_status。
    pub async fn wait_run(&self, run_id: i64, timeout_secs: u64) -> Result<String, AppError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            let status = self.get_run_status(run_id).await?;
            if matches!(status.as_str(), "success" | "failed" | "partial") {
                return Ok(status);
            }
            if std::time::Instant::now() >= deadline {
                return Err(AppError::internal(format!("等待作业 {run_id} 超时")));
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn get_run_status(&self, run_id: i64) -> Result<String, AppError> {
        let url = format!("{}/api/jobs/runs/{}", self.base_url, run_id);
        let response = self
            .http
            .get(url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .map_err(|e| AppError::internal(format!("查询作业状态失败: {e}")))?;
        let body: Value = response
            .json()
            .await
            .map_err(|e| AppError::internal(format!("解析作业状态失败: {e}")))?;
        if body["code"].as_i64().unwrap_or(-1) != 0 {
            let msg = body["message"].as_str().unwrap_or("查询作业失败");
            return Err(AppError::bad(msg));
        }
        Ok(body["data"]["overallStatus"]
            .as_str()
            .unwrap_or("running")
            .to_string())
    }
}
