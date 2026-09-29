use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::error::AppError;
use crate::state::AppState;

/// Gateway 调用本服务时携带的服务令牌。来自环境变量 `ZABBIX_CTL_SERVICE_TOKEN`。
pub struct ServiceAuth;

#[async_trait]
impl FromRequestParts<AppState> for ServiceAuth {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let token = header.strip_prefix("Bearer ").unwrap_or("").trim();
        if state.service_token.is_empty() || token != state.service_token {
            return Err(AppError::Unauthorized);
        }
        Ok(ServiceAuth)
    }
}
