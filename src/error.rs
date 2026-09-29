use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("未授权")]
    Unauthorized,
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Internal(String),
}

impl AppError {
    pub fn bad(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }

    pub fn internal(err: impl std::fmt::Display) -> Self {
        Self::Internal(err.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, 401),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, 400),
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, 404),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, 500),
        };
        let body = Json(json!({
            "code": code,
            "message": self.to_string(),
        }));
        (status, body).into_response()
    }
}
