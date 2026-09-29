use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::DbPool;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub http: reqwest::Client,
    pub service_token: String,
    pub db: Option<DbPool>,
}
