//! # karma-server — the Karma SQL engine as an HTTP service.
//!
//! `POST /sql` binds the user's SQL against a request-scoped catalog (the referenced
//! tables → Iceberg snapshots resolved via the ml-runner), lowers it to the Substrait
//! contract, and executes it over the Iceberg warehouse on R2 with Karma pruning
//! (`karma_sql::KarmaSession`). This is what Carbon's SQL Editor seam (`KARMA_SQL_URL`)
//! calls — the native replacement for the Postgres `dataset_rows` shim.
//!
//! Config (env): `ML_RUNNER_URL` + `ML_RUNNER_TOKEN` (the catalog binding) and
//! `LAKEHOUSE_S3_*` (R2, read by [`S3Config::from_env`]). `KARMA_BIND` (default
//! `0.0.0.0:8088`).

mod resolver;

use std::collections::HashMap;
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use datafusion::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use karma_parquet::{s3_store, S3Config};
use karma_sql::KarmaSession;

use resolver::{MlRunnerClient, MlRunnerResolver, TableRef};

#[derive(Clone)]
struct AppState {
    client: MlRunnerClient,
    store: Arc<dyn ObjectStore>,
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default)]
    tables: HashMap<String, TableRef>,
    #[serde(default)]
    #[allow(dead_code)]
    limit: Option<usize>,
    #[serde(default)]
    #[allow(dead_code)]
    user: Option<String>,
}

#[derive(Serialize)]
struct ColumnDto {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Serialize)]
struct SqlResponse {
    columns: Vec<ColumnDto>,
    rows: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[tokio::main]
async fn main() {
    // Bind priority: KARMA_BIND (explicit) > PORT (Railway/Render inject it) > default.
    let bind = std::env::var("KARMA_BIND")
        .ok()
        .or_else(|| std::env::var("PORT").ok().map(|p| format!("0.0.0.0:{p}")))
        .unwrap_or_else(|| "0.0.0.0:8088".to_string());
    let ml_url = std::env::var("ML_RUNNER_URL").expect("ML_RUNNER_URL is required");
    let ml_token = std::env::var("ML_RUNNER_TOKEN").expect("ML_RUNNER_TOKEN is required");
    let store = s3_store(&S3Config::from_env().expect("LAKEHOUSE_S3_* env is required"))
        .expect("failed to build the R2 object store");

    let state = AppState { client: MlRunnerClient::new(ml_url, ml_token), store };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/sql", post(post_sql))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind).await.expect("failed to bind");
    eprintln!("karma-server listening on {bind}");
    axum::serve(listener, app).await.expect("server error");
}

/// Execute one SQL query. `400` on a bind/execution error (bad SQL, unknown table),
/// `500` on a serialization failure.
async fn post_sql(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, (StatusCode, Json<ErrorResponse>)> {
    let tables: HashMap<String, TableRef> =
        req.tables.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect();
    let resolver = Arc::new(MlRunnerResolver::new(state.client.clone(), state.store.clone(), tables));
    let session = KarmaSession::new(resolver);

    let result = session
        .run(&req.sql)
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;

    // Parity with the Postgres path: the reserved identity columns are stamped on
    // every Iceberg row but hidden from the user (the PG CTE never projects them).
    const RESERVED: [&str; 3] = ["__row_index", "__row_id", "__created_at"];
    let columns: Vec<ColumnDto> = result
        .columns
        .iter()
        .filter(|c| !RESERVED.contains(&c.name.as_str()))
        .map(|c| ColumnDto { name: c.name.clone(), ty: c.data_type.clone() })
        .collect();
    let mut rows = batches_to_json(&result.batches).map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    for row in &mut rows {
        if let serde_json::Value::Object(map) = row {
            for k in RESERVED {
                map.remove(k);
            }
        }
    }

    Ok(Json(SqlResponse { columns, rows }))
}

fn err(code: StatusCode, message: String) -> (StatusCode, Json<ErrorResponse>) {
    (code, Json(ErrorResponse { error: message }))
}

/// Serialize Arrow result batches to JSON row objects (column → value), the wire shape
/// the SQL Editor renders. Uses the arrow JSON writer bundled with datafusion (same
/// arrow instance as the batches).
fn batches_to_json(batches: &[RecordBatch]) -> Result<Vec<serde_json::Value>, String> {
    let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    {
        let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
        for b in &non_empty {
            writer.write(b).map_err(|e| e.to_string())?;
        }
        writer.finish().map_err(|e| e.to_string())?;
    }
    match serde_json::from_slice(&buf).map_err(|e| e.to_string())? {
        serde_json::Value::Array(rows) => Ok(rows),
        _ => Ok(Vec::new()),
    }
}
