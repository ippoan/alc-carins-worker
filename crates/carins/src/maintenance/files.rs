//! 整備記録の添付 (rust の `alc-maintenance` の files.rs の写し。型は ippoan/alc-trouble-worker の files.rs)。
//!
//! - `POST /maintenance/records/{record_id}/files` — multipart の最初の 1 つを R2 (`MAINTENANCE_R2`) に置き、行を作る (201)
//! - `GET /maintenance/records/{record_id}/files` — 論理削除していないものを作成の順
//! - `GET /maintenance/files/{file_id}/download` — 本体と `Content-Type` (保存した値)・`Content-Disposition`
//!   (ASCII だけ。**rust と違う**、[`crate::store::content_disposition`])。論理削除したものは 404 (rust と同じ)
//! - `DELETE /maintenance/files/{file_id}` — 論理削除 (204)。R2 の実体は消さない
//!
//! 記録がこの会社に無ければ (無い・別の会社) 404、R2 の binding が無ければ 503 (rust と同じ順)。
//! アップロードは先に `tenant_tx` で記録を確かめてから R2 に書く (外部キーは RLS を通らない)。R2 のキーは rust と同じ
//! `{tenant}/maintenance/{record}/{uuid}.{ext}` で、uuid はその確かめの文の `gen_random_uuid()` で取る。
//! R2 の await は `tenant_tx` の中に入れない。ログは固定の語だけ (ファイル名・キー・tenant を出さない)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Multipart, Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::store::content_disposition;
use crate::AppState;

/// rust の `MaintenanceFile`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct File {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub record_id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub storage_key: String,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TxOutput for File {}

macro_rules! columns {
    () => {
        "id, tenant_id, record_id, filename, content_type, size_bytes, storage_key, created_at, deleted_at"
    };
}

pub mod sql {
    /// 記録がこの会社に在るか (論理削除したものは無い扱い) と、新しいファイルの uuid
    pub const RECORD_AND_UUID: &str = "SELECT gen_random_uuid() FROM alc_api.maintenance_records \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const RECORD_IN_TENANT: &str = "SELECT EXISTS(SELECT 1 FROM alc_api.maintenance_records \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL)";

    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.maintenance_files \
         (tenant_id, record_id, filename, content_type, size_bytes, storage_key) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING ",
        columns!()
    );

    pub const LIST: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.maintenance_files \
         WHERE record_id = $1 AND tenant_id = $2 AND deleted_at IS NULL ORDER BY created_at"
    );

    pub const GET_FOR_DOWNLOAD: &str = "SELECT filename, content_type, storage_key \
         FROM alc_api.maintenance_files WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const SOFT_DELETE: &str = "UPDATE alc_api.maintenance_files SET deleted_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";
}

fn file(r: &Row) -> File {
    File {
        id: r.get(0),
        tenant_id: r.get(1),
        record_id: r.get(2),
        filename: r.get(3),
        content_type: r.get(4),
        size_bytes: r.get(5),
        storage_key: r.get(6),
        created_at: r.get(7),
        deleted_at: r.get(8),
    }
}

/// R2 のキー (rust と同じ。拡張子は最後の `.` より後 = `.` が無ければ名前の全体)
pub fn storage_key(tenant_id: Uuid, record_id: Uuid, file_uuid: Uuid, filename: &str) -> String {
    let ext = filename.rsplit('.').next().unwrap_or("bin");
    format!("{tenant_id}/maintenance/{record_id}/{file_uuid}.{ext}")
}

/// `(stmt, $1)` で 1 行を引く文を流す小さな部品 (`$2` は tenant)
async fn by_id<T: TxOutput>(
    pg: &mut PgClient,
    tenant_id: Uuid,
    stmt: &'static str,
    id: Uuid,
    read: fn(Option<&Row>) -> T,
) -> Result<T, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(stmt, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(read(row.as_ref()))
        })
    })
    .await
}

pub struct NewFile {
    pub record_id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub storage_key: String,
}

pub async fn insert(pg: &mut PgClient, tenant_id: Uuid, new: NewFile) -> Result<File, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = &new;
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&n.record_id, Type::UUID),
                        (&n.filename, Type::TEXT),
                        (&n.content_type, Type::TEXT),
                        (&n.size_bytes, Type::INT8),
                        (&n.storage_key, Type::TEXT),
                    ],
                )
                .await?;
            Ok(file(&row))
        })
    })
    .await
}

/// 記録がこの会社に在れば、その記録の添付 (無ければ None)
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    record_id: Uuid,
) -> Result<Option<Vec<File>>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let params = [(&record_id as _, Type::UUID), (&tenant_id as _, Type::UUID)];
            let found: bool = tx
                .query_typed_one(sql::RECORD_IN_TENANT, &params)
                .await?
                .get(0);
            if !found {
                return Ok(None);
            }
            let rows = tx.query_typed(sql::LIST, &params).await?;
            Ok(Some(rows.iter().map(file).collect()))
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/maintenance/records/{record_id}/files",
            post(upload_file).get(list_files),
        )
        .route("/maintenance/files/{file_id}/download", get(download_file))
        .route("/maintenance/files/{file_id}", delete(delete_file))
}

/// rust と同じ順: 記録の確かめ (404) → R2 の binding (503) → multipart の読み (400) → R2 → 行の作成
async fn upload_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(record_id): Path<Uuid>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<File>), StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        by_id(&mut pg, tenant_id, sql::RECORD_AND_UUID, record_id, |r| {
            r.map(|r| r.get::<_, Uuid>(0))
        })
        .await
    };
    let file_uuid = found
        .map_err(|e| state.db_fail("maintenance file upload record", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = state
        .maintenance_files
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let field = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .ok_or(StatusCode::BAD_REQUEST)?;
    let filename = field.file_name().unwrap_or("unknown").to_owned();
    let content_type = field
        .content_type()
        .unwrap_or("application/octet-stream")
        .to_owned();
    let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
    let size_bytes = data.len() as i64;

    let key = storage_key(tenant_id, record_id, file_uuid, &filename);
    store
        .put(&key, data.to_vec(), &content_type)
        .await
        .map_err(|e| state.store_fail("maintenance file upload", e))?;
    let new = NewFile {
        record_id,
        filename,
        content_type,
        size_bytes,
        storage_key: key,
    };
    let mut pg = state.pg.lock().await;
    let f = insert(&mut pg, tenant_id, new)
        .await
        .map_err(|e| state.db_fail("maintenance file create", &e))?;
    Ok((StatusCode::CREATED, Json(f)))
}

async fn list_files(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(record_id): Path<Uuid>,
) -> Result<Json<Vec<File>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id, record_id)
        .await
        .map_err(|e| state.db_fail("maintenance file list", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// rust と同じ順: 行 (404) → R2 の binding (503) → R2 から読む
async fn download_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(file_id): Path<Uuid>,
) -> Result<impl IntoResponse, StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        by_id(&mut pg, tenant_id, sql::GET_FOR_DOWNLOAD, file_id, |r| {
            r.map(|r| {
                (
                    r.get::<_, String>(0),
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                )
            })
        })
        .await
    };
    let (filename, content_type, key) = found
        .map_err(|e| state.db_fail("maintenance file download", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = state
        .maintenance_files
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let data = store
        .get(&key)
        .await
        .map_err(|e| state.store_fail("maintenance file download", e))?;
    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_DISPOSITION, content_disposition(&filename)),
        ],
        data,
    ))
}

async fn delete_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(file_id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                let n = tx
                    .execute_typed(
                        sql::SOFT_DELETE,
                        &[(&file_id, Type::UUID), (&tenant_id, Type::UUID)],
                    )
                    .await?;
                Ok(n > 0)
            })
        })
        .await
        .map_err(|e| state.db_fail("maintenance file delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_the_rust_layout() {
        let t = Uuid::from_u128(1);
        let r = Uuid::from_u128(2);
        let f = Uuid::from_u128(3);
        assert_eq!(
            storage_key(t, r, f, "点検.v2.jpg"),
            format!("{t}/maintenance/{r}/{f}.jpg")
        );
        assert_eq!(
            storage_key(t, r, f, "README"),
            format!("{t}/maintenance/{r}/{f}.README")
        );
    }
}
