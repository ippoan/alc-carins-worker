//! 車検証のファイル (rust の `alc-carins` の carins_files.rs と repo/carins_files.rs の写し)。
//!
//! - `GET /files?type=` — 論理削除していないものを新しい順 (`type` で Content-Type を絞る)
//! - `GET /files/recent` — 同じく新しい 50 件
//! - `POST /files` — `{filename, type, content (base64)}` を R2 (`{tenant}/{uuid}`) に置き、在ることを確かめてから行を作る
//!   (201)。JSON / PDF なら車検証として読む ([`crate::parse`]。失敗しても 201)。smb の取り込み (auth-worker の
//!   SmbIngest)・端末の取り込み (device-data-proxy)・nuxt-pwa-carins が呼ぶ
//! - `GET /files/{uuid}/download` — 本体 (R2。古い行は DB の base64 の `blob`)。論理削除したものも落とせる (rust と同じ)
//! - `POST /files/verify` — `{uuids}` の各ファイルが R2 に在るか (`{uuid: bool}`)。確かめた結果は行に残し、
//!   残っている行は R2 を見ない
//!
//! rust との違い:
//! - 一覧の口は、確かめていない行を裏で確かめる (`tokio::spawn`) のをやめた (Workers は応答の後の処理を待たない。
//!   画面は一覧の後に `/files/verify` を呼ぶので、そちらで確かめる)
//! - 登録の応答の `storageVerified` は `true` (rust は行を作った直後の NULL を返し、その後に true へ更新していた。
//!   残る行は同じ)
//! - `{uuid}` が UUID でなければ 400 (rust は DB の型変換の失敗で 500)。`/files/verify` の UUID でない値は
//!   結果に入れない (rust は 500)
//! - R2 の binding が無ければ 503 (rust は storage が必ず在った)
//! - 新しいファイルの uuid は `gen_random_uuid()` で取る (wasm で乱数の crate を持たないため)

use std::collections::HashMap;

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::parse::{self, Skip};
use crate::store::content_disposition;
use crate::AppState;

/// rust の `FileRow` (camelCase。日時は rust と同じ `to_char` の文字列。一覧の `blob` は常に null)
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileRow {
    pub uuid: String,
    pub filename: String,
    pub file_type: String,
    pub created: String,
    pub deleted: Option<String>,
    pub blob: Option<String>,
    pub s3_key: Option<String>,
    pub storage_class: Option<String>,
    pub last_accessed_at: Option<String>,
    pub access_count_weekly: Option<i32>,
    pub access_count_total: Option<i32>,
    pub promoted_to_standard_at: Option<String>,
    pub storage_verified: Option<bool>,
}

impl TxOutput for FileRow {}

/// rust の `FileListResponse`
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub files: Vec<FileRow>,
}

macro_rules! file_select {
    () => {
        r#"uuid::text, filename, type,
        to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        to_char(deleted_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        NULL::text, s3_key, storage_class,
        to_char(last_accessed_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        access_count_weekly, access_count_total,
        to_char(promoted_to_standard_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        storage_verified"#
    };
}

pub mod sql {
    /// `$2` が NULL なら全部の type
    pub const LIST: &str = concat!(
        "SELECT ",
        file_select!(),
        " FROM alc_api.files WHERE tenant_id = $1 AND deleted_at IS NULL \
         AND ($2::text IS NULL OR type = $2) ORDER BY created_at DESC"
    );

    pub const LIST_RECENT: &str = concat!(
        "SELECT ",
        file_select!(),
        " FROM alc_api.files WHERE tenant_id = $1 AND deleted_at IS NULL \
         ORDER BY created_at DESC LIMIT 50"
    );

    pub const NEW_UUID: &str = "SELECT gen_random_uuid()";

    /// R2 に在ることを確かめた後に作るので `storage_verified` は true
    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.files \
         (uuid, tenant_id, filename, type, created_at, s3_key, storage_class, last_accessed_at, storage_verified) \
         VALUES ($1, $2, $3, $4, now(), $5, 'STANDARD', now(), true) RETURNING ",
        file_select!()
    );

    pub const GET_FOR_DOWNLOAD: &str =
        "SELECT filename, type, s3_key, blob FROM alc_api.files WHERE tenant_id = $1 AND uuid = $2";

    pub const VERIFY_ROWS: &str = "SELECT uuid, storage_verified, s3_key, blob IS NOT NULL \
         FROM alc_api.files WHERE tenant_id = $1 AND uuid = ANY($2)";

    pub const SET_VERIFIED: &str =
        "UPDATE alc_api.files SET storage_verified = $3 WHERE tenant_id = $1 AND uuid = $2";
}

fn file_row(r: &Row) -> FileRow {
    FileRow {
        uuid: r.get(0),
        filename: r.get(1),
        file_type: r.get(2),
        created: r.get(3),
        deleted: r.get(4),
        blob: r.get(5),
        s3_key: r.get(6),
        storage_class: r.get(7),
        last_accessed_at: r.get(8),
        access_count_weekly: r.get(9),
        access_count_total: r.get(10),
        promoted_to_standard_at: r.get(11),
        storage_verified: r.get(12),
    }
}

/// R2 のキー (rust と同じ `{tenant}/{uuid}`)
pub fn storage_key(tenant_id: Uuid, file_uuid: Uuid) -> String {
    format!("{tenant_id}/{file_uuid}")
}

pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    type_filter: Option<String>,
) -> Result<Vec<FileRow>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST,
                    &[(&tenant_id, Type::UUID), (&type_filter, Type::TEXT)],
                )
                .await?;
            Ok(rows.iter().map(file_row).collect())
        })
    })
    .await
}

pub async fn list_recent(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<FileRow>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_RECENT, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(file_row).collect())
        })
    })
    .await
}

pub async fn new_uuid(pg: &mut PgClient, tenant_id: Uuid) -> Result<Uuid, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move { Ok(tx.query_typed_one(sql::NEW_UUID, &[]).await?.get(0)) })
    })
    .await
}

pub async fn insert(
    pg: &mut PgClient,
    tenant_id: Uuid,
    file_uuid: Uuid,
    filename: String,
    file_type: String,
    key: String,
) -> Result<FileRow, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&file_uuid, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&filename, Type::TEXT),
                        (&file_type, Type::TEXT),
                        (&key, Type::TEXT),
                    ],
                )
                .await?;
            Ok(file_row(&row))
        })
    })
    .await
}

/// 落とすのに要る (名前, Content-Type, キー, base64 の本体)
pub type Download = (String, String, Option<String>, Option<String>);

pub async fn get_for_download(
    pg: &mut PgClient,
    tenant_id: Uuid,
    file_uuid: Uuid,
) -> Result<Option<Download>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_FOR_DOWNLOAD,
                    &[(&tenant_id, Type::UUID), (&file_uuid, Type::UUID)],
                )
                .await?;
            Ok(row.map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))))
        })
    })
    .await
}

/// 確かめる行 (uuid, 確かめた結果, キー, base64 の本体が在るか)
pub type VerifyRow = (Uuid, Option<bool>, Option<String>, bool);

pub async fn verify_rows(
    pg: &mut PgClient,
    tenant_id: Uuid,
    uuids: Vec<Uuid>,
) -> Result<Vec<VerifyRow>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::VERIFY_ROWS,
                    &[(&tenant_id, Type::UUID), (&uuids, Type::UUID_ARRAY)],
                )
                .await?;
            Ok(rows
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                .collect())
        })
    })
    .await
}

pub async fn set_verified(
    pg: &mut PgClient,
    tenant_id: Uuid,
    results: Vec<(Uuid, bool)>,
) -> Result<(), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            for (id, verified) in &results {
                tx.execute_typed(
                    sql::SET_VERIFIED,
                    &[
                        (&tenant_id, Type::UUID),
                        (id, Type::UUID),
                        (verified, Type::BOOL),
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/files", get(list_files).post(create_file))
        .route("/files/recent", get(list_recent_files))
        .route("/files/{uuid}/download", get(download_file))
        .route("/files/verify", post(verify_files))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(rename = "type")]
    pub type_filter: Option<String>,
}

async fn list_files(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let files = list(&mut pg, tenant_id, q.type_filter)
        .await
        .map_err(|e| state.db_fail("list_files", &e))?;
    Ok(Json(ListResponse { files }))
}

async fn list_recent_files(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<ListResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let files = list_recent(&mut pg, tenant_id)
        .await
        .map_err(|e| state.db_fail("list_recent", &e))?;
    Ok(Json(ListResponse { files }))
}

#[derive(Debug, Deserialize)]
pub struct CreateFileRequest {
    pub filename: String,
    #[serde(rename = "type")]
    pub file_type: String,
    /// base64
    pub content: String,
}

/// rust と同じ順: base64 (400) → R2 に置く → 在るかの確かめ (無ければ 500) → 行の作成 → 車検証として読む
async fn create_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateFileRequest>,
) -> Result<(StatusCode, Json<FileRow>), StatusCode> {
    let data = STANDARD
        .decode(&body.content)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let store = state
        .carins_files
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let file_uuid = {
        let mut pg = state.pg.lock().await;
        new_uuid(&mut pg, tenant_id).await
    }
    .map_err(|e| state.db_fail("create_file uuid", &e))?;
    let key = storage_key(tenant_id, file_uuid);
    store
        .put(&key, data.clone(), &body.file_type)
        .await
        .map_err(|e| state.store_fail("create_file upload", e))?;
    let verified = store
        .exists(&key)
        .await
        .map_err(|e| state.store_fail("create_file exists", e))?;
    if !verified {
        (state.log)("carins: create_file failed: r2 (not found after upload)");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let mut pg = state.pg.lock().await;
    let row = insert(
        &mut pg,
        tenant_id,
        file_uuid,
        body.filename,
        body.file_type.clone(),
        key,
    )
    .await
    .map_err(|e| state.db_fail("create_file insert", &e))?;
    let parsed = match body.file_type.as_str() {
        "application/json" => parse::apply_json(&mut pg, tenant_id, file_uuid, &data).await,
        "application/pdf" => parse::apply_pdf(&mut pg, tenant_id, file_uuid, &data).await,
        _ => Ok(()),
    };
    match parsed {
        Ok(()) => {}
        Err(Skip::Reason(r)) => (state.log)(&format!("carins: car inspection parse skipped ({r})")),
        Err(Skip::Db) => (state.log)("carins: car inspection parse skipped (db)"),
    }
    Ok((StatusCode::CREATED, Json(row)))
}

/// rust と同じ順: 行 (404) → R2 のキーが在れば R2、無ければ base64 の本体、どちらも無ければ 404
async fn download_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(file_uuid): Path<Uuid>,
) -> Result<impl IntoResponse, StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        get_for_download(&mut pg, tenant_id, file_uuid).await
    };
    let (filename, content_type, key, blob) = found
        .map_err(|e| state.db_fail("download_file", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let data = match (key, blob) {
        (Some(key), _) => {
            let store = state
                .carins_files
                .clone()
                .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
            store
                .get(&key)
                .await
                .map_err(|e| state.store_fail("download_file", e))?
        }
        (None, Some(blob)) => STANDARD.decode(blob).map_err(|_| {
            (state.log)("carins: download_file failed: blob (not base64)");
            StatusCode::INTERNAL_SERVER_ERROR
        })?,
        (None, None) => return Err(StatusCode::NOT_FOUND),
    };
    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_DISPOSITION, content_disposition(&filename)),
        ],
        data,
    ))
}

#[derive(Debug, Deserialize)]
pub struct VerifyRequest {
    pub uuids: Vec<String>,
}

/// 確かめた結果が行に在ればそれ、無ければ R2 (キーが在るとき) か base64 の本体の有無で決めて行に残す。
/// 無い uuid・UUID でない値は結果に入れない
async fn verify_files(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<VerifyRequest>,
) -> Result<Json<HashMap<String, bool>>, StatusCode> {
    let store = state
        .carins_files
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    // 応答のキーは受け取った文字列のまま (rust と同じ)
    let asked: Vec<(String, Uuid)> = body
        .uuids
        .into_iter()
        .filter_map(|s| Uuid::parse_str(&s).ok().map(|u| (s, u)))
        .collect();
    let ids: Vec<Uuid> = asked.iter().map(|(_, u)| *u).collect();
    let rows = {
        let mut pg = state.pg.lock().await;
        verify_rows(&mut pg, tenant_id, ids).await
    }
    .map_err(|e| state.db_fail("verify_files", &e))?;
    let mut by_id: HashMap<Uuid, bool> = HashMap::new();
    let mut checked = Vec::new();
    for (id, stored, key, has_blob) in rows {
        let verified = match (stored, key) {
            (Some(v), _) => v,
            (None, Some(key)) => {
                let v = store.exists(&key).await.unwrap_or(false);
                checked.push((id, v));
                v
            }
            (None, None) => {
                checked.push((id, has_blob));
                has_blob
            }
        };
        by_id.insert(id, verified);
    }
    if !checked.is_empty() {
        let mut pg = state.pg.lock().await;
        if let Err(e) = set_verified(&mut pg, tenant_id, checked).await {
            // rust と同じく結果は返す (残せなかったことだけログに出す)
            state.db_fail("verify_files update", &e);
        }
    }
    let results = asked
        .into_iter()
        .filter_map(|(s, u)| by_id.get(&u).map(|v| (s, *v)))
        .collect();
    Ok(Json(results))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_tenant_and_uuid() {
        let t = Uuid::from_u128(1);
        let f = Uuid::from_u128(2);
        assert_eq!(storage_key(t, f), format!("{t}/{f}"));
    }
}
