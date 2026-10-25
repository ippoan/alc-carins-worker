//! NFC タグと車検証の紐づけ (rust の `alc-carins` の nfc_tags.rs と repo/nfc_tags.rs の写し。nuxt-pwa-carins が呼ぶ)。
//!
//! - `GET /nfc-tags/search?uuid=` — タグと、その車検証の行 (`to_jsonb`)。無ければ 404
//! - `GET /nfc-tags?car_inspection_id=` — タグを新しい順 (絞り込みは任意)
//! - `POST /nfc-tags` — 登録 (201)。同じタグは付け替える
//! - `DELETE /nfc-tags/{nfc_uuid}` — 削除 (204 / 404)
//!
//! タグの uuid は小文字にして `:` を除いてから使う (rust と同じ)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::{AppState, JsonOut};

/// rust の `NfcTag` (camelCase)
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NfcTag {
    pub id: i32,
    pub nfc_uuid: String,
    pub car_inspection_id: i32,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for NfcTag {}

/// rust の `NfcTagSearchResponse`
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub nfc_tag: NfcTag,
    pub car_inspection: Option<Value>,
}

pub mod sql {
    pub const SEARCH: &str = "SELECT id, nfc_uuid, car_inspection_id, created_at \
         FROM alc_api.car_inspection_nfc_tags WHERE tenant_id = $1 AND nfc_uuid = $2";

    pub const CAR_INSPECTION: &str =
        "SELECT to_jsonb(ci) FROM alc_api.car_inspection ci WHERE ci.tenant_id = $1 AND ci.id = $2";

    /// `$2` が NULL なら全部 (rust は 2 本の文に分けていた)
    pub const LIST: &str = "SELECT id, nfc_uuid, car_inspection_id, created_at \
         FROM alc_api.car_inspection_nfc_tags \
         WHERE tenant_id = $1 AND ($2::int4 IS NULL OR car_inspection_id = $2) \
         ORDER BY created_at DESC";

    pub const REGISTER: &str = "INSERT INTO alc_api.car_inspection_nfc_tags \
         (tenant_id, nfc_uuid, car_inspection_id) VALUES ($1, $2, $3) \
         ON CONFLICT (tenant_id, nfc_uuid) DO UPDATE \
         SET car_inspection_id = EXCLUDED.car_inspection_id, created_at = NOW() \
         RETURNING id, nfc_uuid, car_inspection_id, created_at";

    pub const DELETE: &str =
        "DELETE FROM alc_api.car_inspection_nfc_tags WHERE tenant_id = $1 AND nfc_uuid = $2";
}

/// rust の `normalize_nfc_uuid`
pub fn normalize_nfc_uuid(uuid: &str) -> String {
    uuid.to_lowercase().replace(':', "")
}

fn tag(r: &Row) -> NfcTag {
    NfcTag {
        id: r.get(0),
        nfc_uuid: r.get(1),
        car_inspection_id: r.get(2),
        created_at: r.get(3),
    }
}

/// タグと、その車検証の行 (1 つの transaction)
pub async fn search(
    pg: &mut PgClient,
    tenant_id: Uuid,
    nfc_uuid: String,
) -> Result<Option<(NfcTag, Option<JsonOut>)>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let Some(row) = tx
                .query_typed_opt(
                    sql::SEARCH,
                    &[(&tenant_id, Type::UUID), (&nfc_uuid, Type::TEXT)],
                )
                .await?
            else {
                return Ok(None);
            };
            let t = tag(&row);
            let ci = tx
                .query_typed_opt(
                    sql::CAR_INSPECTION,
                    &[(&tenant_id, Type::UUID), (&t.car_inspection_id, Type::INT4)],
                )
                .await?;
            Ok(Some((t, ci.map(|r| JsonOut(r.get(0))))))
        })
    })
    .await
}

pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    car_inspection_id: Option<i32>,
) -> Result<Vec<NfcTag>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST,
                    &[(&tenant_id, Type::UUID), (&car_inspection_id, Type::INT4)],
                )
                .await?;
            Ok(rows.iter().map(tag).collect())
        })
    })
    .await
}

pub async fn register(
    pg: &mut PgClient,
    tenant_id: Uuid,
    nfc_uuid: String,
    car_inspection_id: i32,
) -> Result<NfcTag, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::REGISTER,
                    &[
                        (&tenant_id, Type::UUID),
                        (&nfc_uuid, Type::TEXT),
                        (&car_inspection_id, Type::INT4),
                    ],
                )
                .await?;
            Ok(tag(&row))
        })
    })
    .await
}

pub async fn remove(pg: &mut PgClient, tenant_id: Uuid, nfc_uuid: String) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(
                    sql::DELETE,
                    &[(&tenant_id, Type::UUID), (&nfc_uuid, Type::TEXT)],
                )
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/nfc-tags", get(list_tags).post(register_tag))
        .route("/nfc-tags/search", get(search_by_uuid))
        .route("/nfc-tags/{nfc_uuid}", delete(delete_tag))
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub uuid: String,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub car_inspection_id: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub nfc_uuid: String,
    pub car_inspection_id: i32,
}

async fn search_by_uuid(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let (nfc_tag, ci) = search(&mut pg, tenant_id, normalize_nfc_uuid(&q.uuid))
        .await
        .map_err(|e| state.db_fail("nfc search", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(SearchResponse {
        nfc_tag,
        car_inspection: ci.map(|j| j.0),
    }))
}

async fn list_tags(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<NfcTag>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id, q.car_inspection_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("nfc list", &e))
}

async fn register_tag(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<NfcTag>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let t = register(
        &mut pg,
        tenant_id,
        normalize_nfc_uuid(&body.nfc_uuid),
        body.car_inspection_id,
    )
    .await
    .map_err(|e| state.db_fail("nfc register", &e))?;
    Ok((StatusCode::CREATED, Json(t)))
}

async fn delete_tag(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(nfc_uuid): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = remove(&mut pg, tenant_id, normalize_nfc_uuid(&nfc_uuid))
        .await
        .map_err(|e| state.db_fail("nfc delete", &e))?;
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
    fn nfc_uuid_is_lowercase_without_colons() {
        assert_eq!(normalize_nfc_uuid("04:A1:B2:C3"), "04a1b2c3");
        assert_eq!(normalize_nfc_uuid("abc"), "abc");
    }
}
