//! 車検証の一覧と照合 (rust の `alc-carins` の car_inspections.rs と repo/car_inspections.rs の写し)。
//!
//! - `GET /car-inspections/current` — 車両 (`CarId`) ごとに期限の最も新しい 1 枚。PDF・JSON のファイルの uuid
//!   (`pdfUuid`・`jsonUuid`) を付ける。nuxt-pwa-carins・nuxt-trouble が呼ぶ
//! - `GET /car-inspections/expired` — 期限が 30 日以内 (過ぎたものを含む) の全部を期限の順
//! - `GET /car-inspections/by-car/{car_id}/history` — 1 台の車検証を新しい順 (`pdfUuid`・`jsonUuid` 付き)
//! - `GET /car-inspections/vehicle-categories` — 車種・用途・形状・自家用/事業用の候補 (alc-app が呼ぶ)
//! - `POST /car-inspections/lookup` — 管理番号 / 車両 ID で車検期限を照合 (番号を URL に載せないよう POST。
//!   alc-app と auth-worker の端末の口が呼ぶ)。所有者・住所・車台番号は返さない
//!
//! 行は rust と同じく `to_jsonb` で列の名前のまま返す (`{"carInspections": [...]}`)。

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::lookup::{lookup_expiry, normalize_carins_numbers, CarinsLookup};
use crate::{AppState, JsonOut};

/// 1 枚の車検証に付いた PDF (files_b) と JSON (files_a) の uuid (論理削除していない新しいもの)
macro_rules! file_uuids {
    () => {
        r#"(SELECT uuid::text FROM alc_api.car_inspection_files_b
             WHERE tenant_id = ci.tenant_id
               AND "ElectCertMgNo" = ci."ElectCertMgNo"
               AND "GrantdateE" = ci."GrantdateE"
               AND "GrantdateY" = ci."GrantdateY"
               AND "GrantdateM" = ci."GrantdateM"
               AND "GrantdateD" = ci."GrantdateD"
               AND type = 'application/pdf'
               AND deleted_at IS NULL
             ORDER BY created_at DESC LIMIT 1) AS "pdfUuid",
            (SELECT uuid::text FROM alc_api.car_inspection_files_a
             WHERE tenant_id = ci.tenant_id
               AND "ElectCertMgNo" = ci."ElectCertMgNo"
               AND "GrantdateE" = ci."GrantdateE"
               AND "GrantdateY" = ci."GrantdateY"
               AND "GrantdateM" = ci."GrantdateM"
               AND "GrantdateD" = ci."GrantdateD"
               AND type = 'application/json'
               AND deleted_at IS NULL
             ORDER BY created_at DESC LIMIT 1) AS "jsonUuid""#
    };
}

pub mod sql {
    pub const LIST_CURRENT: &str = concat!(
        r#"SELECT to_jsonb(sub) FROM (
            SELECT DISTINCT ON (ci."CarId") ci.*, "#,
        file_uuids!(),
        r#"
            FROM alc_api.car_inspection ci
            WHERE ci.tenant_id = $1
            ORDER BY ci."CarId",
                     ci."TwodimensionCodeInfoValidPeriodExpirdate" DESC,
                     ci.created_at DESC
        ) sub"#
    );

    pub const LIST_EXPIRED: &str = r#"SELECT to_jsonb(ci) FROM alc_api.car_inspection ci
        WHERE ci.tenant_id = $1
          AND "TwodimensionCodeInfoValidPeriodExpirdate" <= to_char(CURRENT_DATE + INTERVAL '30 days', 'YYMMDD')
        ORDER BY "TwodimensionCodeInfoValidPeriodExpirdate" ASC"#;

    pub const LIST_BY_CAR_ID: &str = concat!(
        r#"SELECT to_jsonb(sub) FROM (
            SELECT ci.*, "#,
        file_uuids!(),
        r#"
            FROM alc_api.car_inspection ci
            WHERE ci.tenant_id = $1 AND ci."CarId" = $2
            ORDER BY ci."TwodimensionCodeInfoValidPeriodExpirdate" DESC,
                     ci.created_at DESC
        ) sub"#
    );

    pub const VEHICLE_CATEGORIES: &str = r#"SELECT
        COALESCE(ARRAY(SELECT DISTINCT "CarKind" FROM alc_api.car_inspection WHERE tenant_id = $1 AND "CarKind" != '' ORDER BY "CarKind"), '{}') AS car_kinds,
        COALESCE(ARRAY(SELECT DISTINCT "Use" FROM alc_api.car_inspection WHERE tenant_id = $1 AND "Use" != '' ORDER BY "Use"), '{}') AS uses,
        COALESCE(ARRAY(SELECT DISTINCT "CarShape" FROM alc_api.car_inspection WHERE tenant_id = $1 AND "CarShape" != '' ORDER BY "CarShape"), '{}') AS car_shapes,
        COALESCE(ARRAY(SELECT DISTINCT "PrivateBusiness" FROM alc_api.car_inspection WHERE tenant_id = $1 AND "PrivateBusiness" != '' ORDER BY "PrivateBusiness"), '{}') AS private_businesses"#;
}

/// rust の `CarInspectionListResponse`
#[derive(Debug, Serialize)]
pub struct ListResponse {
    #[serde(rename = "carInspections")]
    pub car_inspections: Vec<Value>,
}

/// rust の `VehicleCategories`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VehicleCategories {
    pub car_kinds: Vec<String>,
    pub uses: Vec<String>,
    pub car_shapes: Vec<String>,
    pub private_businesses: Vec<String>,
}

impl alc_worker_db::TxOutput for VehicleCategories {}

#[derive(Debug, Deserialize)]
pub struct LookupRequest {
    #[serde(default)]
    pub cert_no: Option<String>,
    #[serde(default)]
    pub car_id: Option<String>,
}

/// 車検期限の照合の応答 (rust の `CarInspectionLookupResponse`)。所有者・住所・車台番号は返さない
#[derive(Debug, Serialize)]
pub struct LookupResponse {
    /// "YYYY-MM-DD"
    pub expires_on: Option<chrono::NaiveDate>,
    /// `cert_no` / `car_id` / `none`
    pub matched_by: String,
    /// 登録番号
    pub car_no: Option<String>,
}

/// `to_jsonb` の 1 列の行を並べる (`stmt` は [`sql::LIST_CURRENT`] か [`sql::LIST_EXPIRED`]、`car_id` は
/// [`sql::LIST_BY_CAR_ID`] のときだけ)
pub async fn list_json(
    pg: &mut PgClient,
    tenant_id: Uuid,
    stmt: &'static str,
    car_id: Option<String>,
) -> Result<Vec<Value>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = match &car_id {
                Some(c) => {
                    tx.query_typed(stmt, &[(&tenant_id, Type::UUID), (c, Type::TEXT)])
                        .await?
                }
                None => tx.query_typed(stmt, &[(&tenant_id, Type::UUID)]).await?,
            };
            Ok(rows.iter().map(|r| JsonOut(r.get(0))).collect::<Vec<_>>())
        })
    })
    .await
    .map(|v| v.into_iter().map(|j| j.0).collect())
}

pub async fn vehicle_categories(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<VehicleCategories, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let r = tx
                .query_typed_one(sql::VEHICLE_CATEGORIES, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(VehicleCategories {
                car_kinds: r.get(0),
                uses: r.get(1),
                car_shapes: r.get(2),
                private_businesses: r.get(3),
            })
        })
    })
    .await
}

impl alc_worker_db::TxOutput for CarinsLookup {}

pub async fn lookup_in_tx(
    pg: &mut PgClient,
    tenant_id: Uuid,
    cert_no: Option<String>,
    car_id: Option<String>,
) -> Result<CarinsLookup, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            lookup_expiry(tx, tenant_id, cert_no.as_deref(), car_id.as_deref()).await
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/car-inspections/current", get(list_current))
        .route("/car-inspections/expired", get(list_expired))
        .route(
            "/car-inspections/vehicle-categories",
            get(vehicle_categories_handler),
        )
        .route(
            "/car-inspections/by-car/{car_id}/history",
            get(list_history),
        )
        .route("/car-inspections/lookup", post(lookup))
}

async fn list_with(
    state: AppState,
    tenant_id: Uuid,
    stmt: &'static str,
    car_id: Option<String>,
    stage: &str,
) -> Result<Json<ListResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let car_inspections = list_json(&mut pg, tenant_id, stmt, car_id)
        .await
        .map_err(|e| state.db_fail(stage, &e))?;
    Ok(Json(ListResponse { car_inspections }))
}

async fn list_current(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<ListResponse>, StatusCode> {
    list_with(state, tenant_id, sql::LIST_CURRENT, None, "list_current").await
}

async fn list_expired(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<ListResponse>, StatusCode> {
    list_with(state, tenant_id, sql::LIST_EXPIRED, None, "list_expired").await
}

async fn list_history(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(car_id): Path<String>,
) -> Result<Json<ListResponse>, StatusCode> {
    list_with(
        state,
        tenant_id,
        sql::LIST_BY_CAR_ID,
        Some(car_id),
        "list_by_car_id",
    )
    .await
}

async fn vehicle_categories_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<VehicleCategories>, StatusCode> {
    let mut pg = state.pg.lock().await;
    vehicle_categories(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("vehicle_categories", &e))
}

/// rust と同じ: 番号の形が違う・両方とも無い → 400。番号はログに出さない
async fn lookup(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(mut body): Json<LookupRequest>,
) -> Result<Json<LookupResponse>, StatusCode> {
    normalize_carins_numbers(&mut body.cert_no, &mut body.car_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if body.cert_no.is_none() && body.car_id.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let found = lookup_in_tx(&mut pg, tenant_id, body.cert_no, body.car_id)
        .await
        .map_err(|e| state.db_fail("lookup_expiry", &e))?;
    Ok(Json(LookupResponse {
        expires_on: found.expires_on,
        matched_by: found.matched_by.to_owned(),
        car_no: found.car_no,
    }))
}
