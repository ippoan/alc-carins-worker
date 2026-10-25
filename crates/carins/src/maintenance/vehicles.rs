//! 整備の車両 (rust の `alc-maintenance` の vehicles.rs の写し)。
//!
//! - `GET /maintenance/vehicles` — 絞り込み (`q` = 登録番号・表示名の部分一致、`linked` = 車検証に紐づいているか) と頁
//! - `POST /maintenance/vehicles` — 201。登録番号が空白だけなら 400、同じ `car_id` の車両が在れば 409
//! - `GET`/`PUT`/`DELETE /maintenance/vehicles/{id}` — 更新は渡した列だけ (COALESCE)。削除は論理削除 (204)。無ければ 404
//! - `PUT /maintenance/vehicles/{id}/carins` — 管理番号 / 車両 ID で車検証を照合して `car_id` を紐づける
//!   (形が違う・一致しない → 400、車両が無い → 404、別の車両がその `car_id` を持つ → 409)
//! - `DELETE /maintenance/vehicles/{id}/carins` — 紐づけを外す (204 / 404)
//! - `GET /maintenance/vehicles/{id}/carins-candidates` — 登録番号が一致しそうな車検証 (`CarId` ごとに最新の交付日の 1 枚)
//! - `GET /maintenance/vehicles/carins-import-candidates` — まだ車両になっていない車検証の全部
//! - `POST /maintenance/vehicles/carins-import` — 選んだ `car_id` (1〜500 件) を 1 つの transaction で取り込む
//!   (登録番号の一致する車両が在れば紐づけ、無ければ作る。既に紐づいていれば飛ばす)。件数だけ返す
//!
//! 所有者・住所・車台番号は返さない (rust と同じ)。
//! rust との違い: 紐づけ (照合 → `CarId` を引く → 更新) を 1 つの transaction にした (rust は 3 つの接続)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use super::records::paging;
use crate::lookup::{lookup_expiry, normalize_carins_numbers};
use crate::{is_unique_violation, AppState};

/// `POST .../carins-import` の 1 回の上限 (rust と同じ)
pub const MAX_IMPORT_CAR_IDS: usize = 500;

/// 全角の数字・ダッシュを半角へ (rust の `normalize_registration_number`。SQL の `translate` と同じ文字の組)
pub fn normalize_registration_number(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '０'..='９' => char::from_u32(c as u32 - '０' as u32 + '0' as u32).unwrap_or(c),
            '－' | 'ー' => '-',
            other => other,
        })
        .collect()
}

/// rust の `MaintenanceVehicle`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Vehicle {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub registration_number: String,
    pub display_name: Option<String>,
    pub car_id: Option<String>,
    pub carins_linked_at: Option<DateTime<Utc>>,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TxOutput for Vehicle {}

/// rust の `CreateMaintenanceVehicle`
#[derive(Debug, Clone, Deserialize)]
pub struct CreateVehicle {
    pub registration_number: String,
    pub display_name: Option<String>,
    pub car_id: Option<String>,
    pub note: Option<String>,
}

/// rust の `UpdateMaintenanceVehicle` (`None` の列は変えない)
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateVehicle {
    pub registration_number: Option<String>,
    pub display_name: Option<String>,
    pub note: Option<String>,
}

/// rust の `VehicleListFilter`
#[derive(Debug, Default, Deserialize)]
pub struct ListFilter {
    pub q: Option<String>,
    pub linked: Option<bool>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `VehicleListResponse`
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub items: Vec<Vehicle>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

/// rust の `LinkCarinsRequest`
#[derive(Debug, Clone, Deserialize)]
pub struct LinkCarinsRequest {
    pub cert_no: Option<String>,
    pub car_id: Option<String>,
}

/// rust の `CarinsCandidate`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Candidate {
    pub car_id: String,
    pub cert_no: String,
    pub car_no: String,
}

impl TxOutput for Candidate {}

/// rust の `CarinsImportCandidate`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ImportCandidate {
    pub car_id: String,
    pub cert_no: String,
    pub car_no: String,
    pub existing_vehicle_id: Option<Uuid>,
}

impl TxOutput for ImportCandidate {}

/// rust の `CarinsImportRequest`
#[derive(Debug, Clone, Deserialize)]
pub struct ImportRequest {
    pub car_ids: Vec<String>,
}

/// rust の `CarinsImportResult`
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ImportResult {
    pub created: i64,
    pub linked: i64,
    pub skipped: i64,
}

impl TxOutput for ImportResult {}

macro_rules! columns {
    () => {
        "id, tenant_id, registration_number, display_name, car_id, carins_linked_at, note, \
         created_at, updated_at, deleted_at"
    };
}

macro_rules! list_filter {
    () => {
        " FROM alc_api.maintenance_vehicles \
         WHERE tenant_id = $1 AND deleted_at IS NULL \
           AND ($2::text IS NULL OR registration_number ILIKE '%' || $2 || '%' \
                OR COALESCE(display_name, '') ILIKE '%' || $2 || '%') \
           AND ($3::bool IS NULL OR (car_id IS NOT NULL) = $3)"
    };
}

/// 全角の数字・ダッシュを半角にする SQL の式の中身 (rust と同じ文字の組)
macro_rules! tr {
    ($e:literal) => {
        concat!(
            "translate(",
            $e,
            ", '０１２３４５６７８９－ー', '0123456789--')"
        )
    };
}

pub mod sql {
    pub const COUNT: &str = concat!("SELECT COUNT(*)", list_filter!());

    pub const LIST: &str = concat!(
        "SELECT ",
        columns!(),
        list_filter!(),
        " ORDER BY created_at DESC LIMIT $4 OFFSET $5"
    );

    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number, display_name, car_id, note) \
         VALUES ($1, $2, $3, $4, $5) RETURNING ",
        columns!()
    );

    pub const GET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.maintenance_vehicles WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL"
    );

    pub const UPDATE: &str = concat!(
        "UPDATE alc_api.maintenance_vehicles SET \
         registration_number = COALESCE($3, registration_number), \
         display_name = COALESCE($4, display_name), \
         note = COALESCE($5, note), updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL RETURNING ",
        columns!()
    );

    pub const SOFT_DELETE: &str = "UPDATE alc_api.maintenance_vehicles \
         SET deleted_at = now(), updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const LINK: &str = concat!(
        "UPDATE alc_api.maintenance_vehicles \
         SET car_id = $3, carins_linked_at = now(), updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL RETURNING ",
        columns!()
    );

    pub const UNLINK: &str = "UPDATE alc_api.maintenance_vehicles \
         SET car_id = NULL, carins_linked_at = NULL, updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    /// 管理番号から `CarId` を引く (`lookup_expiry` は `CarId` を返さないため)
    pub const CAR_ID_BY_CERT_NO: &str = r#"SELECT NULLIF("CarId", '') FROM alc_api.car_inspection
        WHERE "ElectCertMgNo" = $1 AND tenant_id = $2
        ORDER BY "GrantdateY" DESC, "GrantdateM" DESC, "GrantdateD" DESC LIMIT 1"#;

    /// 車両の (正規化した) 登録番号が車検証の値を含むもの。空の値は全部に当たるので除く
    pub const CANDIDATES: &str = concat!(
        r#"SELECT DISTINCT ON (ci."CarId")
            ci."CarId", ci."ElectCertMgNo", COALESCE(NULLIF(ci."EntryNoCarNo", ''), ci."CarNo")
        FROM alc_api.car_inspection ci
        WHERE ci.tenant_id = $2
          AND ((ci."EntryNoCarNo" <> '' AND $1 ILIKE '%' || "#,
        tr!(r#"ci."EntryNoCarNo""#),
        r#" || '%')
            OR (ci."CarNo" <> '' AND $1 ILIKE '%' || "#,
        tr!(r#"ci."CarNo""#),
        r#" || '%'))
        ORDER BY ci."CarId", ci."GrantdateY" DESC, ci."GrantdateM" DESC, ci."GrantdateD" DESC"#
    );

    pub const IMPORT_CANDIDATES: &str = concat!(
        r#"SELECT DISTINCT ON (ci."CarId")
            ci."CarId", ci."ElectCertMgNo", COALESCE(NULLIF(ci."EntryNoCarNo", ''), ci."CarNo"), mv.id
        FROM alc_api.car_inspection ci
        LEFT JOIN alc_api.maintenance_vehicles mv
          ON mv.tenant_id = $1 AND mv.deleted_at IS NULL
         AND "#,
        tr!("mv.registration_number"),
        " = ",
        tr!(r#"COALESCE(NULLIF(ci."EntryNoCarNo", ''), ci."CarNo")"#),
        r#"
        WHERE ci.tenant_id = $1
          AND ci."CarId" <> ''
          AND COALESCE(NULLIF(ci."EntryNoCarNo", ''), ci."CarNo") <> ''
          AND NOT EXISTS (
            SELECT 1 FROM alc_api.maintenance_vehicles linked
            WHERE linked.tenant_id = $1 AND linked.car_id = ci."CarId")
        ORDER BY ci."CarId", ci."GrantdateY" DESC, ci."GrantdateM" DESC, ci."GrantdateD" DESC, mv.created_at"#
    );

    /// 取り込み (1): 既にこの `car_id` を持つ車両 (論理削除したものも。一意の索引も見ないので)
    pub const IMPORT_ALREADY: &str =
        "SELECT id FROM alc_api.maintenance_vehicles WHERE tenant_id = $1 AND car_id = $2 LIMIT 1";

    /// 取り込み (2): 最新の車検証の登録番号相当
    pub const IMPORT_CAR_NO: &str = r#"SELECT COALESCE(NULLIF(ci."EntryNoCarNo", ''), ci."CarNo")
        FROM alc_api.car_inspection ci
        WHERE ci.tenant_id = $1 AND ci."CarId" = $2
        ORDER BY ci."GrantdateY" DESC, ci."GrantdateM" DESC, ci."GrantdateD" DESC
        LIMIT 1"#;

    /// 取り込み (3): 登録番号の一致する車両 (紐づいていないものを先に、同じなら古い順)
    pub const IMPORT_EXISTING: &str = concat!(
        "SELECT id, car_id FROM alc_api.maintenance_vehicles \
         WHERE tenant_id = $1 AND deleted_at IS NULL AND ",
        tr!("registration_number"),
        " = ",
        tr!("$2"),
        " ORDER BY (car_id IS NOT NULL), created_at LIMIT 1"
    );

    pub const IMPORT_LINK: &str = "UPDATE alc_api.maintenance_vehicles \
         SET car_id = $3, carins_linked_at = now(), updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const IMPORT_CREATE: &str = "INSERT INTO alc_api.maintenance_vehicles \
         (tenant_id, registration_number, car_id, carins_linked_at) VALUES ($1, $2, $3, now())";

    pub const SAVEPOINT: &str = "SAVEPOINT carins_import_row";
    pub const RELEASE: &str = "RELEASE SAVEPOINT carins_import_row";
    pub const ROLLBACK_TO: &str = "ROLLBACK TO SAVEPOINT carins_import_row";
}

fn vehicle(r: &Row) -> Vehicle {
    Vehicle {
        id: r.get(0),
        tenant_id: r.get(1),
        registration_number: r.get(2),
        display_name: r.get(3),
        car_id: r.get(4),
        carins_linked_at: r.get(5),
        note: r.get(6),
        created_at: r.get(7),
        updated_at: r.get(8),
        deleted_at: r.get(9),
    }
}

pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    filter: ListFilter,
) -> Result<ListResponse, Error> {
    let (page, per_page) = paging(filter.page, filter.per_page);
    let offset = (page - 1) * per_page;
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let params: [(&(dyn ToSql + Sync), Type); 3] = [
                (&tenant_id, Type::UUID),
                (&filter.q, Type::TEXT),
                (&filter.linked, Type::BOOL),
            ];
            let total: i64 = tx.query_typed_one(sql::COUNT, &params).await?.get(0);
            let mut all = params.to_vec();
            all.push((&per_page, Type::INT8));
            all.push((&offset, Type::INT8));
            let rows = tx.query_typed(sql::LIST, &all).await?;
            Ok((total, rows.iter().map(vehicle).collect::<Vec<_>>()))
        })
    })
    .await
    .map(|(total, items)| ListResponse {
        items,
        total,
        page,
        per_page,
    })
}

/// 1 行を返す文 (`$1` = 車両、`$2` = tenant、`$3` 以降 = `extra`)
async fn one_row(
    pg: &mut PgClient,
    tenant_id: Uuid,
    stmt: &'static str,
    id: Uuid,
    extra: Vec<Option<String>>,
) -> Result<Option<Vehicle>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut params: Vec<(&(dyn ToSql + Sync), Type)> =
                vec![(&id, Type::UUID), (&tenant_id, Type::UUID)];
            params.extend(extra.iter().map(|v| (v as &(dyn ToSql + Sync), Type::TEXT)));
            let row = tx.query_typed_opt(stmt, &params).await?;
            Ok(row.as_ref().map(vehicle))
        })
    })
    .await
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateVehicle,
) -> Result<Vehicle, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.registration_number, Type::TEXT),
                        (&input.display_name, Type::TEXT),
                        (&input.car_id, Type::TEXT),
                        (&input.note, Type::TEXT),
                    ],
                )
                .await?;
            Ok(vehicle(&row))
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<Vehicle>, Error> {
    one_row(pg, tenant_id, sql::GET, id, Vec::new()).await
}

/// 変えた行が在れば true (`stmt` は [`sql::SOFT_DELETE`] か [`sql::UNLINK`])
async fn change(
    pg: &mut PgClient,
    tenant_id: Uuid,
    stmt: &'static str,
    id: Uuid,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(stmt, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

/// 紐づけの結果
#[derive(Debug, Clone, PartialEq)]
pub enum Linked {
    Done(Vehicle),
    /// 車検証が一致しない・`CarId` が引けない (400)
    NoMatch,
    /// 車両が無い (404)
    NotFound,
}

impl TxOutput for Linked {}

/// 照合 → (管理番号だけなら) `CarId` を引く → 紐づける (1 つの transaction。番号の形は呼び手が確かめ済み)。
/// 別の車両が同じ `car_id` を持てば一意制約の違反の `Err`
pub async fn link_carins(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    cert_no: Option<String>,
    car_id: Option<String>,
) -> Result<Linked, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let found = lookup_expiry(tx, tenant_id, cert_no.as_deref(), car_id.as_deref()).await?;
            if found.matched_by == "none" {
                return Ok(Linked::NoMatch);
            }
            let resolved = match car_id {
                Some(c) => Some(c),
                None => car_id_by_cert_no(tx, tenant_id, cert_no.as_deref()).await?,
            };
            let Some(resolved) = resolved else {
                return Ok(Linked::NoMatch);
            };
            let row = tx
                .query_typed_opt(
                    sql::LINK,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&resolved, Type::TEXT),
                    ],
                )
                .await?;
            Ok(match row {
                Some(r) => Linked::Done(vehicle(&r)),
                None => Linked::NotFound,
            })
        })
    })
    .await
}

async fn car_id_by_cert_no(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    cert_no: Option<&str>,
) -> Result<Option<String>, Error> {
    let row = tx
        .query_typed_opt(
            sql::CAR_ID_BY_CERT_NO,
            &[(&cert_no, Type::TEXT), (&tenant_id, Type::UUID)],
        )
        .await?;
    Ok(row.and_then(|r| r.get(0)))
}

pub async fn candidates(
    pg: &mut PgClient,
    tenant_id: Uuid,
    normalized: String,
) -> Result<Vec<Candidate>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::CANDIDATES,
                    &[(&normalized, Type::TEXT), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows
                .iter()
                .map(|r| Candidate {
                    car_id: r.get(0),
                    cert_no: r.get(1),
                    car_no: r.get(2),
                })
                .collect())
        })
    })
    .await
}

pub async fn import_candidates(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<ImportCandidate>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::IMPORT_CANDIDATES, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows
                .iter()
                .map(|r| ImportCandidate {
                    car_id: r.get(0),
                    cert_no: r.get(1),
                    car_no: r.get(2),
                    existing_vehicle_id: r.get(3),
                })
                .collect())
        })
    })
    .await
}

/// 1 行を SAVEPOINT の中で書く。書けた (変えた行が在る) → true、一意制約の違反・変えた行が無い → false (戻す)
async fn import_write(
    tx: &TenantTx<'_>,
    stmt: &'static str,
    params: &[(&(dyn ToSql + Sync), Type)],
) -> Result<bool, Error> {
    tx.execute_typed(sql::SAVEPOINT, &[]).await?;
    match tx.execute_typed(stmt, params).await {
        Ok(n) if n > 0 => {
            tx.execute_typed(sql::RELEASE, &[]).await?;
            Ok(true)
        }
        Ok(_) => {
            tx.execute_typed(sql::ROLLBACK_TO, &[]).await?;
            Ok(false)
        }
        Err(e) if is_unique_violation(&e) => {
            tx.execute_typed(sql::ROLLBACK_TO, &[]).await?;
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// 選んだ `car_id` を 1 つの transaction で取り込む (rust の `carins_import`。同時に走った取り込みとの一意制約の違反は
/// SAVEPOINT で戻して飛ばす)
pub async fn import(
    pg: &mut PgClient,
    tenant_id: Uuid,
    car_ids: Vec<String>,
) -> Result<ImportResult, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut result = ImportResult::default();
            for car_id in &car_ids {
                let car_id = car_id.trim().to_owned();
                if car_id.is_empty() {
                    result.skipped += 1;
                    continue;
                }
                let key: [(&(dyn ToSql + Sync), Type); 2] =
                    [(&tenant_id, Type::UUID), (&car_id, Type::TEXT)];
                if tx
                    .query_typed_opt(sql::IMPORT_ALREADY, &key)
                    .await?
                    .is_some()
                {
                    result.skipped += 1;
                    continue;
                }
                let car_no: Option<String> = tx
                    .query_typed_opt(sql::IMPORT_CAR_NO, &key)
                    .await?
                    .and_then(|r| r.get(0));
                let Some(car_no) = car_no.filter(|v| !v.trim().is_empty()) else {
                    result.skipped += 1;
                    continue;
                };
                let existing = tx
                    .query_typed_opt(
                        sql::IMPORT_EXISTING,
                        &[(&tenant_id, Type::UUID), (&car_no, Type::TEXT)],
                    )
                    .await?
                    .map(|r| (r.get::<_, Uuid>(0), r.get::<_, Option<String>>(1)));
                match existing {
                    // 一致した車両が既に別の車検証に紐づいている → 上書きせず飛ばす
                    Some((_, Some(_))) => result.skipped += 1,
                    Some((id, None)) => {
                        let ok = import_write(
                            tx,
                            sql::IMPORT_LINK,
                            &[
                                (&id, Type::UUID),
                                (&tenant_id, Type::UUID),
                                (&car_id, Type::TEXT),
                            ],
                        )
                        .await?;
                        if ok {
                            result.linked += 1;
                        } else {
                            result.skipped += 1;
                        }
                    }
                    None => {
                        let ok = import_write(
                            tx,
                            sql::IMPORT_CREATE,
                            &[
                                (&tenant_id, Type::UUID),
                                (&car_no, Type::TEXT),
                                (&car_id, Type::TEXT),
                            ],
                        )
                        .await?;
                        if ok {
                            result.created += 1;
                        } else {
                            result.skipped += 1;
                        }
                    }
                }
            }
            Ok(result)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/maintenance/vehicles",
            get(list_vehicles).post(create_vehicle),
        )
        .route(
            "/maintenance/vehicles/carins-import-candidates",
            get(import_candidates_handler),
        )
        .route("/maintenance/vehicles/carins-import", post(import_handler))
        .route(
            "/maintenance/vehicles/{id}",
            get(get_vehicle).put(update_vehicle).delete(delete_vehicle),
        )
        .route(
            "/maintenance/vehicles/{id}/carins",
            put(link_handler).delete(unlink_handler),
        )
        .route(
            "/maintenance/vehicles/{id}/carins-candidates",
            get(candidates_handler),
        )
}

async fn list_vehicles(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<ListFilter>,
) -> Result<Json<ListResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id, filter)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("maintenance vehicles list", &e))
}

async fn create_vehicle(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateVehicle>,
) -> Result<(StatusCode, Json<Vehicle>), StatusCode> {
    if body.registration_number.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let v = create(&mut pg, tenant_id, body).await.map_err(|e| {
        if is_unique_violation(&e) {
            return StatusCode::CONFLICT;
        }
        state.db_fail("maintenance vehicles create", &e)
    })?;
    Ok((StatusCode::CREATED, Json(v)))
}

async fn get_vehicle(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vehicle>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("maintenance vehicles get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_vehicle(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateVehicle>,
) -> Result<Json<Vehicle>, StatusCode> {
    if body
        .registration_number
        .as_deref()
        .is_some_and(|v| v.trim().is_empty())
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let extra = vec![body.registration_number, body.display_name, body.note];
    one_row(&mut pg, tenant_id, sql::UPDATE, id, extra)
        .await
        .map_err(|e| state.db_fail("maintenance vehicles update", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn changed_or_404(
    state: AppState,
    tenant_id: Uuid,
    stmt: &'static str,
    id: Uuid,
    stage: &str,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    if change(&mut pg, tenant_id, stmt, id)
        .await
        .map_err(|e| state.db_fail(stage, &e))?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn delete_vehicle(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    changed_or_404(
        state,
        tenant_id,
        sql::SOFT_DELETE,
        id,
        "maintenance vehicles delete",
    )
    .await
}

async fn unlink_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    changed_or_404(
        state,
        tenant_id,
        sql::UNLINK,
        id,
        "maintenance vehicles unlink",
    )
    .await
}

async fn link_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<LinkCarinsRequest>,
) -> Result<Json<Vehicle>, StatusCode> {
    let (mut cert_no, mut car_id) = (body.cert_no, body.car_id);
    normalize_carins_numbers(&mut cert_no, &mut car_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut pg = state.pg.lock().await;
    match link_carins(&mut pg, tenant_id, id, cert_no, car_id)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                return StatusCode::CONFLICT;
            }
            state.db_fail("maintenance vehicles link", &e)
        })? {
        Linked::Done(v) => Ok(Json(v)),
        Linked::NoMatch => Err(StatusCode::BAD_REQUEST),
        Linked::NotFound => Err(StatusCode::NOT_FOUND),
    }
}

async fn candidates_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<Candidate>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let v = get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("maintenance vehicles candidates", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let normalized = normalize_registration_number(&v.registration_number);
    if normalized.trim().is_empty() {
        return Ok(Json(Vec::new()));
    }
    candidates(&mut pg, tenant_id, normalized)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("maintenance vehicles candidates", &e))
}

async fn import_candidates_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<ImportCandidate>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    import_candidates(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("maintenance vehicles import candidates", &e))
}

async fn import_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<ImportRequest>,
) -> Result<Json<ImportResult>, StatusCode> {
    if body.car_ids.is_empty() || body.car_ids.len() > MAX_IMPORT_CAR_IDS {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    import(&mut pg, tenant_id, body.car_ids)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("maintenance vehicles import", &e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_number_is_halfwidth() {
        assert_eq!(
            normalize_registration_number("品川５００あ１２－３４ー"),
            "品川500あ12-34-"
        );
        assert_eq!(normalize_registration_number("０９"), "09");
    }
}
