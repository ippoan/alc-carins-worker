//! 整備記録 (rust の `alc-maintenance` の records.rs の写し)。
//!
//! - `GET /maintenance/records` — 絞り込み (`vehicle_id`・`category_id`・`date_from`・`date_to`・`q` = 内容と工場の部分一致)
//!   と頁 (`page` 既定 1・`per_page` 既定 20、1〜100)。並びは実施日・作成の新しい順
//! - `POST /maintenance/records` — 201。車両・カテゴリがこの会社のものでなければ 400
//! - `GET`/`PUT`/`DELETE /maintenance/records/{id}` — 更新は渡した列だけ (COALESCE)。削除は論理削除 (204)。無ければ 404
//!
//! 外部キーは行の存在しか見ず会社は見ないので、車両・カテゴリの会社を先に確かめる (rust と同じ)。
//! `cost` (NUMERIC(12,2)) は文字列で返す (rust と同じ。f64 に丸めない)。`created_by` は入れない (rust と同じ)。
//! 一覧の絞り込みは、rust の「足した条件の数だけ `$n` を振る」形をやめ、NULL なら効かない条件の 1 本の文にした
//! (結果は同じ)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::AppState;

const DEFAULT_PER_PAGE: i64 = 20;
const MAX_PER_PAGE: i64 = 100;

/// rust の `MaintenanceRecord`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Record {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub vehicle_id: Uuid,
    pub category_id: Uuid,
    pub performed_on: NaiveDate,
    pub odometer_km: Option<i32>,
    pub vendor: Option<String>,
    pub description: Option<String>,
    pub cost: Option<String>,
    pub next_due_on: Option<NaiveDate>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TxOutput for Record {}

/// rust の `CreateMaintenanceRecord`
#[derive(Debug, Clone, Deserialize)]
pub struct CreateRecord {
    pub vehicle_id: Uuid,
    pub category_id: Uuid,
    pub performed_on: NaiveDate,
    pub odometer_km: Option<i32>,
    pub vendor: Option<String>,
    pub description: Option<String>,
    pub cost: Option<f64>,
    pub next_due_on: Option<NaiveDate>,
}

/// rust の `UpdateMaintenanceRecord` (`None` の列は変えない)
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateRecord {
    pub vehicle_id: Option<Uuid>,
    pub category_id: Option<Uuid>,
    pub performed_on: Option<NaiveDate>,
    pub odometer_km: Option<i32>,
    pub vendor: Option<String>,
    pub description: Option<String>,
    pub cost: Option<f64>,
    pub next_due_on: Option<NaiveDate>,
}

/// rust の `MaintenanceRecordListFilter`
#[derive(Debug, Default, Deserialize)]
pub struct ListFilter {
    pub vehicle_id: Option<Uuid>,
    pub category_id: Option<Uuid>,
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<NaiveDate>,
    pub q: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `MaintenanceRecordsResponse`
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub records: Vec<Record>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

macro_rules! columns {
    () => {
        "id, tenant_id, vehicle_id, category_id, performed_on, odometer_km, vendor, description, \
         cost::text, next_due_on, created_by, created_at, updated_at, deleted_at"
    };
}

macro_rules! filter {
    () => {
        " FROM alc_api.maintenance_records \
         WHERE tenant_id = $1 AND deleted_at IS NULL \
           AND ($2::uuid IS NULL OR vehicle_id = $2) \
           AND ($3::uuid IS NULL OR category_id = $3) \
           AND ($4::date IS NULL OR performed_on >= $4) \
           AND ($5::date IS NULL OR performed_on <= $5) \
           AND ($6::text IS NULL OR COALESCE(description, '') ILIKE '%' || $6 || '%' \
                OR COALESCE(vendor, '') ILIKE '%' || $6 || '%')"
    };
}

pub mod sql {
    pub const COUNT: &str = concat!("SELECT COUNT(*)", filter!());

    pub const LIST: &str = concat!(
        "SELECT ",
        columns!(),
        filter!(),
        " ORDER BY performed_on DESC, created_at DESC LIMIT $7 OFFSET $8"
    );

    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.maintenance_records \
         (tenant_id, vehicle_id, category_id, performed_on, odometer_km, vendor, description, cost, next_due_on) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING ",
        columns!()
    );

    pub const GET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.maintenance_records WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL"
    );

    pub const UPDATE: &str = concat!(
        "UPDATE alc_api.maintenance_records SET \
         vehicle_id = COALESCE($3, vehicle_id), \
         category_id = COALESCE($4, category_id), \
         performed_on = COALESCE($5, performed_on), \
         odometer_km = COALESCE($6, odometer_km), \
         vendor = COALESCE($7, vendor), \
         description = COALESCE($8, description), \
         cost = COALESCE($9::numeric, cost), \
         next_due_on = COALESCE($10, next_due_on), \
         updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL RETURNING ",
        columns!()
    );

    pub const SOFT_DELETE: &str = "UPDATE alc_api.maintenance_records \
         SET deleted_at = now(), updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const VEHICLE_IN_TENANT: &str = "SELECT EXISTS(SELECT 1 FROM alc_api.maintenance_vehicles \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL)";

    pub const CATEGORY_IN_TENANT: &str =
        "SELECT EXISTS(SELECT 1 FROM alc_api.maintenance_categories \
         WHERE id = $1 AND tenant_id = $2)";
}

fn record(r: &Row) -> Record {
    Record {
        id: r.get(0),
        tenant_id: r.get(1),
        vehicle_id: r.get(2),
        category_id: r.get(3),
        performed_on: r.get(4),
        odometer_km: r.get(5),
        vendor: r.get(6),
        description: r.get(7),
        cost: r.get(8),
        next_due_on: r.get(9),
        created_by: r.get(10),
        created_at: r.get(11),
        updated_at: r.get(12),
        deleted_at: r.get(13),
    }
}

/// 頁の大きさと番号 (rust と同じ: per_page は 1〜100、page は 1 以上)
pub fn paging(page: Option<i64>, per_page: Option<i64>) -> (i64, i64) {
    (
        page.unwrap_or(1).max(1),
        per_page.unwrap_or(DEFAULT_PER_PAGE).clamp(1, MAX_PER_PAGE),
    )
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
            let f = &filter;
            let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 6] = [
                (&tenant_id, Type::UUID),
                (&f.vehicle_id, Type::UUID),
                (&f.category_id, Type::UUID),
                (&f.date_from, Type::DATE),
                (&f.date_to, Type::DATE),
                (&f.q, Type::TEXT),
            ];
            let total: i64 = tx.query_typed_one(sql::COUNT, &params).await?.get(0);
            let mut all = params.to_vec();
            all.push((&per_page, Type::INT8));
            all.push((&offset, Type::INT8));
            let rows = tx.query_typed(sql::LIST, &all).await?;
            Ok((total, rows.iter().map(record).collect::<Vec<_>>()))
        })
    })
    .await
    .map(|(total, records)| ListResponse {
        records,
        total,
        page,
        per_page,
    })
}

/// 車両・カテゴリがこの会社のものか (渡されたものだけ確かめる)。違えば false
async fn refs_in_tenant(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    vehicle_id: Option<Uuid>,
    category_id: Option<Uuid>,
) -> Result<bool, Error> {
    for (stmt, id) in [
        (sql::VEHICLE_IN_TENANT, vehicle_id),
        (sql::CATEGORY_IN_TENANT, category_id),
    ] {
        let Some(id) = id else { continue };
        let ok: bool = tx
            .query_typed_one(stmt, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
            .await?
            .get(0);
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// 作成。車両・カテゴリが別の会社 (か無い) なら `Ok(None)`
pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateRecord,
) -> Result<Option<Record>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let i = &input;
            if !refs_in_tenant(tx, tenant_id, Some(i.vehicle_id), Some(i.category_id)).await? {
                return Ok(None);
            }
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&i.vehicle_id, Type::UUID),
                        (&i.category_id, Type::UUID),
                        (&i.performed_on, Type::DATE),
                        (&i.odometer_km, Type::INT4),
                        (&i.vendor, Type::TEXT),
                        (&i.description, Type::TEXT),
                        (&i.cost, Type::FLOAT8),
                        (&i.next_due_on, Type::DATE),
                    ],
                )
                .await?;
            Ok(Some(record(&row)))
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<Record>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(record))
        })
    })
    .await
}

/// 更新の結果
#[derive(Debug, Clone, PartialEq)]
pub enum Updated {
    Done(Box<Record>),
    /// 記録が無い (404)
    NotFound,
    /// 車両・カテゴリが別の会社 (か無い) (400)
    BadRef,
}

impl TxOutput for Updated {}

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    input: UpdateRecord,
) -> Result<Updated, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let i = &input;
            if !refs_in_tenant(tx, tenant_id, i.vehicle_id, i.category_id).await? {
                return Ok(Updated::BadRef);
            }
            let row = tx
                .query_typed_opt(
                    sql::UPDATE,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&i.vehicle_id, Type::UUID),
                        (&i.category_id, Type::UUID),
                        (&i.performed_on, Type::DATE),
                        (&i.odometer_km, Type::INT4),
                        (&i.vendor, Type::TEXT),
                        (&i.description, Type::TEXT),
                        (&i.cost, Type::FLOAT8),
                        (&i.next_due_on, Type::DATE),
                    ],
                )
                .await?;
            Ok(match row {
                Some(r) => Updated::Done(Box::new(record(&r))),
                None => Updated::NotFound,
            })
        })
    })
    .await
}

pub async fn soft_delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(
                    sql::SOFT_DELETE,
                    &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/maintenance/records",
            get(list_records).post(create_record),
        )
        .route(
            "/maintenance/records/{id}",
            get(get_record).put(update_record).delete(delete_record),
        )
}

async fn list_records(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<ListFilter>,
) -> Result<Json<ListResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id, filter)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("maintenance records list", &e))
}

async fn create_record(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateRecord>,
) -> Result<(StatusCode, Json<Record>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let r = create(&mut pg, tenant_id, body)
        .await
        .map_err(|e| state.db_fail("maintenance records create", &e))?
        .ok_or(StatusCode::BAD_REQUEST)?;
    Ok((StatusCode::CREATED, Json(r)))
}

async fn get_record(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Record>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("maintenance records get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_record(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateRecord>,
) -> Result<Json<Record>, StatusCode> {
    let mut pg = state.pg.lock().await;
    match update(&mut pg, tenant_id, id, body)
        .await
        .map_err(|e| state.db_fail("maintenance records update", &e))?
    {
        Updated::Done(r) => Ok(Json(*r)),
        Updated::NotFound => Err(StatusCode::NOT_FOUND),
        Updated::BadRef => Err(StatusCode::BAD_REQUEST),
    }
}

async fn delete_record(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = soft_delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("maintenance records delete", &e))?;
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
    fn paging_clamps_like_rust() {
        assert_eq!(paging(None, None), (1, 20));
        assert_eq!(paging(Some(0), Some(0)), (1, 1));
        assert_eq!(paging(Some(3), Some(1000)), (3, 100));
    }
}
