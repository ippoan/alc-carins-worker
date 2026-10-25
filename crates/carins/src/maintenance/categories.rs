//! 整備カテゴリ (rust の `alc-maintenance` の categories.rs と `alc_core::master_data`・`master_handlers` の写し)。
//!
//! - `GET /maintenance/categories` — 並びは `sort_order, name`。1 行も無ければ既定の 5 行を作って返す
//!   (1 行ずつ作り、作れなかった行は飛ばす)
//! - `POST /maintenance/categories` — 201。名前が空白だけなら 400、制約違反 (同じ名前) は 409
//! - `PUT /maintenance/categories/{id}` — sort_order だけ変える (無ければ 404)
//! - `DELETE /maintenance/categories/{id}` — 204 / 404

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::AppState;

/// rust の `DEFAULT_CATEGORIES`
pub const DEFAULT_CATEGORIES: &[&str] =
    &["定期点検", "修理", "部品交換", "タイヤ交換", "オイル交換"];

/// rust の `MaintenanceCategory`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Category {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub sort_order: i32,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for Category {}

/// rust の `CreateMaintenanceCategory`
#[derive(Debug, Deserialize)]
pub struct CreateCategory {
    pub name: String,
    pub sort_order: Option<i32>,
}

/// rust の `master_handlers::UpdateSortOrder`
#[derive(Debug, Deserialize)]
pub struct UpdateSortOrder {
    pub sort_order: i32,
}

pub mod sql {
    pub const LIST: &str = "SELECT id, tenant_id, name, sort_order, created_at \
         FROM alc_api.maintenance_categories WHERE tenant_id = $1 ORDER BY sort_order, name";

    pub const INSERT: &str =
        "INSERT INTO alc_api.maintenance_categories (tenant_id, name, sort_order) \
         VALUES ($1, $2, $3) RETURNING id, tenant_id, name, sort_order, created_at";

    pub const DELETE: &str =
        "DELETE FROM alc_api.maintenance_categories WHERE id = $1 AND tenant_id = $2";

    pub const UPDATE_SORT_ORDER: &str =
        "UPDATE alc_api.maintenance_categories SET sort_order = $3 \
         WHERE id = $1 AND tenant_id = $2 RETURNING id, tenant_id, name, sort_order, created_at";
}

fn category(r: &Row) -> Category {
    Category {
        id: r.get(0),
        tenant_id: r.get(1),
        name: r.get(2),
        sort_order: r.get(3),
        created_at: r.get(4),
    }
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Category>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(category).collect())
        })
    })
    .await
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    name: String,
    sort_order: Option<i32>,
) -> Result<Category, Error> {
    let sort_order = sort_order.unwrap_or(0);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&name, Type::TEXT),
                        (&sort_order, Type::INT4),
                    ],
                )
                .await?;
            Ok(category(&row))
        })
    })
    .await
}

pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(sql::DELETE, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub async fn update_sort_order(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    sort_order: i32,
) -> Result<Option<Category>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE_SORT_ORDER,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&sort_order, Type::INT4),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(category))
        })
    })
    .await
}

/// 制約の名前が付いた DB のエラーか (rust の `master_handlers::is_conflict`)。409 にする
fn is_conflict(e: &Error) -> bool {
    e.as_db_error().is_some_and(|db| db.constraint().is_some())
}

pub fn router() -> axum::Router<AppState> {
    Router::new()
        .route(
            "/maintenance/categories",
            get(list_categories).post(create_category),
        )
        .route(
            "/maintenance/categories/{id}",
            axum::routing::delete(delete_category).put(update_category_sort),
        )
}

async fn list_categories(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<Category>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let items = list(&mut pg, tenant_id)
        .await
        .map_err(|e| state.db_fail("maintenance categories list", &e))?;
    if !items.is_empty() {
        return Ok(Json(items));
    }
    let mut seeded = Vec::new();
    for (i, name) in DEFAULT_CATEGORIES.iter().enumerate() {
        match create(&mut pg, tenant_id, (*name).to_owned(), Some(i as i32 + 1)).await {
            Ok(c) => seeded.push(c),
            Err(e) => {
                state.db_fail("maintenance categories auto-seed", &e);
            }
        }
    }
    Ok(Json(seeded))
}

async fn create_category(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateCategory>,
) -> Result<(StatusCode, Json<Category>), StatusCode> {
    if body.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let c = create(&mut pg, tenant_id, body.name, body.sort_order)
        .await
        .map_err(|e| {
            if is_conflict(&e) {
                return StatusCode::CONFLICT;
            }
            state.db_fail("maintenance categories create", &e)
        })?;
    Ok((StatusCode::CREATED, Json(c)))
}

async fn update_category_sort(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSortOrder>,
) -> Result<Json<Category>, StatusCode> {
    let mut pg = state.pg.lock().await;
    update_sort_order(&mut pg, tenant_id, id, body.sort_order)
        .await
        .map_err(|e| state.db_fail("maintenance categories update", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_category(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("maintenance categories delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
