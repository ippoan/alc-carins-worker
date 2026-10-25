//! 車検証 (carins) と車両整備 (maintenance) の口 (Refs ippoan/rust-alc-api#680)。rust-alc-api の `crates/alc-carins`・
//! `crates/alc-maintenance` を移す先で、path・method・JSON の形・status は rust と同じ (nuxt-pwa-carins・alc-app・
//! nuxt-trouble・nuxt-vehicle-maintenance・smb の取り込みがそのまま使う)。
//!
//! - [`car_inspections`] — 車検証の一覧 (現在・期限の近いもの・車両ごとの履歴)・分類の候補・期限の照合
//! - [`files`] — 車検証のファイル (登録・一覧・取り出し・R2 に在るかの確かめ)。登録した JSON / PDF は [`parse`] が読む
//! - [`nfc_tags`] — NFC タグと車検証の紐づけ
//! - [`maintenance`] — 車両整備 (車両・カテゴリ・整備記録・添付)
//!
//! rust に在って移さない口 (呼び手が無い): `car-inspection-files/current`・`car-inspections/renew`・
//! `car-inspections/{id}`・`files/not-attached`・`GET files/{uuid}`・`files/{uuid}/delete`・`files/{uuid}/restore`。
//!
//! 全部の口が `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401。`alc_core_wasm::require_tenant_header`)。
//! rust と同じく role は見ない。
//! DB は `PgClient::tenant_tx` + `query_typed` 系だけ (名前付き prepared statement は使わない)。WHERE に tenant_id も書く。
//! ログ・エラー応答は固定の語だけ (エラーの文・識別子・番号を載せない。DB のエラーは `kind` = SQLSTATE か固定の label)。

pub mod car_inspections;
pub mod files;
pub mod lookup;
pub mod maintenance;
pub mod nfc_tags;
pub mod parse;
pub mod store;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_worker_db::{kind, PgClient};
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::{middleware, Router};
use futures_util::lock::Mutex;
use tokio_postgres::error::SqlState;

use crate::store::{SharedStore, StoreError};

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
    /// 車検証のファイルの置き場 (R2 の binding `CARINS_R2`)。無ければファイルの読み書きは 503
    pub carins_files: Option<SharedStore>,
    /// 整備記録の添付の置き場 (R2 の binding `MAINTENANCE_R2`)。無ければ添付の読み書きは 503 (rust の storage 無しと同じ)
    pub maintenance_files: Option<SharedStore>,
}

impl AppState {
    /// DB の失敗を固定の語でログに出し、500 にする
    fn db_fail(&self, stage: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("carins: {stage} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }

    /// R2 の失敗を固定の語でログに出し、500 にする
    fn store_fail(&self, stage: &str, e: StoreError) -> StatusCode {
        (self.log)(&format!("carins: {stage} failed: r2 ({})", e.0));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// `to_jsonb` などの JSON の 1 値を transaction の外へ持ち出す印 (`serde_json::Value` は外部の型で、
/// `TxOutput` を直に付けられないため)
pub struct JsonOut(pub serde_json::Value);

impl alc_worker_db::TxOutput for JsonOut {}

/// 一意制約の違反か (rust の `is_unique_violation` と同じ SQLSTATE 23505)。呼び手は 409 にする
pub fn is_unique_violation(e: &tokio_postgres::Error) -> bool {
    e.code() == Some(&SqlState::UNIQUE_VIOLATION)
}

/// 本文の上限 (rust の main.rs の `DefaultBodyLimit` と同じ 20MB。車検証の登録の JSON と添付の multipart に効く)
pub const BODY_LIMIT: usize = 20 * 1024 * 1024;

/// 全部の口。rust と同じく `/api` の下 (auth-worker は `/api/...` のまま Service Binding に渡す)
pub fn router() -> Router<AppState> {
    let api = Router::new()
        .merge(car_inspections::router())
        .merge(files::router())
        .merge(nfc_tags::router())
        .merge(maintenance::router())
        .route_layer(middleware::from_fn(require_tenant_header));
    Router::new()
        .nest("/api", api)
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}
