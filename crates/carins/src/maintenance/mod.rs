//! 車両整備 (rust-alc-api の `crates/alc-maintenance` の写し。Refs ippoan/rust-alc-api#651)。nuxt-vehicle-maintenance が呼ぶ。
//!
//! 遠隔点呼の国交省要件 2-四-ト「運行に使用する事業用自動車の整備状況」のための、車両本体の整備記録。
//! 車両 identity の正本は `maintenance_vehicles` で、電子車検証 (`car_id`) は従属する任意のリンク。
//!
//! - [`vehicles`] — 車両 (CRUD・車検証との紐づけ・車検証からの取り込み)
//! - [`categories`] — 整備カテゴリ (空なら既定の行を作る)
//! - [`records`] — 整備記録 (CRUD・一覧の絞り込み)
//! - [`files`] — 整備記録の添付 (R2 の binding `MAINTENANCE_R2`)
//!
//! rust と同じく、WHERE に tenant_id を明示し、別の会社の車両・カテゴリ・記録を指す入力は先に確かめて弾く。

pub mod categories;
pub mod files;
pub mod records;
pub mod vehicles;

use axum::Router;

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .merge(vehicles::router())
        .merge(categories::router())
        .merge(records::router())
        .merge(files::router())
}
