# alc-carins-worker

車検証 (carins) と車両整備 (maintenance) の Worker `alc-carins` (Refs ippoan/rust-alc-api#680)。rust-alc-api の
`crates/alc-carins`・`crates/alc-maintenance` を移す先で、path・method・JSON の形・status は rust と同じ
(nuxt-pwa-carins・alc-app・nuxt-trouble・nuxt-vehicle-maintenance・smb の取り込みがそのまま使う)。
口は `/api/` の下: `car-inspections/{current,expired,vehicle-categories,by-car/{car_id}/history,lookup}`
(`crates/carins/src/car_inspections.rs`)・`files`・`files/recent`・`files/{uuid}/download`・`files/verify` (`files.rs`。
登録した JSON / PDF は `parse.rs` が読む)・`nfc-tags` (`nfc_tags.rs`)・`maintenance/{vehicles,categories,records,files}`
(`maintenance/`)。直下 = Worker (wasm 専用)、`crates/carins/` = 口・SQL。

rust の口のうち呼び手の無い 7 本は移さない: `car-inspection-files/current`・`car-inspections/renew`・`car-inspections/{id}`・
`files/not-attached`・`GET files/{uuid}`・`files/{uuid}/delete`・`files/{uuid}/restore`。

**呼び手は auth-worker の Service Binding だけ** (auth-worker が JWT を検証し `X-Tenant-ID` を付け直す。この Worker は
認証しない)。auth-worker の振り分け (`ALC_BINDING_ROUTES`)・SmbIngest の行き先は別の PR。

## コマンド
```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh   # 版は scripts/ALC_MIGRATIONS_REV
CARINS_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace
worker-build --release && npx wrangler@4.144.0 deploy --dry-run
```
DB テストは使い捨ての postgres:17 へ。URL が無ければ失敗 (skip しない)。接続ロールは `alc_api_rt`。
準備は取り込みのぶつかりを作る trigger (`RACE-*`) も足すので、本物の DB に向けない。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`CARINS_HYPERDRIVE`) は本番だけ。`wrangler.toml` の表の順を変えない (新しい表は placement より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id・管理番号・ファイル名・行の中身を出さない。
  DB の失敗は `carins: <段> failed: db (<kind>)`、R2 は `carins: <段> failed: r2 (<段の名前>)`。応答の本文は rust と同じく空。
- R2 は 2 つ: `CARINS_R2` (車検証のファイル。キー `{tenant}/{uuid}`) と `MAINTENANCE_R2` (整備の添付。rust の既定の
  共有 bucket に `{tenant}/maintenance/{record}/{uuid}.{ext}` で相乗り)。置き場は `store::FileStore` (Worker は
  `src/r2.rs`、テストはモック)。R2 の await を `tenant_tx` の中に入れない。staging は本番の bucket を指さない (check-exposure.sh)。
  Content-Disposition は rust と違い ASCII だけ (`store::content_disposition`)。
- 車検証の PDF は pdf-extract (rust と同じ版) で読む。壊れた PDF で panic することがある (rust も同じ。Worker は
  wasm を作り直すので、その 1 リクエストだけが落ちる)。読めなくても登録の口は 201 (rust と同じ)。
- role は見ない (rust も `X-Tenant-ID` だけ)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない
  (JSON の値は `JsonOut` で包む)。WHERE に tenant_id も明示する。SQL の意味は rust の repo と同じにする。
- 照合の SQL (`lookup.rs`) は ippoan/alc-measurements-worker にも写しが在る。変えたらそちらも揃える。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 12・carins_db 9・maintenance_db 9) はテストを足したら上げる。
- public repo: ホスト名・tenant UUID・個人情報 (車検証の中身) を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-trouble-worker と同じ rev)。
