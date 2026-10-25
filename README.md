# alc-carins-worker

車検証 (carins) と車両整備 (maintenance) の Cloudflare Worker `alc-carins`。rust-alc-api の `crates/alc-carins`・
`crates/alc-maintenance` から分割する (Refs ippoan/rust-alc-api#680)。型は ippoan/alc-trouble-worker と同じ。

## 口

全部の口に `X-Tenant-ID` が要る (auth-worker が付ける。無い・UUID でない → 401)。path・method・JSON の形・status は rust-alc-api と同じ。

| path | method | 内容 |
|---|---|---|
| `/api/car-inspections/current` | GET | 車両ごとに期限の最も新しい車検証 (`pdfUuid`・`jsonUuid` 付き) |
| `/api/car-inspections/expired` | GET | 期限が 30 日以内 (過ぎたものを含む) の車検証 |
| `/api/car-inspections/by-car/{car_id}/history` | GET | 1 台の車検証を新しい順 |
| `/api/car-inspections/vehicle-categories` | GET | 車種・用途・形状・自家用/事業用の候補 |
| `/api/car-inspections/lookup` | POST | 管理番号 / 車両 ID で車検期限を照合 (所有者・住所・車台番号は返さない) |
| `/api/files` | GET / POST | 車検証のファイルの一覧 (`?type=`) と登録 (`{filename, type, content(base64)}`、201) |
| `/api/files/recent` | GET | 新しい 50 件 |
| `/api/files/{uuid}/download` | GET | 本体 (R2。古い行は DB の base64) |
| `/api/files/verify` | POST | `{uuids}` の各ファイルが R2 に在るか |
| `/api/nfc-tags` | GET / POST | NFC タグの一覧 (`?car_inspection_id=`) と登録 (201) |
| `/api/nfc-tags/search` | GET | `?uuid=` のタグと車検証 |
| `/api/nfc-tags/{nfc_uuid}` | DELETE | タグの削除 |
| `/api/maintenance/vehicles[/{id}]` | GET / POST / PUT / DELETE | 整備の車両 (一覧の絞り込み・頁、論理削除) |
| `/api/maintenance/vehicles/{id}/carins` | PUT / DELETE | 車検証との紐づけ・外し |
| `/api/maintenance/vehicles/{id}/carins-candidates` | GET | 登録番号が一致しそうな車検証 |
| `/api/maintenance/vehicles/carins-import-candidates` | GET | まだ車両になっていない車検証 |
| `/api/maintenance/vehicles/carins-import` | POST | 選んだ車検証の取り込み (件数だけ返す) |
| `/api/maintenance/categories[/{id}]` | GET / POST / PUT / DELETE | 整備カテゴリ。1 行も無ければ既定の 5 行を作って返す |
| `/api/maintenance/records[/{id}]` | GET / POST / PUT / DELETE | 整備記録 (一覧の絞り込み・頁、論理削除) |
| `/api/maintenance/records/{id}/files` | GET / POST | 整備記録の添付の一覧と、multipart の 1 つを R2 に置く (201) |
| `/api/maintenance/files/{id}`[`/download`] | DELETE / GET | 添付の論理削除・本体 |

rust の口のうち呼び手の無い 7 本は移さない: `car-inspection-files/current`・`car-inspections/renew`・`car-inspections/{id}`・
`files/not-attached`・`GET files/{uuid}`・`files/{uuid}/delete`・`files/{uuid}/restore`。

### 車検証のファイルの登録

`POST /api/files` は R2 の binding `CARINS_R2` (本番 `carins-files`・staging `carins-files-staging`、キーは rust と同じ
`{tenant}/{uuid}`) に置き、在ることを確かめてから行を作る。`type` が `application/json` なら電子車検証の JSON として
car_inspection に upsert し、`application/pdf` なら 1 ページ目の文字から管理番号と交付日を取って JSON と紐づける
(JSON がまだ無ければ待ちに入れ、JSON が来たときに紐づける)。読めなくても 201 (rust と同じ)。

**rust と違う** ところ:
- 一覧の口は、確かめていないファイルを裏で確かめない (画面が `files/verify` を呼ぶ)
- 登録の応答の `storageVerified` は `true` (rust は登録の直後の NULL を返していた。残る行は同じ)
- `{uuid}` が UUID でなければ 400 (rust は 500)。`files/verify` の UUID でない値は結果に入れない (rust は 500)
- JSON の upsert と紐づけを 1 つの transaction にした

### 整備記録の添付

R2 の binding `MAINTENANCE_R2` (本番 `alc-face-photos`・staging `alc-face-photos-staging`。rust の既定の共有 bucket に
`{tenant}/maintenance/{record}/{uuid}.{ext}` で相乗りしている) に置く。binding が無ければ添付の読み書きは 503。上限は rust と同じ 20MB。

`Content-Disposition` は **rust と違う**: ASCII だけで `attachment; filename*=UTF-8''<percent-encoding>; filename="<ASCII の代替>"`
(Workers が非 ASCII のヘッダ値を値入りの警告にしてログに出すため)。

## 構成

- 直下 = Worker (`src/lib.rs`・`src/db.rs`・`src/r2.rs`。wasm 専用)
- `crates/carins/` = 口・SQL (native でも wasm でもビルドでき、テストと coverage の対象)
- DB: 本番は Hyperdrive (`CARINS_HYPERDRIVE`)、staging は Workers VPC (`CARINS_DB_VPC`) の先の PgBouncer
- デプロイ: main への merge = staging、Tag Release (手動、`v*`) = 本番
