//! 車両整備の口 (車両・カテゴリ・整備記録・添付) を、実 DB (`alc_api_rt` で接続) と R2 のモックで通す。
//! 期待値は rust-alc-api の tests/maintenance_*_test.rs に合わせる。
//! 接続先は `CARINS_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use std::sync::atomic::Ordering;

use axum::http::header;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const VEHICLE_KEYS: &[&str] = &[
    "car_id",
    "carins_linked_at",
    "created_at",
    "deleted_at",
    "display_name",
    "id",
    "note",
    "registration_number",
    "tenant_id",
    "updated_at",
];

const RECORD_KEYS: &[&str] = &[
    "category_id",
    "cost",
    "created_at",
    "created_by",
    "deleted_at",
    "description",
    "id",
    "next_due_on",
    "odometer_km",
    "performed_on",
    "tenant_id",
    "updated_at",
    "vehicle_id",
    "vendor",
];

const FILE_KEYS: &[&str] = &[
    "content_type",
    "created_at",
    "deleted_at",
    "filename",
    "id",
    "record_id",
    "size_bytes",
    "storage_key",
    "tenant_id",
];

fn keys(v: &Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

async fn ok(app: &App, method: &str, uri: &str, t: Uuid, body: Option<Value>, want: u16) -> Value {
    let got = call(app, method, uri, Some(t), body).await;
    assert_eq!(
        got.status,
        want,
        "{method} {uri}: {}",
        String::from_utf8_lossy(&got.body)
    );
    if got.body.is_empty() {
        Value::Null
    } else {
        got.json()
    }
}

async fn status(app: &App, method: &str, uri: &str, t: Uuid, body: Option<Value>) -> u16 {
    call(app, method, uri, Some(t), body).await.status
}

async fn vehicle(app: &App, t: Uuid, reg: &str) -> String {
    let v = ok(
        app,
        "POST",
        "/api/maintenance/vehicles",
        t,
        Some(json!({"registration_number": reg})),
        201,
    )
    .await;
    v["id"].as_str().unwrap().to_owned()
}

/// 車検証の JSON を登録して car_inspection の行を作る (口を通す)
async fn inspection(
    app: &App,
    t: Uuid,
    mg_no: &str,
    car_id: &str,
    d: &str,
    entry_no: &str,
    car_no: &str,
) {
    let v = json!({"CertInfo": {
        "ElectCertMgNo": mg_no, "CarId": car_id, "GrantdateE": "令和", "GrantdateY": "8", "GrantdateM": "2",
        "GrantdateD": d, "TwodimensionCodeInfoValidPeriodExpirdate": "991231", "EntryNoCarNo": entry_no, "CarNo": car_no
    }});
    let body = json!({"filename": "c.json", "type": "application/json", "content": STANDARD.encode(v.to_string())});
    ok(app, "POST", "/api/files", t, Some(body), 201).await;
}

#[tokio::test]
async fn vehicles_crud_and_list() {
    let app = app().await;
    let t = tenant().await;
    let v = ok(
        &app,
        "POST",
        "/api/maintenance/vehicles",
        t,
        Some(
            json!({"registration_number": "品川500あ1234", "display_name": "1 号車", "note": "n"}),
        ),
        201,
    )
    .await;
    assert_eq!(keys(&v), VEHICLE_KEYS);
    assert_eq!(v["tenant_id"], t.to_string());
    let id = v["id"].as_str().unwrap().to_owned();
    vehicle(&app, t, "足立100い1").await;
    ok(
        &app,
        "POST",
        "/api/maintenance/vehicles",
        t,
        Some(json!({"registration_number": "練馬", "car_id": "CARID000000099"})),
        201,
    )
    .await;
    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/vehicles",
            t,
            Some(json!({"registration_number": " "}))
        )
        .await,
        400
    );
    // 同じ car_id の車両 → 409
    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/vehicles",
            t,
            Some(json!({"registration_number": "x", "car_id": "CARID000000099"}))
        )
        .await,
        409
    );

    let list = ok(&app, "GET", "/api/maintenance/vehicles", t, None, 200).await;
    assert_eq!(
        (
            list["total"].clone(),
            list["page"].clone(),
            list["per_page"].clone()
        ),
        (json!(3), json!(1), json!(20))
    );
    assert_eq!(list["items"][0]["registration_number"], "練馬");
    let found = ok(
        &app,
        "GET",
        "/api/maintenance/vehicles?q=1%20%E5%8F%B7",
        t,
        None,
        200,
    )
    .await;
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    let linked = ok(
        &app,
        "GET",
        "/api/maintenance/vehicles?linked=true",
        t,
        None,
        200,
    )
    .await;
    assert_eq!(linked["total"], 1);
    let page = ok(
        &app,
        "GET",
        "/api/maintenance/vehicles?linked=false&page=2&per_page=1",
        t,
        None,
        200,
    )
    .await;
    assert_eq!(
        (
            page["total"].clone(),
            page["items"].as_array().unwrap().len()
        ),
        (json!(2), 1)
    );

    let uri = format!("/api/maintenance/vehicles/{id}");
    assert_eq!(
        ok(&app, "GET", &uri, t, None, 200).await["display_name"],
        "1 号車"
    );
    let updated = ok(&app, "PUT", &uri, t, Some(json!({"note": "更新"})), 200).await;
    assert_eq!(
        (updated["note"].clone(), updated["display_name"].clone()),
        (json!("更新"), json!("1 号車"))
    );
    assert_eq!(
        status(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({"registration_number": ""}))
        )
        .await,
        400
    );
    // 別の会社からは見えない
    let other = tenant().await;
    assert_eq!(status(&app, "GET", &uri, other, None).await, 404);
    assert_eq!(status(&app, "PUT", &uri, other, Some(json!({}))).await, 404);
    assert_eq!(status(&app, "DELETE", &uri, other, None).await, 404);
    assert_eq!(status(&app, "DELETE", &uri, t, None).await, 204);
    assert_eq!(status(&app, "GET", &uri, t, None).await, 404);
    assert_eq!(status(&app, "DELETE", &uri, t, None).await, 404);
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn link_unlink_and_candidates() {
    let app = app().await;
    let t = tenant().await;
    inspection(
        &app,
        t,
        "700000000001",
        "CARID000000101",
        "1",
        "品川500あ1234",
        "",
    )
    .await;
    inspection(&app, t, "700000000002", "", "1", "", "").await;
    let a = vehicle(&app, t, "品川５００あ１２３４").await;
    let b = vehicle(&app, t, "足立").await;
    let link = |id: &str| format!("/api/maintenance/vehicles/{id}/carins");

    // 候補 (全角を半角にして車検証の登録番号を含むか)
    let c = ok(
        &app,
        "GET",
        &format!("/api/maintenance/vehicles/{a}/carins-candidates"),
        t,
        None,
        200,
    )
    .await;
    assert_eq!(
        c,
        json!([{"car_id": "CARID000000101", "cert_no": "700000000001", "car_no": "品川500あ1234"}])
    );
    assert_eq!(
        ok(
            &app,
            "GET",
            &format!("/api/maintenance/vehicles/{b}/carins-candidates"),
            t,
            None,
            200
        )
        .await,
        json!([])
    );
    let blank = Uuid::new_v4();
    admin()
        .await
        .execute("INSERT INTO alc_api.maintenance_vehicles (id, tenant_id, registration_number) VALUES ($1, $2, ' ')", &[&blank, &t])
        .await
        .unwrap();
    assert_eq!(
        ok(
            &app,
            "GET",
            &format!("/api/maintenance/vehicles/{blank}/carins-candidates"),
            t,
            None,
            200
        )
        .await,
        json!([])
    );
    assert_eq!(
        status(
            &app,
            "GET",
            &format!(
                "/api/maintenance/vehicles/{}/carins-candidates",
                Uuid::new_v4()
            ),
            t,
            None
        )
        .await,
        404
    );

    // 管理番号だけで紐づけ (CarId を引く)
    let v = ok(
        &app,
        "PUT",
        &link(&a),
        t,
        Some(json!({"cert_no": "700000000001"})),
        200,
    )
    .await;
    assert_eq!(v["car_id"], "CARID000000101");
    assert!(!v["carins_linked_at"].is_null());
    // 別の車両に同じ car_id → 409
    assert_eq!(
        status(
            &app,
            "PUT",
            &link(&b),
            t,
            Some(json!({"car_id": "CARID000000101"}))
        )
        .await,
        409
    );
    // 一致しない・CarId が空・形が違う → 400、車両が無い → 404
    assert_eq!(
        status(
            &app,
            "PUT",
            &link(&b),
            t,
            Some(json!({"cert_no": "799999999999"}))
        )
        .await,
        400
    );
    assert_eq!(
        status(
            &app,
            "PUT",
            &link(&b),
            t,
            Some(json!({"cert_no": "700000000002"}))
        )
        .await,
        400
    );
    assert_eq!(
        status(&app, "PUT", &link(&b), t, Some(json!({"car_id": "x"}))).await,
        400
    );
    assert_eq!(
        status(&app, "PUT", &link(&b), t, Some(json!({}))).await,
        400
    );
    assert_eq!(
        status(
            &app,
            "PUT",
            &link(&Uuid::new_v4().to_string()),
            t,
            Some(json!({"car_id": "CARID000000101"}))
        )
        .await,
        404
    );

    // 外して、車両 ID で付け直す
    assert_eq!(status(&app, "DELETE", &link(&a), t, None).await, 204);
    assert!(ok(
        &app,
        "GET",
        &format!("/api/maintenance/vehicles/{a}"),
        t,
        None,
        200
    )
    .await["car_id"]
        .is_null());
    let v = ok(
        &app,
        "PUT",
        &link(&b),
        t,
        Some(json!({"car_id": "CARID000000101"})),
        200,
    )
    .await;
    assert_eq!(v["car_id"], "CARID000000101");
    assert_eq!(
        status(&app, "DELETE", &link(&Uuid::new_v4().to_string()), t, None).await,
        404
    );
}

#[tokio::test]
async fn carins_import_links_creates_and_skips() {
    let app = app().await;
    let t = tenant().await;
    // linked: 登録番号の一致する未紐づけの車両が在る / created: 無い / taken: 一致する車両が別の車検証に紐づき済み
    inspection(&app, t, "800000000001", "CARID000000201", "1", "品川1", "").await;
    inspection(&app, t, "800000000002", "CARID000000202", "1", "", "足立2").await;
    inspection(&app, t, "800000000003", "CARID000000203", "1", "練馬3", "").await;
    inspection(&app, t, "800000000004", "CARID000000204", "1", "", "").await;
    vehicle(&app, t, "品川１").await;
    let taken = vehicle(&app, t, "練馬3").await;
    admin()
        .await
        .execute("UPDATE alc_api.maintenance_vehicles SET car_id = 'CARID000000299' WHERE id = $1::text::uuid", &[&taken])
        .await
        .unwrap();

    let candidates = ok(
        &app,
        "GET",
        "/api/maintenance/vehicles/carins-import-candidates",
        t,
        None,
        200,
    )
    .await;
    let cs = candidates.as_array().unwrap();
    assert_eq!(cs.len(), 3);
    assert_eq!(cs[0]["car_id"], "CARID000000201");
    assert!(!cs[0]["existing_vehicle_id"].is_null());
    assert!(cs[1]["existing_vehicle_id"].is_null());

    let body = json!({"car_ids": ["CARID000000201", "CARID000000202", "CARID000000203", "CARID000000204", " ", "CARID000000999"]});
    let got = ok(
        &app,
        "POST",
        "/api/maintenance/vehicles/carins-import",
        t,
        Some(body.clone()),
        200,
    )
    .await;
    assert_eq!(got, json!({"created": 1, "linked": 1, "skipped": 4}));
    // もう一度は全部飛ばす (冪等)
    let got = ok(
        &app,
        "POST",
        "/api/maintenance/vehicles/carins-import",
        t,
        Some(body),
        200,
    )
    .await;
    assert_eq!(got, json!({"created": 0, "linked": 0, "skipped": 6}));
    assert_eq!(
        ok(
            &app,
            "GET",
            "/api/maintenance/vehicles?linked=true",
            t,
            None,
            200
        )
        .await["total"],
        3
    );

    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/vehicles/carins-import",
            t,
            Some(json!({"car_ids": []}))
        )
        .await,
        400
    );
    let many: Vec<String> = (0..501).map(|i| format!("{i}")).collect();
    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/vehicles/carins-import",
            t,
            Some(json!({"car_ids": many}))
        )
        .await,
        400
    );
}

#[tokio::test]
async fn carins_import_survives_concurrent_writes() {
    // tests/support の trigger: RACE-DUP は同じ car_id の行を先に作られて一意制約の違反、RACE-NONE は 0 行、RACE-BOOM は例外
    let app = app().await;
    let t = tenant().await;
    inspection(
        &app,
        t,
        "900000000001",
        "CARID000000301",
        "1",
        "RACE-DUP-new",
        "",
    )
    .await;
    inspection(
        &app,
        t,
        "900000000002",
        "CARID000000302",
        "1",
        "RACE-DUP-old",
        "",
    )
    .await;
    inspection(
        &app,
        t,
        "900000000003",
        "CARID000000303",
        "1",
        "RACE-NONE-new",
        "",
    )
    .await;
    inspection(
        &app,
        t,
        "900000000004",
        "CARID000000304",
        "1",
        "RACE-NONE-old",
        "",
    )
    .await;
    inspection(&app, t, "900000000005", "CARID000000305", "1", "品川9", "").await;
    for reg in ["RACE-DUP-old", "RACE-NONE-old"] {
        admin()
            .await
            .execute("INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number) VALUES ($1, $2)", &[&t, &reg])
            .await
            .unwrap();
    }
    let body = json!({"car_ids": ["CARID000000301", "CARID000000302", "CARID000000303", "CARID000000304", "CARID000000305"]});
    let got = ok(
        &app,
        "POST",
        "/api/maintenance/vehicles/carins-import",
        t,
        Some(body),
        200,
    )
    .await;
    // ぶつかった 4 件は戻して飛ばし、ほかの行は取り込む
    assert_eq!(got, json!({"created": 1, "linked": 0, "skipped": 4}));

    // 一意制約の違反でない失敗は 500 (取り込みは全部戻る)
    inspection(
        &app,
        t,
        "900000000006",
        "CARID000000306",
        "1",
        "RACE-BOOM",
        "",
    )
    .await;
    inspection(&app, t, "900000000007", "CARID000000307", "1", "品川10", "").await;
    let body = json!({"car_ids": ["CARID000000307", "CARID000000306"]});
    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/vehicles/carins-import",
            t,
            Some(body)
        )
        .await,
        500
    );
    assert_eq!(
        app.logs(),
        ["carins: maintenance vehicles import failed: db (P0001)"]
    );
    let n: i64 = admin()
        .await
        .query_one("SELECT count(*) FROM alc_api.maintenance_vehicles WHERE tenant_id = $1 AND car_id = 'CARID000000307'", &[&t])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}

#[tokio::test]
async fn categories_seed_create_sort_delete() {
    let app = app().await;
    let t = tenant().await;
    let seeded = ok(&app, "GET", "/api/maintenance/categories", t, None, 200).await;
    let names: Vec<&str> = seeded
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["定期点検", "修理", "部品交換", "タイヤ交換", "オイル交換"]
    );
    assert_eq!(
        keys(&seeded[0]),
        ["created_at", "id", "name", "sort_order", "tenant_id"]
    );
    assert_eq!(
        ok(&app, "GET", "/api/maintenance/categories", t, None, 200)
            .await
            .as_array()
            .unwrap()
            .len(),
        5
    );

    let c = ok(
        &app,
        "POST",
        "/api/maintenance/categories",
        t,
        Some(json!({"name": "洗車"})),
        201,
    )
    .await;
    assert_eq!(c["sort_order"], 0);
    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/categories",
            t,
            Some(json!({"name": " "}))
        )
        .await,
        400
    );
    assert_eq!(
        status(
            &app,
            "POST",
            "/api/maintenance/categories",
            t,
            Some(json!({"name": "洗車"}))
        )
        .await,
        409
    );
    let uri = format!("/api/maintenance/categories/{}", c["id"].as_str().unwrap());
    assert_eq!(
        ok(&app, "PUT", &uri, t, Some(json!({"sort_order": 9})), 200).await["sort_order"],
        9
    );
    let other = tenant().await;
    assert_eq!(
        status(&app, "PUT", &uri, other, Some(json!({"sort_order": 1}))).await,
        404
    );
    assert_eq!(status(&app, "DELETE", &uri, other, None).await, 404);
    assert_eq!(status(&app, "DELETE", &uri, t, None).await, 204);
    assert_eq!(status(&app, "DELETE", &uri, t, None).await, 404);

    // 既定の行を作れない (書けないロール) → 空を返し、ログだけ
    let ro = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(
        ok(
            &ro,
            "GET",
            "/api/maintenance/categories",
            tenant().await,
            None,
            200
        )
        .await,
        json!([])
    );
    assert_eq!(ro.logs().len(), 5);
    assert!(ro
        .logs()
        .iter()
        .all(|l| l == "carins: maintenance categories auto-seed failed: db (42501)"));
}

#[tokio::test]
async fn records_crud_filters_and_tenant_checks() {
    let app = app().await;
    let t = tenant().await;
    let v1 = vehicle(&app, t, "品川1").await;
    let v2 = vehicle(&app, t, "品川2").await;
    let cats = ok(&app, "GET", "/api/maintenance/categories", t, None, 200).await;
    let c1 = cats[0]["id"].as_str().unwrap().to_owned();
    let c2 = cats[1]["id"].as_str().unwrap().to_owned();
    let other = tenant().await;
    let ov = vehicle(&app, other, "他社").await;
    let oc = ok(&app, "GET", "/api/maintenance/categories", other, None, 200).await[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let r = ok(
        &app,
        "POST",
        "/api/maintenance/records",
        t,
        Some(json!({"vehicle_id": v1, "category_id": c1, "performed_on": "2026-10-01", "odometer_km": 120000,
                    "vendor": "東京整備", "description": "12 か月点検", "cost": 12345.6, "next_due_on": "2027-10-01"})),
        201,
    )
    .await;
    assert_eq!(keys(&r), RECORD_KEYS);
    assert_eq!(
        (r["cost"].clone(), r["performed_on"].clone()),
        (json!("12345.60"), json!("2026-10-01"))
    );
    assert!(r["created_by"].is_null());
    ok(
        &app,
        "POST",
        "/api/maintenance/records",
        t,
        Some(json!({"vehicle_id": v2, "category_id": c2, "performed_on": "2026-09-01"})),
        201,
    )
    .await;
    // 別の会社の車両・カテゴリ → 400
    for body in [
        json!({"vehicle_id": ov, "category_id": c1, "performed_on": "2026-10-01"}),
        json!({"vehicle_id": v1, "category_id": oc, "performed_on": "2026-10-01"}),
    ] {
        assert_eq!(
            status(&app, "POST", "/api/maintenance/records", t, Some(body)).await,
            400
        );
    }

    let list = |q: &str| {
        let app = &app;
        let uri = format!("/api/maintenance/records{q}");
        async move { ok(app, "GET", &uri, t, None, 200).await }
    };
    let all = list("").await;
    assert_eq!(
        (
            all["total"].clone(),
            all["records"][0]["performed_on"].clone()
        ),
        (json!(2), json!("2026-10-01"))
    );
    assert_eq!(list(&format!("?vehicle_id={v2}")).await["total"], 1);
    assert_eq!(list(&format!("?category_id={c1}")).await["total"], 1);
    assert_eq!(list("?date_from=2026-09-15").await["total"], 1);
    assert_eq!(list("?date_to=2026-09-15").await["total"], 1);
    assert_eq!(list("?q=%E6%9D%B1%E4%BA%AC").await["total"], 1);
    assert_eq!(list("?q=%E7%82%B9%E6%A4%9C").await["total"], 1);
    let page = list("?page=2&per_page=1").await;
    assert_eq!(
        (
            page["page"].clone(),
            page["records"].as_array().unwrap().len()
        ),
        (json!(2), 1)
    );

    let uri = format!("/api/maintenance/records/{}", r["id"].as_str().unwrap());
    assert_eq!(
        ok(&app, "GET", &uri, t, None, 200).await["vendor"],
        "東京整備"
    );
    let u = ok(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({"vehicle_id": v2, "cost": 100})),
        200,
    )
    .await;
    assert_eq!(
        (
            u["vehicle_id"].clone(),
            u["cost"].clone(),
            u["vendor"].clone()
        ),
        (json!(v2), json!("100.00"), json!("東京整備"))
    );
    assert_eq!(
        status(&app, "PUT", &uri, t, Some(json!({"vehicle_id": ov}))).await,
        400
    );
    assert_eq!(
        status(&app, "PUT", &uri, t, Some(json!({"category_id": oc}))).await,
        400
    );
    assert_eq!(status(&app, "GET", &uri, other, None).await, 404);
    assert_eq!(status(&app, "PUT", &uri, other, Some(json!({}))).await, 404);
    assert_eq!(status(&app, "DELETE", &uri, t, None).await, 204);
    assert_eq!(status(&app, "GET", &uri, t, None).await, 404);
    assert_eq!(status(&app, "DELETE", &uri, t, None).await, 404);
}

const BOUNDARY: &str = "carins-test-boundary";

fn multipart(filename: Option<&str>, content_type: Option<&str>, data: &[u8]) -> Vec<u8> {
    let mut head = format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"");
    if let Some(f) = filename {
        head.push_str(&format!("; filename=\"{f}\""));
    }
    head.push_str("\r\n");
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    head.push_str("\r\n");
    let mut body = head.into_bytes();
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn upload(app: &App, t: Uuid, uri: &str, body: Vec<u8>) -> Got {
    let ct = format!("multipart/form-data; boundary={BOUNDARY}");
    call_raw(app, "POST", uri, Some(t), Some(&ct), body).await
}

async fn record(app: &App, t: Uuid) -> String {
    let v = vehicle(app, t, "添付").await;
    let c = ok(app, "GET", "/api/maintenance/categories", t, None, 200).await[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let r = ok(
        app,
        "POST",
        "/api/maintenance/records",
        t,
        Some(json!({"vehicle_id": v, "category_id": c, "performed_on": "2026-10-01"})),
        201,
    )
    .await;
    r["id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn record_files_upload_list_download_delete() {
    let app = app().await;
    let t = tenant().await;
    let r = record(&app, t).await;
    let files = format!("/api/maintenance/records/{r}/files");
    assert_eq!(ok(&app, "GET", &files, t, None, 200).await, json!([]));

    let got = upload(
        &app,
        t,
        &files,
        multipart(Some("点検 写真.jpg"), Some("image/jpeg"), b"JPEG"),
    )
    .await;
    assert_eq!(got.status, 201);
    let f = got.json();
    assert_eq!(keys(&f), FILE_KEYS);
    assert_eq!(
        (f["size_bytes"].clone(), f["content_type"].clone()),
        (json!(4), json!("image/jpeg"))
    );
    let key = f["storage_key"].as_str().unwrap();
    assert!(key.starts_with(&format!("{t}/maintenance/{r}/")) && key.ends_with(".jpg"));
    assert_eq!(
        app.maintenance.object(key).unwrap(),
        (b"JPEG".to_vec(), "image/jpeg".to_owned())
    );
    // 名前・Content-Type が無ければ既定の値
    let got = upload(&app, t, &files, multipart(None, None, b"x")).await;
    assert_eq!(
        (
            got.json()["filename"].clone(),
            got.json()["content_type"].clone()
        ),
        (json!("unknown"), json!("application/octet-stream"))
    );
    assert_eq!(
        ok(&app, "GET", &files, t, None, 200)
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let id = f["id"].as_str().unwrap();
    let got = call(
        &app,
        "GET",
        &format!("/api/maintenance/files/{id}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!((got.status, got.body.as_slice()), (200, b"JPEG".as_slice()));
    assert_eq!(got.headers[header::CONTENT_TYPE], "image/jpeg");
    assert!(got.headers[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap()
        .ends_with("; filename=\"__ __.jpg\""));

    // 別の会社・無い記録 → 404
    let other = tenant().await;
    assert_eq!(status(&app, "GET", &files, other, None).await, 404);
    assert_eq!(
        upload(&app, other, &files, multipart(Some("a"), None, b"a"))
            .await
            .status,
        404
    );
    assert_eq!(
        status(
            &app,
            "GET",
            &format!("/api/maintenance/files/{id}/download"),
            other,
            None
        )
        .await,
        404
    );
    assert_eq!(
        status(
            &app,
            "DELETE",
            &format!("/api/maintenance/files/{id}"),
            other,
            None
        )
        .await,
        404
    );

    // 論理削除 → 一覧から消え、落とせない
    assert_eq!(
        status(
            &app,
            "DELETE",
            &format!("/api/maintenance/files/{id}"),
            t,
            None
        )
        .await,
        204
    );
    assert_eq!(
        status(
            &app,
            "DELETE",
            &format!("/api/maintenance/files/{id}"),
            t,
            None
        )
        .await,
        404
    );
    assert_eq!(
        ok(&app, "GET", &files, t, None, 200)
            .await
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        status(
            &app,
            "GET",
            &format!("/api/maintenance/files/{id}/download"),
            t,
            None
        )
        .await,
        404
    );

    // multipart が読めない・空 → 400
    assert_eq!(
        upload(&app, t, &files, b"garbage".to_vec()).await.status,
        400
    );
    assert_eq!(
        upload(&app, t, &files, format!("--{BOUNDARY}--\r\n").into_bytes())
            .await
            .status,
        400
    );
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn record_files_r2_failures() {
    let t = tenant().await;
    let app = app().await;
    let r = record(&app, t).await;
    let files = format!("/api/maintenance/records/{r}/files");
    let id = upload(&app, t, &files, multipart(Some("a.jpg"), None, b"a"))
        .await
        .json()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let download = format!("/api/maintenance/files/{id}/download");

    app.maintenance.fail_put.store(true, Ordering::SeqCst);
    assert_eq!(
        upload(&app, t, &files, multipart(Some("b.jpg"), None, b"b"))
            .await
            .status,
        500
    );
    app.maintenance.fail_get.store(true, Ordering::SeqCst);
    assert_eq!(status(&app, "GET", &download, t, None).await, 500);
    assert_eq!(
        app.logs(),
        [
            "carins: maintenance file upload failed: r2 (put)",
            "carins: maintenance file download failed: r2 (get)"
        ]
    );

    // R2 の binding が無い → 503 (記録の確かめの後)
    let no_store = build_with(rt_client().await, true, false);
    assert_eq!(
        upload(&no_store, t, &files, multipart(Some("c"), None, b"c"))
            .await
            .status,
        503
    );
    assert_eq!(status(&no_store, "GET", &download, t, None).await, 503);
    assert_eq!(
        upload(
            &no_store,
            t,
            &format!("/api/maintenance/records/{}/files", Uuid::new_v4()),
            multipart(None, None, b"")
        )
        .await
        .status,
        404
    );

    // 行を作れない → 500 (R2 には置いた後)
    let ro = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(
        upload(&ro, t, &files, multipart(Some("d"), None, b"d"))
            .await
            .status,
        500
    );
    assert_eq!(
        ro.logs(),
        ["carins: maintenance file create failed: db (42501)"]
    );
}

#[tokio::test]
async fn db_failures_are_500_with_fixed_logs() {
    let app = build(broken_client().await);
    let t = tenant().await;
    let id = Uuid::new_v4();
    let calls: Vec<(&str, String, Option<Value>)> = vec![
        ("GET", "/api/maintenance/vehicles".into(), None),
        (
            "POST",
            "/api/maintenance/vehicles".into(),
            Some(json!({"registration_number": "a"})),
        ),
        ("GET", format!("/api/maintenance/vehicles/{id}"), None),
        (
            "PUT",
            format!("/api/maintenance/vehicles/{id}"),
            Some(json!({})),
        ),
        ("DELETE", format!("/api/maintenance/vehicles/{id}"), None),
        (
            "PUT",
            format!("/api/maintenance/vehicles/{id}/carins"),
            Some(json!({"car_id": "CARID000000001"})),
        ),
        (
            "DELETE",
            format!("/api/maintenance/vehicles/{id}/carins"),
            None,
        ),
        (
            "GET",
            format!("/api/maintenance/vehicles/{id}/carins-candidates"),
            None,
        ),
        (
            "GET",
            "/api/maintenance/vehicles/carins-import-candidates".into(),
            None,
        ),
        (
            "POST",
            "/api/maintenance/vehicles/carins-import".into(),
            Some(json!({"car_ids": ["a"]})),
        ),
        ("GET", "/api/maintenance/categories".into(), None),
        (
            "POST",
            "/api/maintenance/categories".into(),
            Some(json!({"name": "a"})),
        ),
        (
            "PUT",
            format!("/api/maintenance/categories/{id}"),
            Some(json!({"sort_order": 1})),
        ),
        ("DELETE", format!("/api/maintenance/categories/{id}"), None),
        ("GET", "/api/maintenance/records".into(), None),
        (
            "POST",
            "/api/maintenance/records".into(),
            Some(json!({"vehicle_id": id, "category_id": id, "performed_on": "2026-01-01"})),
        ),
        ("GET", format!("/api/maintenance/records/{id}"), None),
        (
            "PUT",
            format!("/api/maintenance/records/{id}"),
            Some(json!({})),
        ),
        ("DELETE", format!("/api/maintenance/records/{id}"), None),
        ("GET", format!("/api/maintenance/records/{id}/files"), None),
        ("GET", format!("/api/maintenance/files/{id}/download"), None),
        ("DELETE", format!("/api/maintenance/files/{id}"), None),
    ];
    let n = calls.len();
    for (method, uri, body) in calls {
        assert_eq!(
            call(&app, method, &uri, Some(t), body).await.status,
            500,
            "{method} {uri}"
        );
    }
    let ct = format!("multipart/form-data; boundary={BOUNDARY}");
    let got = call_raw(
        &app,
        "POST",
        &format!("/api/maintenance/records/{id}/files"),
        Some(t),
        Some(&ct),
        multipart(None, None, b""),
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(app.logs().len(), n + 1);
    assert_logs_clean(&app, &[t.to_string(), id.to_string()]);
    assert!(app.logs().iter().all(|l| l.contains("failed: db (")));
}
