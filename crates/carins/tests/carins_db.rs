//! 車検証の口 (一覧・照合・ファイル・NFC タグ) を、実 DB (`alc_api_rt` で接続) と R2 のモックで通す。
//! 期待値は rust-alc-api の tests/mock_tests/mock_carins_files_test.rs・car_inspections_expiry_test.rs に合わせる。
//! 接続先は `CARINS_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use std::sync::atomic::Ordering;

use axum::http::header;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use carins::parse::{link_pdf, CertKey, Skip};
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const FILE_KEYS: &[&str] = &[
    "accessCountTotal",
    "accessCountWeekly",
    "blob",
    "created",
    "deleted",
    "fileType",
    "filename",
    "lastAccessedAt",
    "promotedToStandardAt",
    "s3Key",
    "storageClass",
    "storageVerified",
    "uuid",
];

/// 電子車検証の JSON (閲覧アプリの出力の形。所有者などは入れない)
fn cert(mg_no: &str, car_id: &str, grant_d: &str, expires_yymmdd: &str, entry_no: &str) -> Value {
    json!({
        "CertInfoImportFileVersion": "2.0",
        "CertInfo": {
            "ElectCertMgNo": mg_no,
            "CarId": car_id,
            "GrantdateE": "令 和",
            "GrantdateY": " 8",
            "GrantdateM": "2",
            "GrantdateD": grant_d,
            "TwodimensionCodeInfoValidPeriodExpirdate": expires_yymmdd,
            "EntryNoCarNo": entry_no,
            "CarNo": "",
            "CarKind": "普通",
            "Use": "貨物",
            "CarShape": "キャブオーバ",
            "PrivateBusiness": "事業用",
            "CarName": "テスト",
            "Cap": 2
        }
    })
}

async fn upload(app: &App, t: Uuid, filename: &str, file_type: &str, data: &[u8]) -> Got {
    call(
        app,
        "POST",
        "/api/files",
        Some(t),
        Some(json!({"filename": filename, "type": file_type, "content": STANDARD.encode(data)})),
    )
    .await
}

async fn upload_json(app: &App, t: Uuid, v: &Value) -> String {
    let got = upload(
        app,
        t,
        "cert.json",
        "application/json",
        &serde_json::to_vec(v).unwrap(),
    )
    .await;
    assert_eq!(got.status, 201, "{}", String::from_utf8_lossy(&got.body));
    got.json()["uuid"].as_str().unwrap().to_owned()
}

async fn get_json(app: &App, t: Uuid, uri: &str) -> Value {
    let got = call(app, "GET", uri, Some(t), None).await;
    assert_eq!(got.status, 200, "{uri}");
    got.json()
}

fn inspections(v: &Value) -> Vec<Value> {
    v["carInspections"].as_array().unwrap().clone()
}

async fn count(sql: &str, t: Uuid) -> i64 {
    admin().await.query_one(sql, &[&t]).await.unwrap().get(0)
}

fn key(mg_no: &str, d: &str) -> CertKey {
    CertKey {
        elect_cert_mg_no: mg_no.to_owned(),
        grantdate: ["令和", "8", "2", d].map(String::from),
    }
}

/// `lines` を 1 行ずつ書いた PDF (Type0 の font + ToUnicode。pdf-extract が日本語のまま読める最小の形)
fn text_pdf(lines: &[&str]) -> Vec<u8> {
    use lopdf::content::{Content, Operation};
    use lopdf::{dictionary, Document, Object, Stream, StringFormat};

    let mut chars: Vec<char> = lines.iter().flat_map(|l| l.chars()).collect();
    chars.sort();
    chars.dedup();
    let cid = |c: char| chars.iter().position(|x| *x == c).unwrap() as u16 + 1;
    let mut cmap = String::from(
        "/CIDInit /ProcSet findresource begin 12 dict begin begincmap\n\
         /CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def\n\
         /CMapName /Adobe-Identity-UCS def /CMapType 2 def\n\
         1 begincodespacerange <0000> <FFFF> endcodespacerange\n",
    );
    cmap.push_str(&format!("{} beginbfchar\n", chars.len()));
    for c in &chars {
        let mut buf = [0u16; 2];
        let utf16: String = c
            .encode_utf16(&mut buf)
            .iter()
            .map(|u| format!("{u:04X}"))
            .collect();
        cmap.push_str(&format!("<{:04X}> <{utf16}>\n", cid(*c)));
    }
    cmap.push_str("endbfchar\nendcmap CMapName currentdict /CMap defineresource pop end end\n");

    let mut doc = Document::with_version("1.5");
    let to_unicode = doc.add_object(Stream::new(dictionary! {}, cmap.into_bytes()));
    let descriptor = doc.add_object(dictionary! {
        "Type" => "FontDescriptor",
        "FontName" => "TestFont",
        "Flags" => 4,
    });
    let cid_font = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "CIDFontType2",
        "BaseFont" => "TestFont",
        "CIDSystemInfo" => dictionary! {
            "Registry" => Object::String(b"Adobe".to_vec(), StringFormat::Literal),
            "Ordering" => Object::String(b"Identity".to_vec(), StringFormat::Literal),
            "Supplement" => 0,
        },
        "DW" => 1000,
        "FontDescriptor" => descriptor,
    });
    let font = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type0",
        "BaseFont" => "TestFont",
        "Encoding" => "Identity-H",
        "DescendantFonts" => vec![cid_font.into()],
        "ToUnicode" => to_unicode,
    });
    let mut ops = vec![
        Operation::new("BT", vec![]),
        Operation::new("Tf", vec!["F1".into(), 12.into()]),
        Operation::new("Td", vec![40.into(), 780.into()]),
    ];
    for line in lines {
        let bytes: Vec<u8> = line.chars().flat_map(|c| cid(c).to_be_bytes()).collect();
        ops.push(Operation::new(
            "Tj",
            vec![Object::String(bytes, StringFormat::Hexadecimal)],
        ));
        ops.push(Operation::new("Td", vec![0.into(), (-20).into()]));
    }
    ops.push(Operation::new("ET", vec![]));
    let content = doc.add_object(Stream::new(
        dictionary! {},
        Content { operations: ops }.encode().unwrap(),
    ));
    let pages_id = doc.new_object_id();
    let page = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "Contents" => content,
        "MediaBox" => vec![0.into(), 0.into(), 595.into(), 842.into()],
        "Resources" => dictionary! { "Font" => dictionary! { "F1" => font } },
    });
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => vec![page.into()],
            "Count" => 1,
        }),
    );
    let catalog = doc.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
    doc.trailer.set("Root", catalog);
    let mut out = Vec::new();
    doc.save_to(&mut out).unwrap();
    out
}

#[tokio::test]
async fn every_route_needs_the_tenant_header() {
    let app = app().await;
    for (method, uri) in [
        ("GET", "/api/car-inspections/current"),
        ("GET", "/api/files"),
        ("GET", "/api/nfc-tags"),
        ("GET", "/api/maintenance/vehicles"),
    ] {
        assert_eq!(call(&app, method, uri, None, None).await.status, 401);
    }
}

#[tokio::test]
async fn json_upload_makes_inspections_listable_and_lookupable() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;

    // 古い車検証 (期限切れ) と、同じ車の新しい車検証、別の車
    let old = upload_json(
        &app,
        t,
        &cert(
            "100000000001",
            "CARID000000001",
            "1",
            "200101",
            "品川500あ1",
        ),
    )
    .await;
    let new = upload_json(
        &app,
        t,
        &cert(
            "100000000002",
            "CARID000000001",
            "2",
            "991231",
            "品川500あ1",
        ),
    )
    .await;
    upload_json(
        &app,
        t,
        &cert("100000000003", "CARID000000002", "3", "991231", ""),
    )
    .await;
    // 行は storageVerified = true、キーは {tenant}/{uuid}
    let files = get_json(&app, t, "/api/files?type=application/json").await;
    let rows = files["files"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    let mut keys: Vec<&str> = rows[0]
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    keys.sort();
    assert_eq!(keys, FILE_KEYS);
    assert!(rows
        .iter()
        .all(|r| r["storageVerified"] == true && r["blob"].is_null()));
    assert_eq!(rows[2]["uuid"], old.as_str());
    assert_eq!(rows[2]["s3Key"], format!("{t}/{old}"));
    assert_eq!(rows[2]["fileType"], "application/json");
    assert_eq!(rows[2]["storageClass"], "STANDARD");
    assert_eq!(
        app.carins.object(&format!("{t}/{old}")).unwrap().1,
        "application/json"
    );

    // current: 車ごとに期限の新しい 1 枚。JSON の uuid が付く
    let current = inspections(&get_json(&app, t, "/api/car-inspections/current").await);
    assert_eq!(current.len(), 2);
    assert_eq!(current[0]["ElectCertMgNo"], "100000000002");
    assert_eq!(current[0]["jsonUuid"], new.as_str());
    assert!(current[0]["pdfUuid"].is_null());
    // 交付日の空白は除いて保存する。文字列でない値は空文字
    assert_eq!(current[0]["GrantdateE"], "令和");
    assert_eq!(current[0]["GrantdateY"], "8");
    assert_eq!(current[0]["Cap"], "");
    assert_eq!(current[0]["CertInfoImportFileVersion"], "2.0");

    // history: 1 台の車検証を新しい順
    let history = inspections(
        &get_json(
            &app,
            t,
            "/api/car-inspections/by-car/CARID000000001/history",
        )
        .await,
    );
    assert_eq!(history.len(), 2);
    assert_eq!(history[0]["jsonUuid"], new.as_str());
    assert_eq!(history[1]["jsonUuid"], old.as_str());

    // expired: 期限が 30 日以内 (過ぎたものを含む)
    let expired = inspections(&get_json(&app, t, "/api/car-inspections/expired").await);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0]["ElectCertMgNo"], "100000000001");

    // 分類の候補 (重複と空を除いて並べる)
    assert_eq!(
        get_json(&app, t, "/api/car-inspections/vehicle-categories").await,
        json!({"car_kinds": ["普通"], "uses": ["貨物"], "car_shapes": ["キャブオーバ"], "private_businesses": ["事業用"]})
    );

    // 同じ車検証 (管理番号・交付日) をもう一度入れると更新 (行は増えない)
    let mut again = cert(
        "100000000002",
        "CARID000000001",
        "2",
        "991231",
        "品川500あ1",
    );
    again["CertInfo"]["CarName"] = json!("変更後");
    upload_json(&app, t, &again).await;
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.car_inspection WHERE tenant_id = $1",
            t
        )
        .await,
        3
    );
    let history = inspections(
        &get_json(
            &app,
            t,
            "/api/car-inspections/by-car/CARID000000001/history",
        )
        .await,
    );
    assert_eq!(history[0]["CarName"], "変更後");

    // 照合
    let lookup = |body: Value| {
        let app = &app;
        async move {
            call(
                app,
                "POST",
                "/api/car-inspections/lookup",
                Some(t),
                Some(body),
            )
            .await
        }
    };
    let got = lookup(json!({"cert_no": "100000000002"})).await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!({"expires_on": "2099-12-31", "matched_by": "cert_no", "car_no": "品川500あ1"})
    );
    let got = lookup(json!({"car_id": "CARID000000001", "cert_no": ""})).await;
    assert_eq!(got.json()["matched_by"], "car_id");
    assert_eq!(got.json()["expires_on"], "2099-12-31");
    let got = lookup(json!({"car_id": "CARID000000002"})).await;
    assert!(got.json()["car_no"].is_null());
    assert_eq!(
        lookup(json!({"cert_no": "999999999999"})).await.json(),
        json!({"expires_on": null, "matched_by": "none", "car_no": null})
    );
    assert_eq!(lookup(json!({"cert_no": "12"})).await.status, 400);
    assert_eq!(lookup(json!({})).await.status, 400);

    // 和暦の 4 列からの期限 (2 次元コードの期限が空の行)
    let mut wareki = cert("100000000004", "CARID000000003", "4", "", "");
    wareki["CertInfo"]["ValidPeriodExpirdateE"] = json!("令 和");
    wareki["CertInfo"]["ValidPeriodExpirdateY"] = json!("8");
    wareki["CertInfo"]["ValidPeriodExpirdateM"] = json!("3");
    wareki["CertInfo"]["ValidPeriodExpirdateD"] = json!("31");
    upload_json(&app, t, &wareki).await;
    assert_eq!(
        lookup(json!({"car_id": "CARID000000003"})).await.json()["expires_on"],
        "2026-03-31"
    );

    // 別の会社には何も見えない
    assert!(inspections(&get_json(&app, other, "/api/car-inspections/current").await).is_empty());
    assert!(get_json(&app, other, "/api/files").await["files"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/car-inspections/lookup",
            Some(other),
            Some(json!({"cert_no": "100000000002"}))
        )
        .await
        .json()["matched_by"],
        "none"
    );
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn pdf_waits_for_json_and_links_when_it_arrives() {
    let app = app().await;
    let t = tenant().await;
    let mut pg = rt_client().await;

    // JSON がまだ無い → 待ちに入る
    let pdf1 = Uuid::new_v4();
    link_pdf(&mut pg, t, pdf1, key("200000000001", "13"))
        .await
        .unwrap();
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.pending_car_inspection_pdfs WHERE tenant_id = $1",
            t
        )
        .await,
        1
    );

    // JSON が来ると、待っていた PDF を files_b に紐づけて待ちを消す
    upload_json(
        &app,
        t,
        &cert("200000000001", "CARID000000010", "13", "991231", ""),
    )
    .await;
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.pending_car_inspection_pdfs WHERE tenant_id = $1",
            t
        )
        .await,
        0
    );
    let current = inspections(&get_json(&app, t, "/api/car-inspections/current").await);
    assert_eq!(current[0]["pdfUuid"], pdf1.to_string());

    // JSON が在れば直に files_b へ (新しい方が出る)
    let pdf2 = Uuid::new_v4();
    link_pdf(&mut pg, t, pdf2, key("200000000001", "13"))
        .await
        .unwrap();
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.car_inspection_files_b WHERE tenant_id = $1",
            t
        )
        .await,
        2
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.pending_car_inspection_pdfs WHERE tenant_id = $1",
            t
        )
        .await,
        0
    );

    // 同じ管理番号の待ちは 1 つ (後から来た PDF に置き換わる)
    link_pdf(&mut pg, t, Uuid::new_v4(), key("200000000002", "1"))
        .await
        .unwrap();
    let latest = Uuid::new_v4();
    link_pdf(&mut pg, t, latest, key("200000000002", "1"))
        .await
        .unwrap();
    let waiting: Uuid = admin()
        .await
        .query_one(
            "SELECT file_uuid FROM alc_api.pending_car_inspection_pdfs WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(waiting, latest);
}

#[tokio::test]
async fn pdf_upload_reads_the_first_page() {
    let app = app().await;
    let t = tenant().await;

    // 車検証の PDF (pdf-extract で読める日本語の 1 ページ) → JSON が無いので待ちに入る
    let pdf = text_pdf(&[
        "自動車検査証記録事項",
        "300000000001",
        "記録年月日 令和8年2月13日",
    ]);
    let got = upload(&app, t, "車検証.pdf", "application/pdf", &pdf).await;
    assert_eq!(got.status, 201);
    let pdf_uuid = got.json()["uuid"].as_str().unwrap().to_owned();
    let row = admin()
        .await
        .query_one(
            r#"SELECT file_uuid, "ElectCertMgNo", "GrantdateE", "GrantdateY", "GrantdateM", "GrantdateD"
               FROM alc_api.pending_car_inspection_pdfs WHERE tenant_id = $1"#,
            &[&t],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Uuid>(0).to_string(), pdf_uuid);
    assert_eq!(row.get::<_, String>(1), "300000000001");
    assert_eq!(
        [2, 3, 4, 5].map(|i| row.get::<_, String>(i)),
        ["令和", "8", "2", "13"].map(String::from)
    );

    // 車検証でない PDF は何もしない (ログも出さない)
    let got = upload(
        &app,
        t,
        "other.pdf",
        "application/pdf",
        &text_pdf(&["請求書"]),
    )
    .await;
    assert_eq!(got.status, 201);
    assert!(app.logs().is_empty(), "{:?}", app.logs());

    // 読めない PDF・JSON でないもの・車検証でない JSON は登録だけして、理由を固定の語でログに出す
    assert_eq!(
        upload(&app, t, "x.pdf", "application/pdf", b"not a pdf")
            .await
            .status,
        201
    );
    assert_eq!(
        upload(&app, t, "x.json", "application/json", b"{")
            .await
            .status,
        201
    );
    assert_eq!(
        upload(&app, t, "x.json", "application/json", br#"{"a": 1}"#)
            .await
            .status,
        201
    );
    // ほかの type は読まない
    assert_eq!(
        upload(&app, t, "x.png", "image/png", b"png").await.status,
        201
    );
    assert_eq!(
        app.logs(),
        [
            "carins: car inspection parse skipped (pdf unreadable)",
            "carins: car inspection parse skipped (json invalid)",
            "carins: car inspection parse skipped (missing CertInfo)",
        ]
    );
    assert_eq!(
        count("SELECT count(*) FROM alc_api.files WHERE tenant_id = $1", t).await,
        6
    );
}

#[tokio::test]
async fn files_list_recent_download_and_verify() {
    let app = app().await;
    let t = tenant().await;
    let su = admin().await;

    let png = upload(&app, t, "写真 \"1\".png", "image/png", b"PNGDATA")
        .await
        .json();
    let png_uuid = png["uuid"].as_str().unwrap().to_owned();
    upload_json(
        &app,
        t,
        &cert("400000000001", "CARID000000020", "1", "991231", ""),
    )
    .await;

    // 一覧 (type の絞り込み) と最近の 50 件
    assert_eq!(
        get_json(&app, t, "/api/files").await["files"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let only_png = get_json(&app, t, "/api/files?type=image/png").await;
    assert_eq!(only_png["files"].as_array().unwrap().len(), 1);
    assert_eq!(
        get_json(&app, t, "/api/files/recent").await["files"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // 取り出し (R2)。Content-Disposition は ASCII だけ
    let got = call(
        &app,
        "GET",
        &format!("/api/files/{png_uuid}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"PNGDATA");
    assert_eq!(got.headers[header::CONTENT_TYPE], "image/png");
    let cd = got.headers[header::CONTENT_DISPOSITION].to_str().unwrap();
    assert!(cd.starts_with("attachment; filename*=UTF-8''"));
    assert!(cd.ends_with("; filename=\"__ _1_.png\""));

    // 古い行 (DB の base64)・壊れた base64・どちらも無い行
    let blob = Uuid::new_v4();
    let bad = Uuid::new_v4();
    let empty = Uuid::new_v4();
    for (id, b) in [
        (blob, Some(STANDARD.encode("legacy"))),
        (bad, Some("!!".to_owned())),
        (empty, None),
    ] {
        su.execute(
            "INSERT INTO alc_api.files (uuid, tenant_id, filename, type, blob) VALUES ($1, $2, 'old.txt', 'text/plain', $3)",
            &[&id, &t, &b],
        )
        .await
        .unwrap();
    }
    let got = call(
        &app,
        "GET",
        &format!("/api/files/{blob}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(
        (got.status, got.body.as_slice()),
        (200, b"legacy".as_slice())
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/files/{bad}/download"),
            Some(t),
            None
        )
        .await
        .status,
        500
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/files/{empty}/download"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );
    let unknown = Uuid::new_v4();
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/files/{unknown}/download"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(&app, "GET", "/api/files/x/download", Some(t), None)
            .await
            .status,
        400
    );
    assert_eq!(
        app.logs(),
        ["carins: download_file failed: blob (not base64)"]
    );

    // verify: 確かめた結果が在ればそれ、無ければ R2 / base64 の有無で決めて残す
    let in_r2 = Uuid::new_v4();
    let not_in_r2 = Uuid::new_v4();
    for id in [in_r2, not_in_r2] {
        su.execute(
            "INSERT INTO alc_api.files (uuid, tenant_id, filename, type, s3_key) VALUES ($1, $2, 'f', 'text/plain', $3)",
            &[&id, &t, &format!("{t}/{id}")],
        )
        .await
        .unwrap();
    }
    app.carins.insert(&format!("{t}/{in_r2}"), b"x");
    let asked = [
        png_uuid.clone(),
        in_r2.to_string(),
        not_in_r2.to_string(),
        blob.to_string(),
        empty.to_string(),
        unknown.to_string(),
        "x".to_owned(),
    ];
    let got = call(
        &app,
        "POST",
        "/api/files/verify",
        Some(t),
        Some(json!({"uuids": asked})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!({png_uuid.clone(): true, in_r2.to_string(): true, not_in_r2.to_string(): false, blob.to_string(): true, empty.to_string(): false})
    );
    let stored = |id: Uuid| {
        let su = &su;
        async move {
            su.query_one(
                "SELECT storage_verified FROM alc_api.files WHERE uuid = $1",
                &[&id],
            )
            .await
            .unwrap()
            .get::<_, Option<bool>>(0)
        }
    };
    assert_eq!(stored(in_r2).await, Some(true));
    assert_eq!(stored(not_in_r2).await, Some(false));
    assert_eq!(stored(blob).await, Some(true));
    assert_eq!(stored(empty).await, Some(false));
    // 残った結果は R2 を見ない (R2 から消えても true のまま)
    app.carins.objects.lock().unwrap().clear();
    let got = call(
        &app,
        "POST",
        "/api/files/verify",
        Some(t),
        Some(json!({"uuids": [in_r2.to_string()]})),
    )
    .await;
    assert_eq!(got.json(), json!({in_r2.to_string(): true}));
    // 別の会社の uuid は結果に入らない
    let got = call(
        &app,
        "POST",
        "/api/files/verify",
        Some(tenant().await),
        Some(json!({"uuids": [in_r2.to_string()]})),
    )
    .await;
    assert_eq!(got.json(), json!({}));
}

#[tokio::test]
async fn r2_failures_and_bad_input() {
    let t = tenant().await;

    // R2 の binding が無い → 503
    let no_store = build_with(rt_client().await, false, true);
    assert_eq!(
        upload(&no_store, t, "a.txt", "text/plain", b"a")
            .await
            .status,
        503
    );
    assert_eq!(
        call(
            &no_store,
            "POST",
            "/api/files/verify",
            Some(t),
            Some(json!({"uuids": []}))
        )
        .await
        .status,
        503
    );
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.files (uuid, tenant_id, filename, type, s3_key) VALUES ($1, $2, 'f', 'text/plain', 'k')",
            &[&id, &t],
        )
        .await
        .unwrap();
    assert_eq!(
        call(
            &no_store,
            "GET",
            &format!("/api/files/{id}/download"),
            Some(t),
            None
        )
        .await
        .status,
        503
    );

    let app = app().await;
    // base64 でない本文 → 400
    let got = call(
        &app,
        "POST",
        "/api/files",
        Some(t),
        Some(json!({"filename": "a", "type": "text/plain", "content": "!!"})),
    )
    .await;
    assert_eq!(got.status, 400);
    // 置けない・置いたのに無い・在るかを確かめられない → 500 (行は作らない)
    for (flag, log) in [
        (
            &app.carins.fail_put,
            "carins: create_file upload failed: r2 (put)",
        ),
        (
            &app.carins.lose_put,
            "carins: create_file failed: r2 (not found after upload)",
        ),
        (
            &app.carins.fail_exists,
            "carins: create_file exists failed: r2 (head)",
        ),
    ] {
        flag.store(true, Ordering::SeqCst);
        assert_eq!(
            upload(&app, t, "a.txt", "text/plain", b"a").await.status,
            500
        );
        flag.store(false, Ordering::SeqCst);
        assert_eq!(app.logs().last().unwrap(), log);
    }
    assert_eq!(
        count("SELECT count(*) FROM alc_api.files WHERE tenant_id = $1", t).await,
        1
    );
    // 読めない → 500
    app.carins.fail_get.store(true, Ordering::SeqCst);
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/files/{id}/download"),
            Some(t),
            None
        )
        .await
        .status,
        500
    );
    assert_eq!(
        app.logs().last().unwrap(),
        "carins: download_file failed: r2 (get)"
    );
    // verify の R2 の失敗は「無い」扱い
    app.carins.fail_exists.store(true, Ordering::SeqCst);
    let got = call(
        &app,
        "POST",
        "/api/files/verify",
        Some(t),
        Some(json!({"uuids": [id.to_string()]})),
    )
    .await;
    assert_eq!(got.json(), json!({id.to_string(): false}));
}

#[tokio::test]
async fn nfc_tags_register_search_list_delete() {
    let app = app().await;
    let t = tenant().await;
    upload_json(
        &app,
        t,
        &cert("500000000001", "CARID000000030", "1", "991231", ""),
    )
    .await;
    upload_json(
        &app,
        t,
        &cert("500000000002", "CARID000000031", "1", "991231", ""),
    )
    .await;
    let ids: Vec<i32> = admin()
        .await
        .query(
            "SELECT id FROM alc_api.car_inspection WHERE tenant_id = $1 ORDER BY id",
            &[&t],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();

    let got = call(
        &app,
        "POST",
        "/api/nfc-tags",
        Some(t),
        Some(json!({"nfc_uuid": "04:A1:B2", "car_inspection_id": ids[0]})),
    )
    .await;
    assert_eq!(got.status, 201);
    let tag = got.json();
    let mut keys: Vec<&str> = tag
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    keys.sort();
    assert_eq!(keys, ["carInspectionId", "createdAt", "id", "nfcUuid"]);
    assert_eq!(tag["nfcUuid"], "04a1b2");
    call(
        &app,
        "POST",
        "/api/nfc-tags",
        Some(t),
        Some(json!({"nfc_uuid": "ff", "car_inspection_id": ids[1]})),
    )
    .await;

    // 同じタグの登録は付け替え
    let got = call(
        &app,
        "POST",
        "/api/nfc-tags",
        Some(t),
        Some(json!({"nfc_uuid": "04a1b2", "car_inspection_id": ids[1]})),
    )
    .await;
    assert_eq!(got.json()["carInspectionId"], ids[1]);
    assert_eq!(
        get_json(&app, t, "/api/nfc-tags")
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        get_json(
            &app,
            t,
            &format!("/api/nfc-tags?car_inspection_id={}", ids[0])
        )
        .await,
        json!([])
    );

    let found = get_json(&app, t, "/api/nfc-tags/search?uuid=04:A1:B2").await;
    assert_eq!(found["nfc_tag"]["nfcUuid"], "04a1b2");
    assert_eq!(found["car_inspection"]["ElectCertMgNo"], "500000000002");
    assert_eq!(
        call(&app, "GET", "/api/nfc-tags/search?uuid=00", Some(t), None)
            .await
            .status,
        404
    );
    // 別の会社からは見えない・消せない
    let other = tenant().await;
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/nfc-tags/search?uuid=04a1b2",
            Some(other),
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(&app, "DELETE", "/api/nfc-tags/04a1b2", Some(other), None)
            .await
            .status,
        404
    );

    assert_eq!(
        call(&app, "DELETE", "/api/nfc-tags/04:A1:B2", Some(t), None)
            .await
            .status,
        204
    );
    assert_eq!(
        call(&app, "DELETE", "/api/nfc-tags/04a1b2", Some(t), None)
            .await
            .status,
        404
    );
}

#[tokio::test]
async fn db_failures_are_500_with_fixed_logs() {
    let app = build(broken_client().await);
    let t = tenant().await;
    let id = Uuid::new_v4();
    let calls: Vec<(&str, String, Option<Value>)> = vec![
        ("GET", "/api/car-inspections/current".into(), None),
        ("GET", "/api/car-inspections/expired".into(), None),
        ("GET", "/api/car-inspections/by-car/X/history".into(), None),
        (
            "GET",
            "/api/car-inspections/vehicle-categories".into(),
            None,
        ),
        (
            "POST",
            "/api/car-inspections/lookup".into(),
            Some(json!({"cert_no": "123456789012"})),
        ),
        ("GET", "/api/files".into(), None),
        ("GET", "/api/files/recent".into(), None),
        (
            "POST",
            "/api/files".into(),
            Some(json!({"filename": "a", "type": "text/plain", "content": "YQ=="})),
        ),
        ("GET", format!("/api/files/{id}/download"), None),
        (
            "POST",
            "/api/files/verify".into(),
            Some(json!({"uuids": [id.to_string()]})),
        ),
        ("GET", "/api/nfc-tags".into(), None),
        ("GET", "/api/nfc-tags/search?uuid=a".into(), None),
        (
            "POST",
            "/api/nfc-tags".into(),
            Some(json!({"nfc_uuid": "a", "car_inspection_id": 1})),
        ),
        ("DELETE", "/api/nfc-tags/a".into(), None),
    ];
    for (method, uri, body) in calls {
        assert_eq!(
            call(&app, method, &uri, Some(t), body).await.status,
            500,
            "{method} {uri}"
        );
    }
    assert_eq!(app.logs().len(), 14);
    assert_logs_clean(&app, &[t.to_string(), id.to_string()]);
    assert!(app.logs().iter().all(|l| l.contains("failed: db (")));
}

#[tokio::test]
async fn partial_db_failures_keep_the_upload() {
    let t = tenant().await;
    let json_cert =
        serde_json::to_vec(&cert("600000000001", "CARID000000040", "1", "991231", "")).unwrap();

    // 行を作れない → 500 (R2 には置いた後)
    let app = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(
        upload(&app, t, "a.txt", "text/plain", b"a").await.status,
        500
    );
    assert_eq!(
        app.logs(),
        ["carins: create_file insert failed: db (42501)"]
    );

    // 車検証の表に書けない → 登録は 201、取り込みの失敗はログだけ
    let app = build(client_as(NO_INSPECTION_ROLE).await);
    assert_eq!(
        upload(&app, t, "c.json", "application/json", &json_cert)
            .await
            .status,
        201
    );
    assert_eq!(app.logs(), ["carins: car inspection parse skipped (db)"]);
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.car_inspection WHERE tenant_id = $1",
            t
        )
        .await,
        0
    );

    // 待ちの表に触れない → 車検証の取り込みは残り、待ちの段の失敗はログだけ。PDF の待ちは Err(Db)
    let app = build(client_as(NO_PENDING_ROLE).await);
    assert_eq!(
        upload(&app, t, "c.json", "application/json", &json_cert)
            .await
            .status,
        201
    );
    assert_eq!(app.logs(), ["carins: car inspection parse skipped (db)"]);
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.car_inspection WHERE tenant_id = $1",
            t
        )
        .await,
        1
    );
    let mut pg = client_as(NO_PENDING_ROLE).await;
    assert_eq!(
        link_pdf(&mut pg, t, Uuid::new_v4(), key("600000000009", "1")).await,
        Err(Skip::Db)
    );

    // 確かめた結果を残せない → 結果は返し、ログだけ
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.files (uuid, tenant_id, filename, type, blob) VALUES ($1, $2, 'f', 'text/plain', 'YQ==')",
            &[&id, &t],
        )
        .await
        .unwrap();
    let app = build(client_as(READ_ONLY_ROLE).await);
    let got = call(
        &app,
        "POST",
        "/api/files/verify",
        Some(t),
        Some(json!({"uuids": [id.to_string()]})),
    )
    .await;
    assert_eq!(got.json(), json!({id.to_string(): true}));
    assert_eq!(
        app.logs(),
        ["carins: verify_files update failed: db (42501)"]
    );
}
