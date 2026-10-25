//! 登録した車検証のファイル (JSON / PDF) を読み、車検証の行とファイルを紐づける (rust の `alc-carins` の carins_files.rs の
//! `try_parse_car_inspection_json`・`try_parse_car_inspection_pdf` と repo/car_inspections.rs の写し)。
//!
//! - JSON (電子車検証の閲覧アプリが出すもの): `CertInfo` を car_inspection に upsert し、ファイルを `car_inspection_files_a`
//!   に紐づける。同じ管理番号の PDF が先に来て待っていれば (`pending_car_inspection_pdfs`)、それを `car_inspection_files_b`
//!   に紐づけて待ちを消す
//! - PDF (自動車検査証記録事項): 1 ページ目の文字から管理番号と交付日を取り、同じ車検証の JSON が在れば
//!   `car_inspection_files_b` に紐づけ、無ければ待ち (`pending_car_inspection_pdfs`) に入れる。車検証でない PDF は何もしない
//!
//! 失敗しても登録の口は成功のまま (rust と同じ)。理由は固定の語だけをログに出す (番号・ファイルの中身を出さない)。
//!
//! rust との違い: JSON の upsert とファイルの紐づけを 1 つの transaction にした (rust は別々の接続で、紐づけが失敗しても
//! upsert が残った)。upsert の 94 列は JSON の値を SQL で取り出す (rust は 1 列ずつ bind していた。値の取り方は同じで、
//! 文字列でない値・無いキーは空文字)。

use std::sync::LazyLock;

use alc_worker_db::{PgClient, TenantTx};
use regex::Regex;
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

/// 読めなかった理由 (固定の語)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skip {
    /// 理由の語 (ログに出す)
    Reason(&'static str),
    /// DB の失敗
    Db,
}

/// `CertInfo` の列 (car_inspection の列の名前と同じ。rust の upsert の `$3`〜`$96` の順)。
/// 先頭の `CertInfoImportFileVersion` は `CertInfo` の外 (JSON の最上位) から取るので、この表には入れない
pub const CERT_FIELDS: &[&str] = &[
    "Acceptoutputno",
    "FormType",
    "ElectCertMgNo",
    "CarId",
    "ElectCertPublishdateE",
    "ElectCertPublishdateY",
    "ElectCertPublishdateM",
    "ElectCertPublishdateD",
    "GrantdateE",
    "GrantdateY",
    "GrantdateM",
    "GrantdateD",
    "TranspotationBureauchiefName",
    "EntryNoCarNo",
    "ReggrantdateE",
    "ReggrantdateY",
    "ReggrantdateM",
    "ReggrantdateD",
    "FirstregistdateE",
    "FirstregistdateY",
    "FirstregistdateM",
    "CarName",
    "CarNameCode",
    "CarNo",
    "Model",
    "EngineModel",
    "OwnernameLowLevelChar",
    "OwnernameHighLevelChar",
    "OwnerAddressChar",
    "OwnerAddressNumValue",
    "OwnerAddressCode",
    "UsernameLowLevelChar",
    "UsernameHighLevelChar",
    "UserAddressChar",
    "UserAddressNumValue",
    "UserAddressCode",
    "UseheadqrterChar",
    "UseheadqrterNumValue",
    "UseheadqrterCode",
    "CarKind",
    "Use",
    "PrivateBusiness",
    "CarShape",
    "CarShapeCode",
    "NoteCap",
    "Cap",
    "NoteMaxloadage",
    "Maxloadage",
    "NoteCarWgt",
    "CarWgt",
    "NoteCarTotalWgt",
    "CarTotalWgt",
    "NoteLength",
    "Length",
    "NoteWidth",
    "Width",
    "NoteHeight",
    "Height",
    "FfAxWgt",
    "FrAxWgt",
    "RfAxWgt",
    "RrAxWgt",
    "Displacement",
    "FuelClass",
    "ModelSpecifyNo",
    "ClassifyAroundNo",
    "ValidPeriodExpirdateE",
    "ValidPeriodExpirdateY",
    "ValidPeriodExpirdateM",
    "ValidPeriodExpirdateD",
    "NoteInfo",
    "TwodimensionCodeInfoEntryNoCarNo",
    "TwodimensionCodeInfoCarNo",
    "TwodimensionCodeInfoValidPeriodExpirdate",
    "TwodimensionCodeInfoModel",
    "TwodimensionCodeInfoModelSpecifyNoClassifyAroundNo",
    "TwodimensionCodeInfoCharInfo",
    "TwodimensionCodeInfoEngineModel",
    "TwodimensionCodeInfoCarNoStampPlace",
    "TwodimensionCodeInfoFirstregistdate",
    "TwodimensionCodeInfoFfAxWgt",
    "TwodimensionCodeInfoFrAxWgt",
    "TwodimensionCodeInfoRfAxWgt",
    "TwodimensionCodeInfoRrAxWgt",
    "TwodimensionCodeInfoNoiseReg",
    "TwodimensionCodeInfoNearNoiseReg",
    "TwodimensionCodeInfoDriveMethod",
    "TwodimensionCodeInfoOpacimeterMeasCar",
    "TwodimensionCodeInfoNoxPmMeasMode",
    "TwodimensionCodeInfoNoxValue",
    "TwodimensionCodeInfoPmValue",
    "TwodimensionCodeInfoSafeStdDate",
    "TwodimensionCodeInfoFuelClassCode",
    "RegistCarLightCar",
];

/// 一意の鍵 (更新しない列)
const KEY_FIELDS: &[&str] = &[
    "ElectCertMgNo",
    "GrantdateE",
    "GrantdateY",
    "GrantdateM",
    "GrantdateD",
];

/// car_inspection の upsert (`$1` = tenant、`$2` = CertInfoImportFileVersion、`$3` = 交付日の空白を除いた `CertInfo`)。
/// 列の名前は [`CERT_FIELDS`] のリテラルだけ (実行時の文字列を埋め込まない)
pub fn upsert_sql() -> String {
    let columns: Vec<String> = CERT_FIELDS.iter().map(|f| format!("\"{f}\"")).collect();
    let values: Vec<String> = CERT_FIELDS
        .iter()
        .map(|f| {
            format!("COALESCE(CASE WHEN jsonb_typeof($3::jsonb -> '{f}') = 'string' THEN $3::jsonb ->> '{f}' END, '')")
        })
        .collect();
    let updates: Vec<String> = std::iter::once("CertInfoImportFileVersion")
        .chain(
            CERT_FIELDS
                .iter()
                .copied()
                .filter(|f| !KEY_FIELDS.contains(f)),
        )
        .map(|f| format!("\"{f}\" = EXCLUDED.\"{f}\""))
        .collect();
    format!(
        "INSERT INTO alc_api.car_inspection (tenant_id, \"CertInfoImportFileVersion\", {}) \
         SELECT $1, $2, {} \
         ON CONFLICT (tenant_id, \"ElectCertMgNo\", \"GrantdateE\", \"GrantdateY\", \"GrantdateM\", \"GrantdateD\") \
         DO UPDATE SET {}, modified_at = NOW()",
        columns.join(", "),
        values.join(", "),
        updates.join(", ")
    )
}

pub mod sql {
    /// `{table}` は `car_inspection_files_a` (JSON) か `car_inspection_files_b` (PDF) のリテラルだけ
    macro_rules! link {
        ($table:literal) => {
            concat!(
                "INSERT INTO alc_api.",
                $table,
                " (uuid, tenant_id, type, \"ElectCertMgNo\", \"GrantdateE\", \"GrantdateY\", \"GrantdateM\", \"GrantdateD\") \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (uuid) DO UPDATE SET modified_at = NOW()"
            )
        };
    }
    pub const LINK_JSON: &str = link!("car_inspection_files_a");
    pub const LINK_PDF: &str = link!("car_inspection_files_b");

    pub const FIND_PENDING_PDF: &str = r#"SELECT file_uuid FROM alc_api.pending_car_inspection_pdfs
        WHERE tenant_id = $1 AND "ElectCertMgNo" = $2"#;

    pub const DELETE_PENDING_PDF: &str = r#"DELETE FROM alc_api.pending_car_inspection_pdfs
        WHERE tenant_id = $1 AND "ElectCertMgNo" = $2"#;

    pub const UPSERT_PENDING_PDF: &str = r#"INSERT INTO alc_api.pending_car_inspection_pdfs
        (tenant_id, file_uuid, "ElectCertMgNo", "GrantdateE", "GrantdateY", "GrantdateM", "GrantdateD")
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tenant_id, "ElectCertMgNo")
        DO UPDATE SET file_uuid = EXCLUDED.file_uuid,
                      "GrantdateE" = EXCLUDED."GrantdateE",
                      "GrantdateY" = EXCLUDED."GrantdateY",
                      "GrantdateM" = EXCLUDED."GrantdateM",
                      "GrantdateD" = EXCLUDED."GrantdateD",
                      created_at = NOW()"#;

    pub const JSON_FILE_EXISTS: &str = r#"SELECT EXISTS(
        SELECT 1 FROM alc_api.car_inspection_files_a
        WHERE tenant_id = $1 AND "ElectCertMgNo" = $2
          AND "GrantdateE" = $3 AND "GrantdateY" = $4
          AND "GrantdateM" = $5 AND "GrantdateD" = $6
          AND type = 'application/json'
          AND deleted_at IS NULL)"#;
}

/// 管理番号と交付日 (和暦の元号・年・月・日。空白を除いたもの)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertKey {
    pub elect_cert_mg_no: String,
    pub grantdate: [String; 4],
}

fn strip_spaces(s: &str) -> String {
    s.replace([' ', '\u{3000}'], "")
}

fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// JSON から upsert に渡す値 (CertInfoImportFileVersion・交付日の空白を除いた CertInfo・鍵) を取り出す
pub fn read_json(data: &[u8]) -> Result<(String, Value, CertKey), Skip> {
    let json: Value = serde_json::from_slice(data).map_err(|_| Skip::Reason("json invalid"))?;
    let mut cert_info = json
        .get("CertInfo")
        .filter(|v| v.is_object())
        .cloned()
        .ok_or(Skip::Reason("missing CertInfo"))?;
    let elect_cert_mg_no = cert_info
        .get("ElectCertMgNo")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(Skip::Reason("missing ElectCertMgNo"))?
        .to_owned();
    let version = str_field(&json, "CertInfoImportFileVersion").to_owned();
    let grantdate = ["GrantdateE", "GrantdateY", "GrantdateM", "GrantdateD"].map(|k| {
        let v = strip_spaces(str_field(&cert_info, k));
        cert_info[k] = Value::String(v.clone());
        v
    });
    Ok((
        version,
        cert_info,
        CertKey {
            elect_cert_mg_no,
            grantdate,
        },
    ))
}

async fn link(
    tx: &TenantTx<'_>,
    stmt: &'static str,
    tenant_id: Uuid,
    file_uuid: Uuid,
    file_type: &str,
    key: &CertKey,
) -> Result<(), Error> {
    let [e, y, m, d] = &key.grantdate;
    tx.execute_typed(
        stmt,
        &[
            (&file_uuid, Type::UUID),
            (&tenant_id, Type::UUID),
            (&file_type, Type::TEXT),
            (&key.elect_cert_mg_no, Type::TEXT),
            (e, Type::TEXT),
            (y, Type::TEXT),
            (m, Type::TEXT),
            (d, Type::TEXT),
        ],
    )
    .await
    .map(|_| ())
}

/// 車検証の JSON を登録する。upsert と files_a の紐づけ (1 つの transaction) の後、待っている PDF を紐づける
/// (別の transaction。失敗しても JSON の登録は残る。rust と同じ)。待ちの段の失敗は `Err(Skip::Db)` で返す
pub async fn apply_json(
    pg: &mut PgClient,
    tenant_id: Uuid,
    file_uuid: Uuid,
    data: &[u8],
) -> Result<(), Skip> {
    let (version, cert_info, key) = read_json(data)?;
    let upsert = upsert_sql();
    let k = key.clone();
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                &upsert,
                &[
                    (&tenant_id, Type::UUID),
                    (&version, Type::TEXT),
                    (&cert_info, Type::JSONB),
                ],
            )
            .await?;
            link(
                tx,
                sql::LINK_JSON,
                tenant_id,
                file_uuid,
                "application/json",
                &k,
            )
            .await
        })
    })
    .await
    .map_err(|_| Skip::Db)?;
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let pending = tx
                .query_typed_opt(
                    sql::FIND_PENDING_PDF,
                    &[
                        (&tenant_id, Type::UUID),
                        (&key.elect_cert_mg_no, Type::TEXT),
                    ],
                )
                .await?;
            let Some(row) = pending else {
                return Ok(());
            };
            let pdf_uuid: Uuid = row.get(0);
            link(
                tx,
                sql::LINK_PDF,
                tenant_id,
                pdf_uuid,
                "application/pdf",
                &key,
            )
            .await?;
            tx.execute_typed(
                sql::DELETE_PENDING_PDF,
                &[
                    (&tenant_id, Type::UUID),
                    (&key.elect_cert_mg_no, Type::TEXT),
                ],
            )
            .await
            .map(|_| ())
        })
    })
    .await
    .map_err(|_| Skip::Db)
}

/// 車検証判定
static RE_CAR_INSPECTION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"自\s*動\s*車\s*検\s*査\s*証\s*記\s*録\s*事\s*項").unwrap());

/// ElectCertMgNo: 12 桁の数字
static RE_ELECT_CERT_MG_NO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d{12}").unwrap());

/// 交付日: pdf-extract の形 "令 和  8  2  13 月 日"
static RE_GRANTDATE_HEADER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)記録年月日.*?(令\s*和|平\s*成|昭\s*和)\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})")
        .unwrap()
});

/// 交付日: 日本語の形 "令和8年2月13日"
static RE_GRANTDATE_STANDARD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?s)記録年月日.*?(令\s*和|平\s*成|昭\s*和)\s*(\d{1,2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
    )
    .unwrap()
});

/// 交付日: 備考の中 (最後の手)
static RE_GRANTDATE_BIKO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?s)４[.．]\s*備考.*?(令\s*和|平\s*成|昭\s*和)\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})",
    )
    .unwrap()
});

/// PDF の 1 ページ目の文字から鍵を取る (rust の `process_car_inspection_pages`)。
/// 車検証でない PDF は `Ok(None)`
pub fn read_pdf_pages(pages: &[String]) -> Result<Option<CertKey>, Skip> {
    let page1 = pages.first().ok_or(Skip::Reason("pdf has no pages"))?;
    if page1.is_empty() {
        return Err(Skip::Reason("pdf page 1 has no text"));
    }
    if !RE_CAR_INSPECTION.is_match(page1) {
        return Ok(None);
    }
    let elect_cert_mg_no = RE_ELECT_CERT_MG_NO
        .find(page1)
        .ok_or(Skip::Reason("pdf has no ElectCertMgNo"))?
        .as_str()
        .to_owned();
    let caps = RE_GRANTDATE_HEADER
        .captures(page1)
        .or_else(|| RE_GRANTDATE_STANDARD.captures(page1))
        .or_else(|| RE_GRANTDATE_BIKO.captures(page1))
        .ok_or(Skip::Reason("pdf has no Grantdate"))?;
    Ok(Some(CertKey {
        elect_cert_mg_no,
        grantdate: [1, 2, 3, 4].map(|i| strip_spaces(&caps[i])),
    }))
}

/// 車検証の PDF を登録する ([`read_pdf_pages`] → [`link_pdf`])。車検証でない PDF は何もしない
pub async fn apply_pdf(
    pg: &mut PgClient,
    tenant_id: Uuid,
    file_uuid: Uuid,
    data: &[u8],
) -> Result<(), Skip> {
    let pages = pdf_extract::extract_text_from_mem_by_pages(data)
        .map_err(|_| Skip::Reason("pdf unreadable"))?;
    match read_pdf_pages(&pages)? {
        Some(key) => link_pdf(pg, tenant_id, file_uuid, key).await,
        None => Ok(()),
    }
}

/// 同じ車検証の JSON が在れば PDF を files_b に紐づけ、無ければ待ちに入れる (1 つの transaction)
pub async fn link_pdf(
    pg: &mut PgClient,
    tenant_id: Uuid,
    file_uuid: Uuid,
    key: CertKey,
) -> Result<(), Skip> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let [e, y, m, d] = &key.grantdate;
            let json_exists: bool = tx
                .query_typed_one(
                    sql::JSON_FILE_EXISTS,
                    &[
                        (&tenant_id, Type::UUID),
                        (&key.elect_cert_mg_no, Type::TEXT),
                        (e, Type::TEXT),
                        (y, Type::TEXT),
                        (m, Type::TEXT),
                        (d, Type::TEXT),
                    ],
                )
                .await?
                .get(0);
            if json_exists {
                return link(
                    tx,
                    sql::LINK_PDF,
                    tenant_id,
                    file_uuid,
                    "application/pdf",
                    &key,
                )
                .await;
            }
            tx.execute_typed(
                sql::UPSERT_PENDING_PDF,
                &[
                    (&tenant_id, Type::UUID),
                    (&file_uuid, Type::UUID),
                    (&key.elect_cert_mg_no, Type::TEXT),
                    (e, Type::TEXT),
                    (y, Type::TEXT),
                    (m, Type::TEXT),
                    (d, Type::TEXT),
                ],
            )
            .await
            .map(|_| ())
        })
    })
    .await
    .map_err(|_| Skip::Db)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_covers_every_field_once() {
        assert_eq!(CERT_FIELDS.len(), 94);
        let mut seen = std::collections::HashSet::new();
        assert!(CERT_FIELDS.iter().all(|f| seen.insert(*f)));
        let sql = upsert_sql();
        // 鍵の列は更新しない。ほかの列と CertInfoImportFileVersion は更新する
        for f in KEY_FIELDS {
            assert!(!sql.contains(&format!("\"{f}\" = EXCLUDED")));
        }
        assert!(
            sql.contains("\"CertInfoImportFileVersion\" = EXCLUDED.\"CertInfoImportFileVersion\"")
        );
        assert!(sql.contains("\"CarId\" = EXCLUDED.\"CarId\""));
        assert_eq!(sql.matches(" = EXCLUDED.").count(), 94 - 5 + 1);
    }

    #[test]
    fn json_is_read_like_rust() {
        let data = serde_json::to_vec(&serde_json::json!({
            "CertInfoImportFileVersion": "1.0",
            "CertInfo": {"ElectCertMgNo": "000000000001", "GrantdateE": "令 和", "GrantdateY": " 8", "GrantdateM": "２", "GrantdateD": "1\u{3000}3"}
        }))
        .unwrap();
        let (version, cert, key) = read_json(&data).unwrap();
        assert_eq!(version, "1.0");
        assert_eq!(key.elect_cert_mg_no, "000000000001");
        assert_eq!(key.grantdate, ["令和", "8", "２", "13"].map(String::from));
        assert_eq!(cert["GrantdateE"], "令和");

        assert_eq!(read_json(b"x"), Err(Skip::Reason("json invalid")));
        assert_eq!(
            read_json(br#"{"CertInfo": 1}"#),
            Err(Skip::Reason("missing CertInfo"))
        );
        assert_eq!(
            read_json(br#"{"CertInfo": {"ElectCertMgNo": ""}}"#),
            Err(Skip::Reason("missing ElectCertMgNo"))
        );
        // 版が無ければ空文字
        let (version, _, _) = read_json(br#"{"CertInfo": {"ElectCertMgNo": "1"}}"#).unwrap();
        assert_eq!(version, "");
    }

    fn page(s: &str) -> Vec<String> {
        vec![s.to_owned()]
    }

    #[test]
    fn pdf_page_is_read_like_rust() {
        let head = "自 動 車 検 査 証 記 録 事 項\n 123456789012 \n";
        // pdf-extract の形
        let key = read_pdf_pages(&page(&format!("{head}記録年月日 令 和  8  2  13 月 日")))
            .unwrap()
            .unwrap();
        assert_eq!(key.elect_cert_mg_no, "123456789012");
        assert_eq!(key.grantdate, ["令和", "8", "2", "13"].map(String::from));
        // 日本語の形
        let key = read_pdf_pages(&page(&format!("{head}記録年月日\n平成31年4月1日")))
            .unwrap()
            .unwrap();
        assert_eq!(key.grantdate, ["平成", "31", "4", "1"].map(String::from));
        // 備考の中
        let key = read_pdf_pages(&page(&format!("{head}４．備考 昭 和 60 1 2")))
            .unwrap()
            .unwrap();
        assert_eq!(key.grantdate, ["昭和", "60", "1", "2"].map(String::from));

        assert_eq!(read_pdf_pages(&page("ただの PDF")), Ok(None));
        assert_eq!(read_pdf_pages(&[]), Err(Skip::Reason("pdf has no pages")));
        assert_eq!(
            read_pdf_pages(&page("")),
            Err(Skip::Reason("pdf page 1 has no text"))
        );
        assert_eq!(
            read_pdf_pages(&page("自動車検査証記録事項")),
            Err(Skip::Reason("pdf has no ElectCertMgNo"))
        );
        assert_eq!(
            read_pdf_pages(&page(head)),
            Err(Skip::Reason("pdf has no Grantdate"))
        );
    }
}
