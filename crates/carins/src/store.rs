//! ファイルの置き場 (Worker では R2 の binding、テストではモック) と、取り出しの口の `Content-Disposition`
//! (ippoan/alc-trouble-worker の files.rs の `FileStore`・`content_disposition` の写しに、在るかの確かめ `exists` を足した)。

use std::sync::Arc;

use futures_util::future::BoxFuture;

/// R2 の段の失敗。持つのは段の名前だけ (キーやランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreError(pub &'static str);

/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
pub trait FileStore: Send + Sync {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// 無いキーも失敗 (rust の download と同じく 500 にする)
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>>;

    /// キーが在るか (rust の `StorageBackend::exists`)
    fn exists<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, StoreError>>;
}

pub type SharedStore = Arc<dyn FileStore>;

/// `Content-Disposition` (RFC 6266)。**ASCII だけ**で組む: `filename*` に UTF-8 の percent-encoding (RFC 5987)、
/// `filename` に ASCII の代替 (非 ASCII・制御文字・`"`・`\` を `_` に)。
///
/// **rust と違う**: rust は `filename="<名前の " を _ に>"` で日本語を生の UTF-8 のまま返す (整備の添付)。Workers は
/// 非 ASCII のヘッダ値を、値 (= ファイル名) 入りの警告にして出すので、ファイル名がログに出る経路を作らないよう ASCII にする
/// (ippoan/alc-trouble-worker と同じ)。呼び手の画面はこのヘッダを読まず、名前は一覧の JSON から取る。
pub fn content_disposition(filename: &str) -> String {
    let fallback: String = filename
        .chars()
        .map(|c| match c {
            ' '..='~' if c != '"' && c != '\\' => c,
            _ => '_',
        })
        .collect();
    let mut encoded = String::new();
    for b in filename.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    format!("attachment; filename*=UTF-8''{encoded}; filename=\"{fallback}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disposition_is_ascii_with_filename_star() {
        assert_eq!(
            content_disposition("車検証 \"新\"\\a\t.pdf"),
            "attachment; filename*=UTF-8''%E8%BB%8A%E6%A4%9C%E8%A8%BC%20%22%E6%96%B0%22%5Ca%09.pdf; \
             filename=\"___ ____a_.pdf\""
        );
        assert_eq!(
            content_disposition("cert_v1-2.final(1).json"),
            "attachment; filename*=UTF-8''cert_v1-2.final%281%29.json; \
             filename=\"cert_v1-2.final(1).json\""
        );
        assert!(content_disposition("整備🚚.jpg").is_ascii());
    }
}
