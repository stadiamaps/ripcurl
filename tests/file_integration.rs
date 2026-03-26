use ripcurl::destination::resolve_destination;
use ripcurl::protocol::file::{FileProtocol, WriteMode};
use ripcurl::protocol::{DestinationProtocol, DestinationWriter, TransferError};
use ripcurl::transfer::TransferConfig;
use tempfile::TempDir;
use url::Url;

fn file_url(path: &std::path::Path) -> Url {
    Url::from_file_path(path).unwrap()
}

#[tokio::test]
async fn test_write_and_finalize() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");
    let data = b"hello, ripcurl!";

    let proto = FileProtocol::new(WriteMode::CreateNew);
    let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();
    writer.write(data).await.unwrap();
    writer.finalize().await.unwrap();

    let content = std::fs::read(&dest).unwrap();
    assert_eq!(content, data);
}

#[tokio::test]
async fn test_drop_without_finalize_cleans_up() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");

    {
        let proto = FileProtocol::new(WriteMode::CreateNew);
        let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();
        writer.write(b"incomplete data").await.unwrap();
        // writer is dropped here without finalize
    }

    assert!(
        !dest.exists(),
        "partial file should be cleaned up on drop without finalize"
    );
}

#[tokio::test]
async fn test_drop_cleanup_with_overwrite_mode() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");

    {
        let proto = FileProtocol::new(WriteMode::Overwrite);
        let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();
        writer.write(b"incomplete data").await.unwrap();
        // writer is dropped here without finalize
    }

    assert!(
        !dest.exists(),
        "partial file should be cleaned up on drop without finalize (Overwrite mode)"
    );
}

#[tokio::test]
async fn test_finalized_file_not_cleaned_up() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");

    {
        let proto = FileProtocol::new(WriteMode::CreateNew);
        let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();
        writer.write(b"complete data").await.unwrap();
        writer.finalize().await.unwrap();
    }

    assert!(dest.exists(), "finalized file should remain on disk");
    assert_eq!(std::fs::read(&dest).unwrap(), b"complete data");
}

#[tokio::test]
async fn test_truncate_and_reset() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");

    let proto = FileProtocol::new(WriteMode::CreateNew);
    let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();

    // Write first batch
    writer.write(b"first batch").await.unwrap();

    // Truncate and reset
    writer.truncate_and_reset().await.unwrap();

    // Write second batch
    writer.write(b"second").await.unwrap();
    writer.finalize().await.unwrap();

    let content = std::fs::read(&dest).unwrap();
    assert_eq!(content, b"second", "only post-truncate data should remain");
}

#[tokio::test]
async fn test_create_new_mode() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");

    let proto = FileProtocol::new(WriteMode::CreateNew);
    let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();
    writer.write(b"data").await.unwrap();
    writer.finalize().await.unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), b"data");
}

#[tokio::test]
async fn test_create_new_fails_if_exists() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("output.bin");

    // Create the file first
    std::fs::write(&dest, b"existing").unwrap();

    let proto = FileProtocol::new(WriteMode::CreateNew);
    let result = proto.get_writer(file_url(&dest)).await;

    assert!(
        result.is_err(),
        "CreateNew should fail if file already exists"
    );
}

#[tokio::test]
async fn test_invalid_scheme_rejected() {
    let proto = FileProtocol::new(WriteMode::CreateNew);
    let url = Url::parse("http://example.com/file").unwrap();
    let result = proto.get_writer(url).await;

    assert!(result.is_err(), "non-file scheme should be rejected");
}

#[tokio::test]
async fn test_resolve_destination_default_fails_if_file_exists() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("existing.txt");
    std::fs::write(&path, b"hello").unwrap();

    let config = TransferConfig {
        max_retries: 10,
        overwrite: false,
    };
    let dest = resolve_destination(&file_url(&path), &config).unwrap();
    let result = dest.get_writer(file_url(&path)).await;

    assert!(
        result.is_err(),
        "expected failure when file exists and overwrite is false"
    );
}

#[tokio::test]
async fn test_resolve_destination_overwrite_succeeds() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("existing.txt");
    std::fs::write(&path, b"hello").unwrap();

    let config = TransferConfig {
        max_retries: 10,
        overwrite: true,
    };
    let dest = resolve_destination(&file_url(&path), &config).unwrap();
    let mut writer = dest.get_writer(file_url(&path)).await.unwrap();

    writer.write(b"world").await.unwrap();
    writer.finalize().await.unwrap();

    assert_eq!(std::fs::read_to_string(&path).unwrap(), "world");
}

#[test]
fn test_resolve_destination_rejects_unsupported_scheme() {
    let url = Url::parse("foo://example.com/file.txt").unwrap();
    let config = TransferConfig {
        max_retries: 10,
        overwrite: false,
    };
    let result = resolve_destination(&url, &config);

    assert!(matches!(result, Err(TransferError::Permanent { .. })));
}
