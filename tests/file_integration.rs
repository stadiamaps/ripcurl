use ripcurl::protocol::file::{FileProtocol, WriteMode};
use ripcurl::protocol::{DestinationProtocol, DestinationWriter};
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

// TODO: A test along these lines makes sense once we have an atomic mode.
// #[tokio::test]
// async fn test_drop_without_finalize_cleans_up() {
//     let tmp = TempDir::new().unwrap();
//     let dest = tmp.path().join("output.bin");
//     // TODO: Do we know the name of the file?
//     // let tmp_path = tmp.path().join(".output.bin.ripcurl-tmp");

//     {
//         let proto = FileProtocol::new(WriteMode::Atomic);
//         let mut writer = proto.get_writer(file_url(&dest)).await.unwrap();
//         writer.write(b"incomplete data").await.unwrap();

//         // assert!(tmp_path.exists(), "temp file should exist during write");
//         // writer is dropped here without finalize
//     }

//     // // After drop: both files should be gone
//     // assert!(!tmp_path.exists(), "temp file should be cleaned up on drop");
//     assert!(!dest.exists(), "final file should not exist");
// }

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
