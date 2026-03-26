mod common;

#[tokio::test]
async fn test_happy_path() {
    transfer_test! {
        length: 8,
        steps: [Read(8)],
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_source_transient_then_resume() {
    transfer_test! {
        length: 8,
        steps: [
            Read(3),                                     // 3 bytes transferred
            SourceTransient("simulated stream error"),   // source fails
            Read(5),                                     // retry: 5 more bytes
        ],                                               // 3 + 5 = 8 ✓
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_source_permanent_error() {
    transfer_test! {
        length: 0,
        steps: [GetReaderPermanent("not found")],
        expect: PermanentError,
    }
}

#[tokio::test]
async fn test_stream_permanent_mid_transfer() {
    transfer_test! {
        length: 100,
        steps: [
            Read(50),
            SourcePermanent("resource gone"),
        ],
        expect: PermanentError,
    }
}

#[tokio::test]
async fn test_writer_transient_error() {
    transfer_test! {
        length: 100,
        steps: [
            Read(50),                          // 50 bytes persisted by writer
            WriterTransient("disk busy"),      // writer fails
            Read(50),                          // retry: 50 more bytes
        ],                                     // 50 + 50 = 100 ✓
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_writer_permanent_error() {
    transfer_test! {
        length: 100,
        steps: [
            Read(50),
            WriterPermanent("disk full"),
        ],
        expect: PermanentError,
    }
}

#[tokio::test]
async fn test_offset_mismatch_truncates() {
    transfer_test! {
        length: 10,
        steps: [
            Read(4),                           // 4 bytes transferred
            SourceTransient("simulated"),      // source fails
            Restart,                           // server can't resume → truncate, start from 0
            Read(10),                           // full re-transfer from byte 0
        ],                                     // restart resets to 0, then 10 = 10 ✓
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_retry_exhaustion() {
    transfer_test! {
        length: 100,
        max_retries: 2,
        steps: [
            SourceTransient("always failing"),   // attempt 1: immediate fail (0 bytes)
            SourceTransient("always failing"),   // attempt 2: immediate fail
            SourceTransient("always failing"),   // attempt 3: exhausts retries
        ],
        expect: PermanentErrorContaining("exhausted"),
    }
}

#[tokio::test]
async fn test_zero_byte_transfer() {
    transfer_test! {
        length: 0,
        steps: [Read(0)],
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_get_reader_transient_then_success() {
    transfer_test! {
        length: 4,
        steps: [
            GetReaderTransient("connection refused"),   // get_reader fails once
            Read(4),                                    // retried, succeeds
        ],
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_partial_write_resume() {
    transfer_test! {
        length: 100,
        steps: [
            Read(40),
            WriterTransient("disk busy"),
            Read(60),
        ],
        expect: Succeeds,
    }
}

#[tokio::test]
async fn test_writer_error_with_multi_chunk_source() {
    transfer_test! {
        length: 120,
        source_chunk_size: 40,
        steps: [
            Read(80),                          // 2 chunks written (40+40)
            WriterTransient("disk busy"),      // writer fails on 3rd chunk
            Read(40),                          // retry: last 40 bytes
        ],                                     // 80 + 40 = 120 ✓
        expect: Succeeds,
    }
}

// DSL self-tests: verify the framework catches invalid step configurations

#[tokio::test]
#[should_panic(expected = "steps read 9 bytes total but length is 10")]
async fn test_dsl_rejects_under_read() {
    transfer_test! {
        length: 10,
        steps: [Read(9)],
        expect: Succeeds,
    }
}

#[tokio::test]
#[should_panic(expected = "exceeds data length")]
async fn test_dsl_rejects_over_read() {
    transfer_test! {
        length: 10,
        steps: [Read(11)],
        expect: Succeeds,
    }
}

#[tokio::test]
#[should_panic(expected = "cannot appear mid-attempt")]
async fn test_dsl_rejects_get_reader_transient_mid_attempt() {
    transfer_test! {
        length: 10,
        steps: [Read(5), GetReaderTransient("err")],
        expect: Succeeds,
    }
}

#[tokio::test]
#[should_panic(expected = "cannot appear mid-attempt")]
async fn test_dsl_rejects_get_reader_permanent_mid_attempt() {
    transfer_test! {
        length: 10,
        steps: [Read(5), GetReaderPermanent("err")],
        expect: Succeeds,
    }
}

#[tokio::test]
#[should_panic(expected = "must follow an error")]
async fn test_dsl_rejects_restart_mid_attempt() {
    transfer_test! {
        length: 10,
        steps: [Read(5), Restart],
        expect: Succeeds,
    }
}

#[tokio::test]
#[should_panic(expected = "multiple writer errors")]
async fn test_dsl_rejects_multiple_writer_errors() {
    transfer_test! {
        length: 100,
        steps: [
            Read(30),
            WriterTransient("first"),
            Read(30),
            WriterTransient("second"),
            Read(40),
        ],
        expect: Succeeds,
    }
}
