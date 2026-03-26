use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;
use url::Url;

#[derive(Parser)]
#[command(name = "ripcurl", version)]
struct Cli {
    /// Source URL of the file to transfer (schema-less URLs are assumed to be file://).
    source: String,

    /// Destination URL for the file to be stored (schema-less URLs are assumed to be file://).
    destination: String,

    /// Overwrite the destination if it already exists.
    #[arg(long)]
    overwrite: bool,

    /// Maximum number of retry attempts for transient errors.
    #[arg(long, default_value_t = 10)]
    max_retries: u32,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let source_url = match parse_url(&cli.source) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("Invalid source URL: {e}");
            return ExitCode::from(3);
        }
    };

    let dest_url = match parse_url(&cli.destination) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("Invalid destination URL: {e}");
            return ExitCode::from(3);
        }
    };

    let config = ripcurl::transfer::TransferConfig {
        max_retries: cli.max_retries,
        overwrite: cli.overwrite,
    };

    match ripcurl::transfer::execute_transfer(source_url, dest_url, &config).await {
        Ok(bytes) => {
            eprintln!("Done — {bytes} bytes transferred.");
            ExitCode::SUCCESS
        }
        Err(ripcurl::protocol::TransferError::Permanent { reason }) => {
            eprintln!("Transfer failed: {reason}");
            ExitCode::FAILURE
        }
        Err(ripcurl::protocol::TransferError::Transient { reason, .. }) => {
            eprintln!("Transfer failed: {reason}");
            ExitCode::FAILURE
        }
    }
}

/// Parse a CLI argument into a URL.
///
/// If the argument already contains a scheme (e.g. `https://...`), parse it directly.
/// Otherwise, it is treated as a file path.
fn parse_url(input: &str) -> Result<Url, String> {
    if input.contains("://") {
        return Url::parse(input).map_err(|e| e.to_string());
    }

    let path = PathBuf::from(input);
    let abs_path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|e| format!("could not resolve current directory: {e}"))?
            .join(path)
    };

    Url::from_file_path(&abs_path)
        .map_err(|_| format!("could not convert path to URL: {}", abs_path.display()))
}
