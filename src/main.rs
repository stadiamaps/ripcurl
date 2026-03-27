use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing_indicatif::IndicatifLayer;
use tracing_indicatif::filter::{IndicatifFilter, hide_indicatif_span_fields};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

#[derive(Parser)]
#[command(name = "ripcurl", about, version)]
struct Cli {
    /// Source URL of the file to transfer (schema-less URLs are assumed to be file://).
    source: String,

    /// Destination URL for the file to be stored (schema-less URLs are assumed to be file://).
    destination: String,

    /// Overwrites the destination if it already exists.
    #[arg(long, help_heading = "General Options")]
    overwrite: bool,

    /// Maximum number of retry attempts for transient errors.
    #[arg(long, default_value_t = 10, help_heading = "General Options")]
    max_retries: u32,

    /// Disables the progress bar.
    #[arg(long, help_heading = "General Options")]
    no_progress: bool,

    #[command(flatten)]
    http: HttpOptions,
}

#[derive(clap::Args)]
#[command(next_help_heading = "HTTP Options")]
struct HttpOptions {
    /// Custom HTTP header to include in source requests (format: "Name: Value").
    ///
    /// Can be specified multiple times. Sensitive headers like Authorization are
    /// automatically stripped when following redirects to a different host.
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let show_progress = !cli.no_progress && !is_ci();
    if show_progress {
        let indicatif_layer = IndicatifLayer::new()
            .with_span_field_formatter(hide_indicatif_span_fields(DefaultFields::new()));
        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
            .with(indicatif_layer.with_filter(IndicatifFilter::new(false)))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

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

    let custom_http_headers: Vec<(String, String)> = match cli
        .http
        .headers
        .iter()
        .map(|h| parse_header(h))
        .collect::<Result<_, _>>()
    {
        Ok(h) => h,
        Err(e) => {
            eprintln!("Invalid header: {e}");
            return ExitCode::from(3);
        }
    };

    let config = ripcurl::transfer::TransferConfig {
        max_retries: cli.max_retries,
        overwrite: cli.overwrite,
        custom_http_headers,
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

/// Returns `true` if running in a CI environment.
fn is_ci() -> bool {
    std::env::var_os("CI").is_some()
}

/// Parse a raw header string in "Name: Value" format into a (name, value) pair.
fn parse_header(raw: &str) -> Result<(String, String), String> {
    let (name, value) = raw
        .split_once(':')
        .ok_or_else(|| format!("expected \"Name: Value\" format, got \"{raw}\""))?;

    let name = name.trim();
    if name.is_empty() {
        return Err(format!("header name cannot be empty in \"{raw}\""));
    }

    Ok((name.to_string(), value.trim().to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_header_basic() {
        assert_eq!(
            parse_header("Authorization: Bearer tok123").unwrap(),
            ("Authorization".to_string(), "Bearer tok123".to_string())
        );
    }

    #[test]
    fn parse_header_colons_in_value() {
        assert_eq!(
            parse_header("X-Data: has:colons:in:value").unwrap(),
            ("X-Data".to_string(), "has:colons:in:value".to_string())
        );
    }

    #[test]
    fn parse_header_trims_whitespace() {
        assert_eq!(
            parse_header("  Name  :  value  ").unwrap(),
            ("Name".to_string(), "value".to_string())
        );
    }

    #[test]
    fn parse_header_empty_value() {
        assert_eq!(
            parse_header("Name:").unwrap(),
            ("Name".to_string(), String::new())
        );
    }

    #[test]
    fn parse_header_missing_colon() {
        assert!(parse_header("InvalidNoColon").is_err());
    }

    #[test]
    fn parse_header_empty_name() {
        assert!(parse_header(": value").is_err());
    }
}
