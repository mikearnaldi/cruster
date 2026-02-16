//! OpenTelemetry integration for Cruster.
//!
//! Provides helpers to initialize an OpenTelemetry tracing pipeline that bridges
//! `tracing` spans (already used throughout the codebase) to OpenTelemetry exporters.
//!
//! # Usage
//!
//! Enable the `otel` feature in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! cruster = { version = "...", features = ["otel"] }
//! ```
//!
//! Then call [`init_tracing`] early in your application:
//!
//! ```text
//! use cruster::otel;
//!
//! #[tokio::main]
//! async fn main() {
//!     let _guard = otel::init_tracing(otel::OtelConfig::default()).expect("otel init failed");
//!     // ... start your cluster
//!     // guard is dropped on shutdown, flushing remaining spans
//! }
//! ```
//!
//! # How It Works
//!
//! Cruster already uses `tracing::instrument` and `tracing::{info, debug, warn, ...}`
//! across all hot paths. This module sets up:
//!
//! 1. An OpenTelemetry OTLP exporter (gRPC to a collector)
//! 2. A `tracing-opentelemetry` layer that bridges `tracing` spans → OTel spans
//! 3. A `tracing-subscriber` registry combining OTel + stdout logging
//!
//! All existing `#[instrument]` spans on message routing, entity management,
//! storage operations, gRPC transport, and workflow execution automatically
//! become OpenTelemetry spans with full parent/child relationships.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Configuration for OpenTelemetry initialization.
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Service name reported to the collector. Default: `"cruster"`.
    pub service_name: String,
    /// OTLP endpoint URL. Default: `"http://localhost:4317"`.
    pub otlp_endpoint: String,
    /// Whether to also output logs to stdout. Default: `true`.
    pub stdout_logs: bool,
    /// `tracing` filter directive for the stdout layer.
    /// Default: `"info,cruster=debug"`.
    pub log_filter: String,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            service_name: "cruster".to_string(),
            otlp_endpoint: "http://localhost:4317".to_string(),
            stdout_logs: true,
            log_filter: "info,cruster=debug".to_string(),
        }
    }
}

/// Guard that shuts down the tracer provider on drop, flushing remaining spans.
///
/// Hold this for the lifetime of your application. When dropped, it calls
/// `TracerProvider::shutdown()` to ensure all buffered spans are exported.
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("OpenTelemetry shutdown error: {e}");
        }
    }
}

/// Initialize the OpenTelemetry tracing pipeline.
///
/// Sets up:
/// - An OTLP gRPC exporter sending spans to `config.otlp_endpoint`
/// - A `tracing-opentelemetry` layer bridging `tracing` spans → OTel spans
/// - Optionally, a formatted stdout log layer
///
/// Returns an [`OtelGuard`] that must be held for the application lifetime.
/// On drop, it flushes remaining spans to the collector.
///
/// # Errors
///
/// Returns an error if the OTLP exporter or tracer provider fails to initialize.
pub fn init_tracing(config: OtelConfig) -> Result<OtelGuard, Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::resource::Resource;

    // Build the OTLP exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()?;

    // Build the tracer provider with batching
    let resource = Resource::new_with_defaults([KeyValue::new(
        opentelemetry::Key::new("service.name"),
        opentelemetry::Value::from(config.service_name.clone()),
    )]);

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(config.service_name);

    // Build the tracing-opentelemetry layer
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Build the subscriber
    let registry = tracing_subscriber::registry().with(otel_layer);

    if config.stdout_logs {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_filter));
        let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);
        registry.with(env_filter).with(fmt_layer).init();
    } else {
        registry.init();
    }

    Ok(OtelGuard { provider })
}

/// Initialize a minimal tracing pipeline without OpenTelemetry export.
///
/// Useful for development/testing when you want structured tracing output
/// but don't need an external collector.
pub fn init_tracing_stdout_only(filter: &str) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();
}
