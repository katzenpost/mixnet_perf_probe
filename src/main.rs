mod metrics;
mod probe;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, extract::State, routing::get};
use clap::Parser;
use katzenpost_thin_client::{Config, DialConfig, TcpDialConfig, ThinClient, UnixDialConfig};

use metrics::Metrics;
use probe::Probe;

/// Measures Pigeonhole box reads and writes on a Katzenpost mixnet
/// through kpclientd and serves the results as Prometheus metrics.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// kpclientd address: @name for an abstract unix socket, a socket path, or HOST:PORT.
    #[arg(long, default_value = "@katzenpost")]
    daemon: String,
    /// Address to serve /metrics on.
    #[arg(long, default_value = "127.0.0.1:9810")]
    listen: SocketAddr,
    /// Time to sleep between probe cycles.
    #[arg(long, default_value = "1h", value_parser = humantime::parse_duration)]
    interval: Duration,
    /// How long to wait for one box to be written or read.
    #[arg(long, default_value = "10m", value_parser = humantime::parse_duration)]
    timeout: Duration,
    /// Number of boxes in the multi-box operations.
    #[arg(long, default_value_t = 4)]
    boxes: usize,
    /// Run one cycle and exit.
    #[arg(long)]
    once: bool,
}

fn dial(daemon: &str) -> DialConfig {
    if daemon.starts_with('@') || daemon.starts_with('/') {
        DialConfig { unix: Some(UnixDialConfig { address: daemon.into() }), tcp: None }
    } else {
        DialConfig { unix: None, tcp: Some(TcpDialConfig { address: daemon.into(), network: None }) }
    }
}

async fn serve(listen: SocketAddr, metrics: Arc<Metrics>) {
    let app = Router::new()
        .route("/metrics", get(|State(m): State<Arc<Metrics>>| async move { m.encode() }))
        .with_state(metrics);
    let listener = tokio::net::TcpListener::bind(listen).await.expect("binding the metrics listener");
    axum::serve(listener, app).await.expect("serving metrics");
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let metrics = Arc::new(Metrics::new());
    tokio::spawn(serve(args.listen, metrics.clone()));

    let config = Config {
        dial: dial(&args.daemon),
        on_connection_status: None,
        on_new_pki_document: None,
        on_message_sent: None,
        on_message_reply: None,
        on_daemon_disconnected: None,
    };
    let client = ThinClient::new(config).await.expect("connecting to kpclientd");

    // The pigeonhole geometry is only known once the daemon has sent its
    // first PKI document.
    while client.pki_document().await.is_err() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let box_len = client.pigeonhole_geometry().max_plaintext_payload_length;
    metrics.box_payload_bytes.set(box_len as i64);
    metrics.boxes.set(args.boxes as i64);

    let probe = Probe { client: client.clone(), metrics: metrics.clone(), timeout: args.timeout, box_len, boxes: args.boxes };
    loop {
        if let Err(e) = probe.cycle().await {
            log::warn!("cycle failed: {e:?}");
        }
        metrics.last_cycle.set(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64());
        if args.once {
            break;
        }
        tokio::time::sleep(args.interval).await;
    }
    client.stop().await;
}
