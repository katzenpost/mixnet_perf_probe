use std::sync::atomic::AtomicU64;
use std::time::Duration;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Op {
    op: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OpResult {
    op: String,
    result: String,
}

pub struct Metrics {
    registry: Registry,
    duration: Family<Op, Histogram>,
    last_duration: Family<Op, Gauge<f64, AtomicU64>>,
    ops: Family<OpResult, Counter>,
    pub boxes: Gauge,
    pub box_payload_bytes: Gauge,
    pub last_cycle: Gauge<f64, AtomicU64>,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("mpp");
        let duration = Family::<Op, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1.0, 1.5, 20))
        });
        let last_duration = Family::<Op, Gauge<f64, AtomicU64>>::default();
        let ops = Family::<OpResult, Counter>::default();
        let boxes = Gauge::default();
        let box_payload_bytes = Gauge::default();
        let last_cycle = Gauge::<f64, AtomicU64>::default();

        registry.register("duration_seconds", "Duration of successful operations", duration.clone());
        registry.register("last_duration_seconds", "Duration of the latest successful operation", last_duration.clone());
        registry.register("ops", "Operations by result", ops.clone());
        registry.register("boxes", "Boxes in each multi-box operation", boxes.clone());
        registry.register("box_payload_bytes", "Plaintext bytes per box", box_payload_bytes.clone());
        registry.register("last_cycle_timestamp_seconds", "Unix time the latest probe cycle finished", last_cycle.clone());

        Metrics { registry, duration, last_duration, ops, boxes, box_payload_bytes, last_cycle }
    }

    pub fn record(&self, op: &str, result: Result<Duration, &str>) {
        let label = Op { op: op.into() };
        if let Ok(d) = result {
            self.duration.get_or_create(&label).observe(d.as_secs_f64());
            self.last_duration.get_or_create(&label).set(d.as_secs_f64());
        }
        let result = result.err().unwrap_or("ok");
        self.ops.get_or_create(&OpResult { op: op.into(), result: result.into() }).inc();
    }

    pub fn encode(&self) -> String {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &self.registry).unwrap();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_successes_and_failures() {
        let m = Metrics::new();
        m.record("write_box", Ok(Duration::from_secs(12)));
        m.record("write_box", Err("Timeout"));
        let text = m.encode();
        assert!(text.contains(r#"mpp_ops_total{op="write_box",result="ok"} 1"#), "{text}");
        assert!(text.contains(r#"mpp_ops_total{op="write_box",result="Timeout"} 1"#));
        assert!(text.contains(r#"mpp_last_duration_seconds{op="write_box"} 12.0"#));
        assert!(text.contains(r#"mpp_duration_seconds_count{op="write_box"} 1"#));
    }
}
