# mixnet_perf_probe

Measures how long Pigeonhole box operations take on a Katzenpost mixnet such
as namenlos, and serves the results as Prometheus metrics. It talks to the
mixnet through a running `kpclientd` using the Rust thin client.

The mixnet is slow and has little bandwidth, so the probe runs one cycle,
sleeps for an hour, and repeats. Each cycle uses fresh channels and times:

| op | what is timed |
|---|---|
| `write_box` | writing one box, until the courier acknowledges it |
| `read_box` | reading that box back |
| `write_n_boxes` | writing n boxes into one channel, one after another |
| `read_n_boxes` | reading those n boxes back |
| `copy_n_boxes` | writing the same n boxes' worth of data with the COPY command: the boxes go to a temporary channel, then the courier copies them into the destination channel |

Every read is checked against what was written. The data written with COPY
is read back and checked too, but that read is not timed: reads of copied
boxes are no different from other reads.

## Running

```sh
cargo build --release
./target/release/mixnet_perf_probe
```

| option | default | |
|---|---|---|
| `--daemon` | `@katzenpost` | kpclientd address: `@name`, a socket path, or `HOST:PORT` |
| `--listen` | `127.0.0.1:9810` | where to serve `/metrics` |
| `--interval` | `1h` | sleep between cycles |
| `--timeout` | `10m` | how long to wait for one box |
| `--boxes` | `4` | n |
| `--once` | | run one cycle and exit |

## Metrics

| metric | labels | |
|---|---|---|
| `mpp_duration_seconds` | op | histogram of successful operations |
| `mpp_last_duration_seconds` | op | the latest successful duration |
| `mpp_ops_total` | op, result | operations by result: `ok`, `Timeout`, `Mismatch`, or the thin client error such as `BoxNotFound` |
| `mpp_boxes` | | n |
| `mpp_box_payload_bytes` | | plaintext bytes per box |
| `mpp_last_cycle_timestamp_seconds` | | when the latest cycle finished |

## Dashboards

`monitoring/` runs Prometheus and Grafana with rootless podman:

```sh
cd monitoring && podman-compose up -d
```

Grafana is at <http://127.0.0.1:3000>. It has two dashboards:

* **Pigeonhole box performance**, from the probe.
* **kpclientd**, from kpclientd's own metrics on `127.0.0.1:9811`. These need
  kpclientd built with `-tags kpclientd_metrics` and
  `MetricsAddress = "127.0.0.1:9811"` set in its config.

## License

AGPL-3.0-only
