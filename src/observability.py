"""
Prometheus metrics and lightweight health endpoints for beacon-indexer.
"""
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


API_LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
CHUNK_DURATION_BUCKETS = (0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800)

api_requests_total = Counter(
    "beacon_indexer_api_requests_total",
    "Beacon API requests",
    ["endpoint", "status"],
)

api_request_duration_seconds = Histogram(
    "beacon_indexer_api_request_duration_seconds",
    "Beacon API request duration",
    ["endpoint"],
    buckets=API_LATENCY_BUCKETS,
)

chain_head_slot = Gauge(
    "beacon_indexer_chain_head_slot",
    "Latest beacon head slot observed by the indexer",
)

highest_raw_slot = Gauge(
    "beacon_indexer_highest_raw_slot",
    "Highest raw slot by loader",
    ["loader"],
)

highest_transformed_slot = Gauge(
    "beacon_indexer_highest_transformed_slot",
    "Highest transformed slot by table",
    ["table"],
)

chain_lag_slots = Gauge(
    "beacon_indexer_chain_lag_slots",
    "Slots behind chain head by stage",
    ["stage", "name"],
)

chunks_total = Counter(
    "beacon_indexer_chunks_total",
    "Chunk state transitions",
    ["loader", "status"],
)

chunk_duration_seconds = Histogram(
    "beacon_indexer_chunk_duration_seconds",
    "Chunk processing duration",
    ["loader", "operation"],
    buckets=CHUNK_DURATION_BUCKETS,
)

chunks_in_progress = Gauge(
    "beacon_indexer_chunks_in_progress",
    "Chunks currently in progress",
    ["loader", "operation"],
)

rows_written_total = Counter(
    "beacon_indexer_rows_written_total",
    "Rows written to transformed tables",
    ["table"],
)

transform_failures_total = Counter(
    "beacon_indexer_transform_failures_total",
    "Transform failures",
    ["loader", "fork"],
)

unknown_fork_total = Counter(
    "beacon_indexer_unknown_fork_total",
    "Unknown fork parser fallbacks",
    ["fork"],
)

active_fork_epoch = Gauge(
    "beacon_indexer_active_fork_epoch",
    "Configured epoch for each fork",
    ["network", "fork", "version"],
)

data_column_sidecars_fetched_total = Counter(
    "beacon_indexer_data_column_sidecars_fetched_total",
    "Data column sidecar payloads fetched",
    ["status"],
)

data_column_sidecars_transformed_total = Counter(
    "beacon_indexer_data_column_sidecars_transformed_total",
    "Data column sidecar rows transformed",
)


_health_state: Dict[str, Any] = {
    "status": "starting",
    "clickhouse_connected": False,
    "beacon_api_connected": False,
    "operation": "",
}
_health_lock = threading.Lock()
_metrics_server = None


def update_health(**kwargs):
    """Update health state. Thread-safe."""
    with _health_lock:
        _health_state.update(kwargs)


def get_health() -> Dict[str, Any]:
    """Get current health state. Thread-safe."""
    with _health_lock:
        return dict(_health_state)


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for /metrics and /health."""

    def do_GET(self):
        if self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
            return

        if self.path == "/health":
            health = get_health()
            status_code = 200 if health.get("status") not in {"failed", "error"} else 503
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(health, default=str).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        """Suppress default HTTP access logging."""
        pass


def start_metrics_server(port: int = 9090):
    """Start the metrics/health server once in a background thread."""
    global _metrics_server
    if _metrics_server is not None:
        return _metrics_server

    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _metrics_server = server
    logging.getLogger(__name__).info("Metrics server started on port %s", port)
    return server
