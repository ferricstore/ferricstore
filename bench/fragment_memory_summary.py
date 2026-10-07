"""Summarize only the final, aligned fragment/frame metadata accounting trials."""

import hashlib
import json
import re
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench/results"


def metrics(rows, names):
    return {
        name: {
            "median": median(row[name] for row in rows),
            "min": min(row[name] for row in rows),
            "max": max(row[name] for row in rows),
        }
        for name in names
    }


memory = json.loads((RESULTS / "fragment-memory-aligned-perf.json").read_text())
frame_source = (
    ROOT / "apps/ferricstore_server/lib/ferricstore_server/native/connection/frame_buffer.ex"
).read_text()
if memory["current_source"] != frame_source:
    raise ValueError("component trials do not match the current frame buffer source")
metadata_bytes = int(re.search(r"@retained_metadata_bytes (\d+)", frame_source)[1])
connection_source = (
    ROOT / "apps/ferricstore_server/lib/ferricstore_server/native/connection.ex"
).read_text()
baseline = connection_source
for old, new in (
    (
        "FrameBuffer.retained_bytes(buffer_stats) + state.decoded_retained_bytes +\n"
        "      state.queued_request_bytes",
        "buffer_stats.buffered_bytes + state.decoded_retained_bytes + state.queued_request_bytes",
    ),
    (
        "FrameBuffer.retained_frame_bytes(byte_size(body(frame)))",
        "FrameBuffer.frame_bytes(byte_size(body(frame)))",
    ),
):
    if baseline.count(old) != 1:
        raise ValueError("socket baseline transform no longer matches the source")
    baseline = baseline.replace(old, new)

summary = {
    "aggregation": "medians and ranges of trial metrics, not pooled percentiles",
    "metadata_bytes_per_fragment_or_frame": metadata_bytes,
    "frame_buffer_source_sha256": hashlib.sha256(frame_source.encode()).hexdigest(),
    "component": {},
    "socket": {},
}
for size in sorted({row["chunk_bytes"] for row in memory["results"]}):
    scenario = {}
    for variant in ("payload_only", "metadata"):
        rows = [
            row for row in memory["results"]
            if row["chunk_bytes"] == size and row["variant"] == variant
        ]
        if len(rows) != 5 or {row["trial"] for row in rows} != set(range(1, 6)):
            raise ValueError("expected five distinct component trials per scenario")
        for row in rows:
            expected = 16_408 + (row["fragments"] * metadata_bytes if variant == "metadata" else 0)
            if row["charge_bytes"] != expected:
                raise ValueError("unexpected admission charge in component trial")
        scenario[variant] = metrics(rows, ("us_per_frame", "reductions_per_frame", "charge_bytes"))
    summary["component"][str(size)] = scenario

socket_rows = []
for variant, source in (("baseline", baseline), ("current", connection_source)):
    expected_hash = hashlib.sha256(source.encode()).hexdigest()
    for trial in range(1, 4):
        report = json.loads((RESULTS / f"fragment-socket-aligned-{variant}-{trial}.json").read_text())
        if (report["source_sha256"] != expected_hash or report["variant"] != variant
                or report["trial"] != str(trial)):
            raise ValueError("socket trial source or identity mismatch")
        if sorted(row["clients"] for row in report["results"]) != [1, 16]:
            raise ValueError("unexpected socket scenarios")
        socket_rows.extend({**row, "variant": variant} for row in report["results"])
for clients in (1, 16):
    scenario = {
        variant: metrics(
            [row for row in socket_rows if row["clients"] == clients and row["variant"] == variant],
            ("ops_per_second", "p50_us", "p95_us", "p99_us"),
        )
        for variant in ("baseline", "current")
    }
    scenario["median_change_pct"] = {
        name: 100 * (scenario["current"][name]["median"] / scenario["baseline"][name]["median"] - 1)
        for name in scenario["baseline"]
    }
    summary["socket"][str(clients)] = scenario

(RESULTS / "fragment-memory-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
