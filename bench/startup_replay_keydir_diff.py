"""Classify key/expiry differences without printing values or key identities."""

import base64
from collections import Counter
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PREFIX = ROOT / "bench/output/startup-replay/startup-replay-keydir-diagnostic"


def load(variant):
    rows = {}
    with Path(f"{PREFIX}-{variant}-1.tsv").open() as stream:
        for line in stream:
            shard, key, expiry = line.rstrip("\n").split("\t")
            rows[(int(shard), base64.b64decode(key))] = int(expiry)
    return rows


def family(identity):
    _shard, key = identity
    if key.startswith(b"f:") and b"}:rtm:" in key:
        return "flow_retention_cleanup_member"
    if key.startswith(b"X:f:") and b"}:h:" in key:
        return "flow_history_entry"
    if key.startswith(b"f:") and b"}:v:p:" in key:
        return "flow_payload_value"
    return "other"


baseline = load("baseline")
candidate = load("candidate")
result = {
    "baseline_count": len(baseline), "candidate_count": len(candidate),
    "baseline_only": dict(Counter(family(key) for key in baseline.keys() - candidate.keys())),
    "candidate_only": dict(Counter(family(key) for key in candidate.keys() - baseline.keys())),
    "changed_expiry": sum(baseline[key] != candidate[key] for key in baseline.keys() & candidate.keys()),
}
print(json.dumps(result, indent=2))
(ROOT / "bench/results/startup-replay-keydir-diff.json").write_text(json.dumps(result, indent=2) + "\n")
