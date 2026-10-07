"""Archive the isolated candidate without editing production sources."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent.parent
paths = [
    "apps/ferricstore/lib/ferricstore/store/compaction_plan.ex",
    "apps/ferricstore/lib/ferricstore/store/promotion.ex",
    "apps/ferricstore/lib/ferricstore/store/shard/compound/separated_compaction.ex",
    "apps/ferricstore/lib/ferricstore/store/shard/info.ex",
    "apps/ferricstore/lib/ferricstore/store/shard/startup.ex",
    "apps/ferricstore/lib/ferricstore/raft/waraft_storage/sections/snapshot_metadata.ex",
]
sources = {path: (root / path).read_text() for path in paths}
result = {"sources": sources, "experimental": True, "default_accepted": False,
          "sha256": {path: hashlib.sha256(source.encode()).hexdigest() for path, source in sources.items()}}
output = root / "bench/results/separate-output-candidate-source.json"
if output.exists():
    raise ValueError("archive already exists")
output.write_text(json.dumps(result, indent=2) + "\n")
print("Archived isolated candidate source")
