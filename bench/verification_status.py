"""Report completion/failure markers from an owned long-running verification log."""
import re
import sys
from pathlib import Path

for filename in sys.argv[1:]:
    lines = Path(filename).read_text(errors="replace").splitlines()
    markers = [line for line in lines if re.search(r"Result:|Finished in|Failed:|\d+\) test|^==> ", line)]
    print(filename, "lines", len(lines))
    print("\n".join(markers[-15:]) or "No completion markers yet")
