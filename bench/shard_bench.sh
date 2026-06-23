#!/bin/bash
set -euo pipefail

cat >&2 <<'MSG'
bench/shard_bench.sh is obsolete.

FerricStore standalone mode now uses the Ferric native binary protocol. Replace
this script with a native-protocol shard benchmark before using it for
regression tracking.
MSG

exit 1
