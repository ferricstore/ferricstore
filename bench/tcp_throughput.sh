#!/bin/bash
set -euo pipefail

cat >&2 <<'MSG'
bench/tcp_throughput.sh is obsolete.

FerricStore standalone mode now uses the Ferric native binary protocol. Replace
this script with a native-protocol SDK/client benchmark before using it for
regression tracking.
MSG

exit 1
