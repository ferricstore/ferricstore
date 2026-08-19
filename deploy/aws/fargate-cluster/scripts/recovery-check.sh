#!/usr/bin/env bash
set -euo pipefail

release_bin="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ferricstore"
export ELIXIR_ERL_OPTIONS="+fnu"

exec "${release_bin}" rpc \
  'IO.puts(if Ferricstore.Cluster.Recovery.ready?(), do: "FERRICSTORE_RECOVERY_READY", else: "FERRICSTORE_RECOVERY_WAIT")'
