#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 || -z "$1" ]]; then
  echo "usage: scripts/smoke-docker-image.sh IMAGE" >&2
  exit 2
fi

image="$1"
name="${FERRICSTORE_SMOKE_CONTAINER:-ferricstore-image-smoke-$$-$RANDOM}"

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    docker logs "$name" >&2 2>/dev/null || true
  fi
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$name" >/dev/null 2>&1 || true
docker run --detach --name "$name" \
  --env FERRICSTORE_PROTECTED_MODE=false \
  --publish 127.0.0.1::6388 \
  "$image" >/dev/null

host_port="$(docker port "$name" 6388/tcp | awk -F: 'NR == 1 {print $NF}')"
if [[ -z "$host_port" ]]; then
  echo "failed to resolve the image's native port" >&2
  exit 1
fi

stable_checks=0

for _ in $(seq 1 60); do
  if [[ "$(docker inspect --format '{{.State.Running}}' "$name" 2>/dev/null || true)" != "true" ]]; then
    echo "container exited before completing the native-port stability window" >&2
    exit 1
  fi

  if { exec 3<>"/dev/tcp/127.0.0.1/${host_port}"; } 2>/dev/null; then
    exec 3>&-
    exec 3<&-
    stable_checks=$((stable_checks + 1))
    if [[ "$stable_checks" -ge 15 ]]; then
      echo "FerricStore image kept its native port healthy: $image"
      exit 0
    fi
  else
    stable_checks=0
  fi

  sleep 1
done

echo "container did not open its native port within 60 seconds" >&2
exit 1
