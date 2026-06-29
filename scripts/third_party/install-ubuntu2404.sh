#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

export DEBIAN_FRONTEND=noninteractive
export TZ="${TZ:-UTC}"

if [ -f "${THIRD_PARTY_ROOT}/system-packages.ubuntu2404.txt" ]; then
    run_privileged apt-get update
    mapfile -t system_packages < "${THIRD_PARTY_ROOT}/system-packages.ubuntu2404.txt"
    run_privileged apt-get install -y --no-install-recommends "${system_packages[@]}"
fi

"${SCRIPT_DIR}/fetch.sh"

"${SCRIPT_DIR}/build-ubuntu2404.sh"
