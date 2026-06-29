#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_SUBSTRATE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
THIRD_PARTY_ROOT="${ELOQ_THIRD_PARTY_ROOT:-${DATA_SUBSTRATE_ROOT}/third_party}"
THIRD_PARTY_SRC="${ELOQ_THIRD_PARTY_SRC:-${THIRD_PARTY_ROOT}/src}"
THIRD_PARTY_BUILD="${ELOQ_THIRD_PARTY_BUILD:-${THIRD_PARTY_ROOT}/build}"
THIRD_PARTY_PREFIX="${ELOQ_THIRD_PARTY_PREFIX:-${THIRD_PARTY_ROOT}/install}"
THIRD_PARTY_CACHE="${ELOQ_THIRD_PARTY_CACHE:-${THIRD_PARTY_ROOT}/cache}"

export CMAKE_PREFIX_PATH="${THIRD_PARTY_PREFIX}:${CMAKE_PREFIX_PATH:-}"
export PKG_CONFIG_PATH="${THIRD_PARTY_PREFIX}/lib/pkgconfig:${THIRD_PARTY_PREFIX}/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
export LD_LIBRARY_PATH="${THIRD_PARTY_PREFIX}/lib:${THIRD_PARTY_PREFIX}/lib64:${LD_LIBRARY_PATH:-}"
export C_INCLUDE_PATH="${THIRD_PARTY_PREFIX}/include:${C_INCLUDE_PATH:-}"
export CPLUS_INCLUDE_PATH="${THIRD_PARTY_PREFIX}/include:${CPLUS_INCLUDE_PATH:-}"
export LIBRARY_PATH="${THIRD_PARTY_PREFIX}/lib:${THIRD_PARTY_PREFIX}/lib64:${LIBRARY_PATH:-}"
export PATH="${THIRD_PARTY_PREFIX}/bin:${PATH}"

run() {
    printf '+ %q' "$@"
    printf '\n'
    "$@"
}

run_with_retry() {
    local max_retries=5
    local attempt=1

    while [ "${attempt}" -le "${max_retries}" ]; do
        if run "$@"; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    return 1
}

run_privileged() {
    if [ "$(id -u)" -eq 0 ]; then
        run "$@"
    elif command -v sudo >/dev/null 2>&1; then
        run sudo "$@"
    else
        echo "This script must run as root or with sudo available: $*" >&2
        exit 1
    fi
}

ensure_dirs() {
    mkdir -p "${THIRD_PARTY_SRC}" "${THIRD_PARTY_BUILD}" "${THIRD_PARTY_PREFIX}" \
        "${THIRD_PARTY_CACHE}"
}

nproc_or_one() {
    nproc 2>/dev/null || printf '1\n'
}

cmake_build_install() {
    local name="$1"
    local source_dir="$2"
    shift 2

    local build_dir="${THIRD_PARTY_BUILD}/${name}"
    run cmake -S "${source_dir}" -B "${build_dir}" \
        -DCMAKE_INSTALL_PREFIX="${THIRD_PARTY_PREFIX}" \
        -DCMAKE_PREFIX_PATH="${THIRD_PARTY_PREFIX}" \
        "$@"
    run cmake --build "${build_dir}" -- -j"$(nproc_or_one)"
    run cmake --install "${build_dir}"
}

source_marker() {
    local source_dir="$1"
    printf '%s/.eloq-source\n' "${source_dir}"
}

clear_source_dir() {
    local source_dir="$1"
    rm -rf "${source_dir}"
    mkdir -p "${source_dir}"
}

fetch_archive_source() {
    local name="$1"
    local url="$2"
    local source_dir="${THIRD_PARTY_SRC}/${name}"
    fetch_archive_to_dir "${name}" "${url}" "${source_dir}"
}

fetch_archive_to_dir() {
    local name="$1"
    local url="$2"
    local source_dir="$3"
    local marker
    marker="$(source_marker "${source_dir}")"

    if [ -f "${marker}" ] && grep -qx "archive ${url}" "${marker}"; then
        return 0
    fi

    clear_source_dir "${source_dir}"
    local archive_hash
    archive_hash="$(printf '%s' "${url}" | sha256sum)"
    archive_hash="${archive_hash%% *}"
    local archive="${THIRD_PARTY_CACHE}/${name//\//_}-${archive_hash}.tar.gz"
    if [ ! -f "${archive}" ]; then
        run_with_retry curl -fL "${url}" -o "${archive}.tmp"
        run mv "${archive}.tmp" "${archive}"
    fi
    run tar -xzf "${archive}" -C "${source_dir}" --strip-components=1
    printf 'archive %s\n' "${url}" > "${marker}"
}

fetch_git_source() {
    local name="$1"
    local repo="$2"
    local ref="$3"
    shift 3

    local source_dir="${THIRD_PARTY_SRC}/${name}"
    local depth="${ELOQ_THIRD_PARTY_GIT_DEPTH:-1}"
    local filter="${ELOQ_THIRD_PARTY_GIT_FILTER:-blob:none}"
    if [ ! -d "${source_dir}/.git" ]; then
        rm -rf "${source_dir}"
        mkdir -p "${source_dir}"
        run git -C "${source_dir}" init
        run git -C "${source_dir}" remote add origin "${repo}"
    fi

    local fetch_args=(fetch --no-tags)
    if [ "${depth}" != "0" ]; then
        fetch_args+=(--depth "${depth}")
    fi
    if [ -n "${filter}" ]; then
        fetch_args+=(--filter "${filter}")
    fi
    fetch_args+=(origin "${ref}")
    run_with_retry git -C "${source_dir}" "${fetch_args[@]}"
    run git -C "${source_dir}" checkout --force FETCH_HEAD

    if [ "$#" -gt 0 ]; then
        local submodule_args=(submodule update --init --recommend-shallow)
        if [ "${ELOQ_THIRD_PARTY_SUBMODULE_DEPTH:-1}" != "0" ]; then
            submodule_args+=(--depth "${ELOQ_THIRD_PARTY_SUBMODULE_DEPTH:-1}")
        fi
        if [ -n "${ELOQ_THIRD_PARTY_SUBMODULE_FILTER-blob:none}" ]; then
            submodule_args+=(--filter "${ELOQ_THIRD_PARTY_SUBMODULE_FILTER-blob:none}")
        fi
        run_with_retry git -C "${source_dir}" "${submodule_args[@]}" "$@"
    fi
}

apply_source_patch() {
    local name="$1"
    local patch_file="$2"
    local source_dir="${THIRD_PARTY_SRC}/${name}"

    # The source is an extracted tarball that lives under data_substrate, whose
    # .git is a gitlink pointing at a gitdir that doesn't exist in the docker
    # build context. `git apply` doesn't need a repo to patch files, but it
    # aborts when it discovers that broken gitlink while searching upward. Stop
    # the upward search at THIRD_PARTY_SRC so it just patches the files.
    local -x GIT_CEILING_DIRECTORIES="${THIRD_PARTY_SRC}"

    if git -C "${source_dir}" apply --reverse --check "${patch_file}" >/dev/null 2>&1; then
        return 0
    fi

    run git -C "${source_dir}" apply --check "${patch_file}"
    run git -C "${source_dir}" apply "${patch_file}"
}

cleanup_source_after_build() {
    local name="$1"
    if [ "${ELOQ_THIRD_PARTY_KEEP_SOURCES:-0}" = "1" ]; then
        return 0
    fi
    cd "${DATA_SUBSTRATE_ROOT}"
    run rm -rf "${THIRD_PARTY_SRC:?}/${name}"
}
