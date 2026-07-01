#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ensure_dirs

RETAINED_THIRD_PARTY_SUBMODULES=(
    third_party/src/brpc
    third_party/src/braft
    third_party/src/mimalloc
    third_party/src/rocksdb-cloud
)

AWS_CRT_SUBMODULES=(
    crt/aws-c-common
    crt/aws-c-io
    crt/aws-c-compression
    crt/aws-c-cal
    crt/aws-c-auth
    crt/aws-c-http
    crt/aws-c-mqtt
    crt/s2n
    crt/aws-checksums
    crt/aws-c-event-stream
    crt/aws-c-s3
    crt/aws-lc
    crt/aws-c-sdkutils
)

fetch_retained_submodules() {
    if git -C "${DATA_SUBSTRATE_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        local submodule_args=(submodule update --init --recommend-shallow)
        if [ "${ELOQ_THIRD_PARTY_SUBMODULE_DEPTH:-1}" != "0" ]; then
            submodule_args+=(--depth "${ELOQ_THIRD_PARTY_SUBMODULE_DEPTH:-1}")
        fi
        if [ -n "${ELOQ_THIRD_PARTY_SUBMODULE_FILTER-blob:none}" ]; then
            submodule_args+=(--filter "${ELOQ_THIRD_PARTY_SUBMODULE_FILTER-blob:none}")
        fi
        run_with_retry git -C "${DATA_SUBSTRATE_ROOT}" \
            "${submodule_args[@]}" "${RETAINED_THIRD_PARTY_SUBMODULES[@]}"
        return 0
    fi

    local submodule_path
    for submodule_path in "${RETAINED_THIRD_PARTY_SUBMODULES[@]}"; do
        if [ ! -d "${DATA_SUBSTRATE_ROOT}/${submodule_path}" ] || \
           [ -z "$(find "${DATA_SUBSTRATE_ROOT}/${submodule_path}" -mindepth 1 -maxdepth 1 | head -n 1)" ]; then
            echo "Missing retained third-party source: ${submodule_path}" >&2
            echo "Initialize retained data_substrate submodules before copying this tree." >&2
            exit 1
        fi
    done
}

fetch_upstream_archives() {
    fetch_archive_source lua \
        https://codeload.github.com/lua/lua/tar.gz/6443185167c77adcc8552a3fee7edab7895db1a9
    fetch_archive_source protobuf \
        https://codeload.github.com/protocolbuffers/protobuf/tar.gz/f0dc78d7e6e331b8c6bb2d5283e06aa26883ca7
    fetch_archive_source glog \
        https://codeload.github.com/google/glog/tar.gz/3a0d4d22c5ae0b9a2216988411cfa6bf860cc372
    fetch_archive_source liburing \
        https://codeload.github.com/axboe/liburing/tar.gz/f7dcc1ea60819475dffd3a45059e16f04381bee7

    fetch_archive_source cuckoofilter \
        https://codeload.github.com/efficient/cuckoofilter/tar.gz/917583d6abef692dfa8e14453bd77d6e0b61eef3
    apply_source_patch cuckoofilter \
        "${THIRD_PARTY_ROOT}/patches/cuckoofilter/0001-eloq-build-fixes.patch"

    fetch_archive_source rocksdb \
        https://codeload.github.com/facebook/rocksdb/tar.gz/bcf88d48ce8aa8b536aee4dd305533b3b83cf435
    fetch_archive_source prometheus-cpp \
        https://codeload.github.com/jupp0r/prometheus-cpp/tar.gz/c9ffcdda9086ffd9e1283ea7a0276d831f3c8a8d
    fetch_archive_to_dir prometheus-cpp/3rdparty/civetweb \
        https://codeload.github.com/civetweb/civetweb/tar.gz/eefb26f82b233268fc98577d265352720d477ba4 \
        "${THIRD_PARTY_SRC}/prometheus-cpp/3rdparty/civetweb"
    fetch_archive_source Catch2 \
        https://codeload.github.com/catchorg/Catch2/tar.gz/3f0283de7a9c43200033da996ff9093be3ac84dc
    fetch_archive_source abseil-cpp \
        https://codeload.github.com/abseil/abseil-cpp/tar.gz/29bf8085f3bf17b84d30e34b3d7ff8248fda404e
    fetch_archive_source re2 \
        https://codeload.github.com/google/re2/tar.gz/960c861764ff54c9a12ff683ba55ccaad1a8f73b
    fetch_archive_source grpc \
        https://codeload.github.com/grpc/grpc/tar.gz/0a82c02a9b817a53574994374dcff53f2e961df2
    fetch_archive_source crc32c \
        https://codeload.github.com/google/crc32c/tar.gz/02e65f4fd3065d27b2e29324800ca6d04df16126
    fetch_archive_source nlohmann-json \
        https://codeload.github.com/nlohmann/json/tar.gz/bc889afb4c5bf1c0d8ee29ef35eaaf4c8bef8a5d
    fetch_archive_source google-cloud-cpp \
        https://codeload.github.com/googleapis/google-cloud-cpp/tar.gz/f55d86a04fc378fff7eebb29fcd44aac7dbcc32f
    fetch_archive_source FakeIt \
        https://codeload.github.com/eranpeer/FakeIt/tar.gz/70180b9870d4f2487fcf8feeaba14ffbb69a14f7

    fetch_archive_source usearch \
        https://codeload.github.com/unum-cloud/usearch/tar.gz/fd6279af6bc205baab1e0ad48651cc0f875cdb7d
    fetch_archive_to_dir usearch/fp16 \
        https://codeload.github.com/maratyszcza/fp16/tar.gz/0a92994d729ff76a58f692d3028ca1b64b145d91 \
        "${THIRD_PARTY_SRC}/usearch/fp16"

    fetch_archive_source yaml-cpp \
        https://codeload.github.com/jbeder/yaml-cpp/tar.gz/0579ae3d976091d7d664aa9d2527e0d0cff25763
}

fetch_aws_sdk() {
    fetch_git_source aws-sdk-cpp \
        https://github.com/aws/aws-sdk-cpp.git \
        b5153b8309526738d498ee86a73ccc386fee0979 \
        crt/aws-crt-cpp

    run_with_retry git -C "${THIRD_PARTY_SRC}/aws-sdk-cpp/crt/aws-crt-cpp" \
        submodule update --init --recommend-shallow --depth 1 --filter blob:none \
        "${AWS_CRT_SUBMODULES[@]}"
}

fetch_retained_submodules
fetch_upstream_archives
fetch_aws_sdk
