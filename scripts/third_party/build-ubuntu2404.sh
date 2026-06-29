#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ensure_dirs

JOBS="$(nproc_or_one)"
ABSEIL_OPTIONS_BACKUP="${THIRD_PARTY_BUILD}/abseil-options.h.restore"
BRAFT_CMAKE_BACKUP="${THIRD_PARTY_BUILD}/braft-CMakeLists.txt.restore"

restore_patched_sources() {
    if [ -f "${ABSEIL_OPTIONS_BACKUP}" ]; then
        mv "${ABSEIL_OPTIONS_BACKUP}" \
            "${THIRD_PARTY_SRC}/abseil-cpp/absl/base/options.h"
    fi

    if [ -f "${BRAFT_CMAKE_BACKUP}" ]; then
        mv "${BRAFT_CMAKE_BACKUP}" \
            "${THIRD_PARTY_SRC}/braft/CMakeLists.txt"
    fi
}

trap restore_patched_sources EXIT

cd "${THIRD_PARTY_SRC}/lua"
run make -j"${JOBS}" all
mkdir -p "${THIRD_PARTY_PREFIX}/bin" "${THIRD_PARTY_PREFIX}/include" "${THIRD_PARTY_PREFIX}/lib"
run cp lua "${THIRD_PARTY_PREFIX}/bin/lua"
run cp liblua.a "${THIRD_PARTY_PREFIX}/lib/liblua.a"
run cp lua.h luaconf.h lualib.h lauxlib.h "${THIRD_PARTY_PREFIX}/include/"
cleanup_source_after_build lua

cp "${THIRD_PARTY_SRC}/abseil-cpp/absl/base/options.h" \
    "${ABSEIL_OPTIONS_BACKUP}"
sed -i 's/^#define ABSL_OPTION_USE_\(.*\) 2/#define ABSL_OPTION_USE_\1 0/' \
    "${THIRD_PARTY_SRC}/abseil-cpp/absl/base/options.h"
cmake_build_install abseil-cpp "${THIRD_PARTY_SRC}/abseil-cpp" \
    -DCMAKE_BUILD_TYPE=Release \
    -DABSL_BUILD_TESTING=OFF \
    -DBUILD_SHARED_LIBS=ON
restore_patched_sources
if git -C "${THIRD_PARTY_SRC}/abseil-cpp" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    run git -C "${THIRD_PARTY_SRC}/abseil-cpp" diff --exit-code -- absl/base/options.h
fi
cleanup_source_after_build abseil-cpp

cmake_build_install protobuf "${THIRD_PARTY_SRC}/protobuf" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_ABSL_PROVIDER=package
cleanup_source_after_build protobuf

cmake_build_install glog "${THIRD_PARTY_SRC}/glog" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DWITH_GTEST=OFF
cleanup_source_after_build glog

cd "${THIRD_PARTY_SRC}/liburing"
run ./configure --prefix="${THIRD_PARTY_PREFIX}" --cc=gcc --cxx=g++
run make -j"${JOBS}"
run make install
cleanup_source_after_build liburing

run cmake -S "${THIRD_PARTY_SRC}/brpc" -B "${THIRD_PARTY_BUILD}/brpc" \
    -DCMAKE_INSTALL_PREFIX="${THIRD_PARTY_PREFIX}" \
    -DCMAKE_PREFIX_PATH="${THIRD_PARTY_PREFIX}" \
    -DWITH_GLOG=ON \
    -DIO_URING_ENABLED=ON \
    -DBUILD_SHARED_LIBS=ON
run cmake --build "${THIRD_PARTY_BUILD}/brpc" -- -j"${JOBS}"
mkdir -p "${THIRD_PARTY_PREFIX}/include" "${THIRD_PARTY_PREFIX}/lib"
run rsync -a "${THIRD_PARTY_BUILD}/brpc/output/include/" "${THIRD_PARTY_PREFIX}/include/"
run rsync -a "${THIRD_PARTY_BUILD}/brpc/output/lib/" "${THIRD_PARTY_PREFIX}/lib/"

cd "${THIRD_PARTY_SRC}/braft"
cp CMakeLists.txt "${BRAFT_CMAKE_BACKUP}"
sed -i 's/libbrpc.a//g' CMakeLists.txt
run cmake -S . -B "${THIRD_PARTY_BUILD}/braft" \
    -DCMAKE_INSTALL_PREFIX="${THIRD_PARTY_PREFIX}" \
    -DCMAKE_PREFIX_PATH="${THIRD_PARTY_PREFIX}" \
    -DBRPC_WITH_GLOG=ON
run cmake --build "${THIRD_PARTY_BUILD}/braft" -- -j"${JOBS}"
mkdir -p "${THIRD_PARTY_PREFIX}/include" "${THIRD_PARTY_PREFIX}/lib"
run rsync -a "${THIRD_PARTY_BUILD}/braft/output/include/" "${THIRD_PARTY_PREFIX}/include/"
run rsync -a "${THIRD_PARTY_BUILD}/braft/output/lib/" "${THIRD_PARTY_PREFIX}/lib/"
restore_patched_sources
if git -C "${THIRD_PARTY_SRC}/braft" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    run git -C "${THIRD_PARTY_SRC}/braft" diff --exit-code -- CMakeLists.txt
fi

cmake_build_install mimalloc "${THIRD_PARTY_SRC}/mimalloc" \
    -DCMAKE_BUILD_TYPE=Release

cd "${THIRD_PARTY_SRC}/cuckoofilter"
run make PREFIX="${THIRD_PARTY_PREFIX}" install
cleanup_source_after_build cuckoofilter

cmake_build_install aws-sdk-cpp "${THIRD_PARTY_SRC}/aws-sdk-cpp" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DENABLE_TESTING=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DFORCE_SHARED_CRT=OFF \
    -DBUILD_ONLY="dynamodb;sqs;s3;kinesis;kafka;transfer"
cleanup_source_after_build aws-sdk-cpp

cd "${THIRD_PARTY_SRC}/rocksdb"
run make -j"${JOBS}" shared_lib USE_RTTI=1 PORTABLE=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1
run make install-shared PREFIX="${THIRD_PARTY_PREFIX}"
cleanup_source_after_build rocksdb

cmake_build_install prometheus-cpp "${THIRD_PARTY_SRC}/prometheus-cpp" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DENABLE_PUSH=OFF \
    -DENABLE_TESTING=OFF
cleanup_source_after_build prometheus-cpp

cmake_build_install Catch2 "${THIRD_PARTY_SRC}/Catch2" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCATCH_BUILD_EXAMPLES=OFF \
    -DBUILD_TESTING=OFF
cleanup_source_after_build Catch2

cmake_build_install re2 "${THIRD_PARTY_SRC}/re2" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DRE2_BUILD_TESTING=OFF
cleanup_source_after_build re2

cmake_build_install grpc "${THIRD_PARTY_SRC}/grpc" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DgRPC_ABSL_PROVIDER=package \
    -DgRPC_CARES_PROVIDER=package \
    -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package \
    -DgRPC_SSL_PROVIDER=package \
    -DgRPC_ZLIB_PROVIDER=package
cleanup_source_after_build grpc

cmake_build_install crc32c "${THIRD_PARTY_SRC}/crc32c" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DCRC32C_BUILD_TESTS=OFF \
    -DCRC32C_BUILD_BENCHMARKS=OFF \
    -DCRC32C_USE_GLOG=OFF
cleanup_source_after_build crc32c

cmake_build_install nlohmann-json "${THIRD_PARTY_SRC}/nlohmann-json" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_TESTING=OFF \
    -DJSON_BuildTests=OFF
cleanup_source_after_build nlohmann-json

cmake_build_install google-cloud-cpp "${THIRD_PARTY_SRC}/google-cloud-cpp" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_TESTING=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
    -DGOOGLE_CLOUD_CPP_ENABLE=bigtable,storage
cleanup_source_after_build google-cloud-cpp

mkdir -p "${THIRD_PARTY_PREFIX}/include"
run rsync -a "${THIRD_PARTY_SRC}/usearch/include/" "${THIRD_PARTY_PREFIX}/include/"
run rsync -a "${THIRD_PARTY_SRC}/usearch/fp16/include/" "${THIRD_PARTY_PREFIX}/include/"
cleanup_source_after_build usearch

mkdir -p "${THIRD_PARTY_PREFIX}/include/catch2"
run cp "${THIRD_PARTY_SRC}/FakeIt/single_header/catch/fakeit.hpp" \
    "${THIRD_PARTY_PREFIX}/include/catch2/fakeit.hpp"
cleanup_source_after_build FakeIt

cmake_build_install yaml-cpp "${THIRD_PARTY_SRC}/yaml-cpp" \
    -DCMAKE_BUILD_TYPE=Release \
    -DYAML_CPP_BUILD_TESTS=OFF \
    -DYAML_CPP_BUILD_TOOLS=OFF \
    -DYAML_BUILD_SHARED_LIBS=ON
cleanup_source_after_build yaml-cpp

cd "${THIRD_PARTY_SRC}/rocksdb-cloud"
run make shared_lib -j"${JOBS}" LIBNAME=librocksdb-cloud-aws USE_RTTI=1 USE_AWS=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1
run make install-shared LIBNAME=librocksdb-cloud-aws PREFIX="${THIRD_PARTY_BUILD}/rocksdb-cloud-aws-output"
mkdir -p "${THIRD_PARTY_PREFIX}/include/rocksdb_cloud_header" "${THIRD_PARTY_PREFIX}/lib"
run rsync -a "${THIRD_PARTY_BUILD}/rocksdb-cloud-aws-output/include/" "${THIRD_PARTY_PREFIX}/include/rocksdb_cloud_header/"
run rsync -a "${THIRD_PARTY_BUILD}/rocksdb-cloud-aws-output/lib/" "${THIRD_PARTY_PREFIX}/lib/"
run make clean
run make shared_lib -j"${JOBS}" LIBNAME=librocksdb-cloud-gcp USE_RTTI=1 USE_GCP=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1
run make install-shared LIBNAME=librocksdb-cloud-gcp PREFIX="${THIRD_PARTY_BUILD}/rocksdb-cloud-gcp-output"
run rsync -a "${THIRD_PARTY_BUILD}/rocksdb-cloud-gcp-output/lib/" "${THIRD_PARTY_PREFIX}/lib/"

mkdir -p "${THIRD_PARTY_PREFIX}/share/eloq"
run cp "${THIRD_PARTY_ROOT}/manifest.yml" "${THIRD_PARTY_PREFIX}/share/eloq/third_party-manifest.yml"

printf 'Third-party dependencies installed to %s\n' "${THIRD_PARTY_PREFIX}"
