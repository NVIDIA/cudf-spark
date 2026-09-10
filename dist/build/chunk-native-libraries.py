# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import shutil

from java.io import FileInputStream, FileOutputStream
from java.util.zip import CRC32
from jarray import zeros


COPY_BUFFER_SIZE = 1024 * 1024
MANIFEST_SUFFIX = ".chunks.properties"
CHUNK_DIRECTORY_SUFFIX = ".chunks"
NATIVE_SUFFIXES = (".so", ".dylib", ".dll")
EMPTY_INCLUDE_PATTERN = "__no_native_chunks__"
BYTE_SIZE_RE = re.compile(r"^\s*(\d+)\s*([KMGT]?)B?\s*$", re.IGNORECASE)
BYTE_SIZE_MULTIPLIERS = {
    "": 1,
    "K": 1024,
    "M": 1024 ** 2,
    "G": 1024 ** 3,
    "T": 1024 ** 4,
}


def parse_byte_size(value):
    match = BYTE_SIZE_RE.match(str(value))
    if not match:
        raise ValueError("Invalid byte size: %r" % value)
    amount, suffix = match.groups()
    return long(amount) * BYTE_SIZE_MULTIPLIERS[suffix.upper()]


def ensure_directory(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def remove_path_ignoring_errors(path):
    try:
        remove_path(path)
    except:
        pass


def native_libraries(root_dir, minimum_size):
    candidates = []
    for arch in sorted(os.listdir(root_dir)):
        arch_dir = os.path.join(root_dir, arch)
        if not os.path.isdir(arch_dir):
            continue
        for operating_system in sorted(os.listdir(arch_dir)):
            os_dir = os.path.join(arch_dir, operating_system)
            if not os.path.isdir(os_dir):
                continue
            for name in sorted(os.listdir(os_dir)):
                path = os.path.join(os_dir, name)
                if (os.path.isfile(path) and name.endswith(NATIVE_SUFFIXES)
                        and os.path.getsize(path) >= minimum_size):
                    candidates.append(path)
    return candidates


def split_library(root_dir, library_path, chunk_size):
    relative_library = os.path.relpath(library_path, root_dir).replace(os.sep, "/")
    source_stat = os.stat(library_path)
    library_size = source_stat.st_size
    chunk_dir = library_path + CHUNK_DIRECTORY_SUFFIX
    temporary_chunk_dir = chunk_dir + ".tmp"
    manifest_path = library_path + MANIFEST_SUFFIX
    temporary_manifest = manifest_path + ".tmp"

    remove_path(temporary_chunk_dir)
    remove_path(temporary_manifest)
    if os.path.exists(chunk_dir) or os.path.exists(manifest_path):
        raise RuntimeError("Chunk output already exists for %s" % relative_library)
    ensure_directory(temporary_chunk_dir)

    source = FileInputStream(library_path)
    buffer = zeros(COPY_BUFFER_SIZE, "b")
    deflated_entries = []
    chunk_crc32 = []
    chunk_count = 0
    total_bytes = 0
    try:
        while total_bytes < library_size:
            chunk_name = "%05d" % chunk_count
            chunk_path = os.path.join(temporary_chunk_dir, chunk_name)
            chunk_output = FileOutputStream(chunk_path)
            chunk_crc = CRC32()
            chunk_bytes = 0
            expected = min(chunk_size, library_size - total_bytes)
            try:
                while chunk_bytes < expected:
                    requested = int(min(COPY_BUFFER_SIZE, expected - chunk_bytes))
                    count = source.read(buffer, 0, requested)
                    if count < 0:
                        raise RuntimeError("Unexpected end of native library %s" % relative_library)
                    if count:
                        chunk_output.write(buffer, 0, count)
                        chunk_crc.update(buffer, 0, count)
                        chunk_bytes += count
                        total_bytes += count
            finally:
                chunk_output.close()

            os.utime(chunk_path, (source_stat.st_atime, source_stat.st_mtime))
            final_relative = (
                relative_library + CHUNK_DIRECTORY_SUFFIX + "/" + chunk_name)
            deflated_entries.append(final_relative)
            chunk_crc32.append(chunk_crc.getValue())
            chunk_count += 1
    except:
        remove_path_ignoring_errors(temporary_chunk_dir)
        remove_path_ignoring_errors(temporary_manifest)
        raise
    finally:
        source.close()

    if total_bytes != library_size:
        remove_path(temporary_chunk_dir)
        raise RuntimeError(
            "Native library changed while chunking %s: expected %d bytes, read %d"
            % (relative_library, library_size, total_bytes))

    manifest = (
        "format.version=1\n"
        "library.size=%d\n"
        "chunk.size=%d\n"
        "chunk.count=%d\n"
        % (library_size, chunk_size, chunk_count))
    manifest += "".join(
        "chunk.%05d.crc32=%08x\n" % (index, value)
        for index, value in enumerate(chunk_crc32))
    manifest_output = open(temporary_manifest, "w")
    try:
        manifest_output.write(manifest)
    finally:
        manifest_output.close()
    os.utime(temporary_manifest, (source_stat.st_atime, source_stat.st_mtime))

    # Any failure here aborts the Maven execution before JAR creation. The initialize-phase
    # cleanup removes partial parallel-world output before the next invocation.
    os.rename(temporary_chunk_dir, chunk_dir)
    os.rename(temporary_manifest, manifest_path)
    os.remove(library_path)
    return relative_library, deflated_entries


def write_lines(path, values):
    output = open(path, "w")
    try:
        if not values:
            output.write(EMPTY_INCLUDE_PATTERN)
            output.write("\n")
        for value in sorted(values):
            output.write(value)
            output.write("\n")
    finally:
        output.close()


root_dir = attributes.get("root_dir")
metadata_dir = attributes.get("metadata_dir")
minimum_size = parse_byte_size(attributes.get("minimum_size"))
chunk_size = parse_byte_size(attributes.get("chunk_size"))

if minimum_size <= 0 or chunk_size <= 0:
    raise RuntimeError("Native chunk sizes must be positive")

ensure_directory(metadata_dir)
deflated_entries = []
manifests = []
for library_path in native_libraries(root_dir, minimum_size):
    relative_library, library_deflated = split_library(
        root_dir, library_path, chunk_size)
    deflated_entries.extend(library_deflated)
    manifests.append(relative_library + MANIFEST_SUFFIX)
    self.log(
        "Chunked %s into %d DEFLATED entries"
        % (relative_library, len(library_deflated)))

write_lines(os.path.join(metadata_dir, "deflated-chunks.list"), deflated_entries)
write_lines(os.path.join(metadata_dir, "chunk-manifests.list"), manifests)
if manifests:
    marker = open(os.path.join(metadata_dir, "enabled"), "w")
    marker.close()
