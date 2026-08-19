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

from java.util import Properties
from java.util.zip import CRC32, ZipEntry, ZipFile
from jarray import zeros


COPY_BUFFER_SIZE = 1024 * 1024
MANIFEST_SUFFIX = ".chunks.properties"
EMPTY_INCLUDE_PATTERN = "__no_native_chunks__"


def read_lines(path):
    source = open(path, "r")
    try:
        return set(
            line.strip() for line in source
            if line.strip() and line.strip() != EMPTY_INCLUDE_PATTERN)
    finally:
        source.close()


def require_property(properties, key, manifest_name):
    value = properties.getProperty(key)
    if value is None or not value.strip():
        raise RuntimeError("Missing %s in %s" % (key, manifest_name))
    return value.strip()


jar_path = attributes.get("jar_path")
metadata_dir = attributes.get("metadata_dir")
deflated_entries = read_lines(os.path.join(metadata_dir, "deflated-chunks.list"))
manifest_entries = read_lines(os.path.join(metadata_dir, "chunk-manifests.list"))
expected_chunks = deflated_entries
verified_chunks = set()

archive = ZipFile(jar_path)
buffer = zeros(COPY_BUFFER_SIZE, "b")
try:
    for entry_name in sorted(deflated_entries):
        entry = archive.getEntry(entry_name)
        if entry is None:
            raise RuntimeError("Missing DEFLATED native chunk %s" % entry_name)
        if entry.getMethod() != ZipEntry.DEFLATED:
            raise RuntimeError("Native chunk %s is not DEFLATED" % entry_name)

    for manifest_name in sorted(manifest_entries):
        manifest_entry = archive.getEntry(manifest_name)
        if manifest_entry is None:
            raise RuntimeError("Missing native chunk manifest %s" % manifest_name)
        properties = Properties()
        manifest_input = archive.getInputStream(manifest_entry)
        try:
            properties.load(manifest_input)
        finally:
            manifest_input.close()

        if require_property(properties, "format.version", manifest_name) != "1":
            raise RuntimeError("Unsupported chunk manifest version in %s" % manifest_name)
        library_size = long(require_property(properties, "library.size", manifest_name))
        chunk_size = long(require_property(properties, "chunk.size", manifest_name))
        chunk_count = int(require_property(properties, "chunk.count", manifest_name))
        if library_size <= 0:
            raise RuntimeError("Invalid library.size in %s" % manifest_name)
        if chunk_size <= 0:
            raise RuntimeError("Invalid chunk.size in %s" % manifest_name)
        if chunk_count <= 0:
            raise RuntimeError("Invalid chunk.count in %s" % manifest_name)
        expected_count = 1 + ((library_size - 1) // chunk_size)
        if chunk_count != expected_count:
            raise RuntimeError("Invalid chunk count in %s" % manifest_name)

        library_name = manifest_name[:-len(MANIFEST_SUFFIX)]
        if archive.getEntry(library_name) is not None:
            raise RuntimeError(
                "Conventional native resource still exists beside %s" % manifest_name)

        total_size = 0
        for index in range(chunk_count):
            chunk_name = "%s.chunks/%05d" % (library_name, index)
            chunk_crc_key = "chunk.%05d.crc32" % index
            expected_chunk_crc = long(
                require_property(properties, chunk_crc_key, manifest_name), 16)
            chunk_entry = archive.getEntry(chunk_name)
            if chunk_entry is None:
                raise RuntimeError("Missing native chunk %s" % chunk_name)
            expected_size = (
                chunk_size if index < chunk_count - 1
                else library_size - chunk_size * index)
            if chunk_entry.getSize() != expected_size:
                raise RuntimeError(
                    "Native chunk %s has size %d, expected %d"
                    % (chunk_name, chunk_entry.getSize(), expected_size))
            chunk_input = archive.getInputStream(chunk_entry)
            chunk_bytes = 0
            chunk_crc = CRC32()
            try:
                while True:
                    count = chunk_input.read(buffer)
                    if count < 0:
                        break
                    if count:
                        chunk_crc.update(buffer, 0, count)
                        chunk_bytes += count
            finally:
                chunk_input.close()
            if chunk_bytes != expected_size:
                raise RuntimeError(
                    "Native chunk %s read %d bytes, expected %d"
                    % (chunk_name, chunk_bytes, expected_size))
            if chunk_crc.getValue() != expected_chunk_crc:
                raise RuntimeError("Native chunk CRC mismatch for %s" % chunk_name)
            total_size += chunk_bytes
            verified_chunks.add(chunk_name)

        if total_size != library_size:
            raise RuntimeError(
                "Reconstructed native library size mismatch for %s" % library_name)
finally:
    archive.close()

if verified_chunks != expected_chunks:
    raise RuntimeError(
        "Native chunk lists do not match manifests: expected=%d verified=%d"
        % (len(expected_chunks), len(verified_chunks)))
self.log(
    "Verified %d chunked native libraries and %d chunks"
    % (len(manifest_entries), len(verified_chunks)))
