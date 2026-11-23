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

"""Verify every chunked native library in the distribution JAR.

Chunks created by this build and chunks copied from the JNI JAR are checked the same way:
each manifest must describe a complete chunk set whose uncompressed sizes and CRC32 values
match, and no chunk entry may exist without a manifest.
"""

import os
import re

from java.io import File, InputStream, IOException
from java.lang import Class
from java.net import URL, URLClassLoader
from java.util import Properties
from java.util.zip import CRC32, ZipEntry, ZipFile
from jarray import array, zeros


COPY_BUFFER_SIZE = 1024 * 1024
MANIFEST_SUFFIX = ".chunks.properties"
CHUNK_DIRECTORY_SUFFIX = ".chunks/"
EMPTY_INCLUDE_PATTERN = "__no_native_chunks__"
ZSTD_INPUT_STREAM = "com.github.luben.zstd.ZstdInputStreamNoFinalizer"
CRC32_RE = re.compile(r"^[0-9a-fA-F]{8}$")


def read_lines(path):
    if not os.path.exists(path):
        return set()
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


class ZstdDecoder(object):
    """Load zstd-jni lazily from its JAR so builds without zstd chunks do not need it."""

    def __init__(self, jar_path):
        self.jar_path = jar_path
        self.constructor = None

    def open(self, stream):
        if self.constructor is None:
            if not self.jar_path or not os.path.isfile(self.jar_path):
                raise RuntimeError(
                    "Verifying zstd native chunks requires zstd-jni at %s" % self.jar_path)
            loader = URLClassLoader(
                array([File(self.jar_path).toURI().toURL()], URL), None)
            stream_class = Class.forName(ZSTD_INPUT_STREAM, True, loader)
            self.constructor = stream_class.getConstructor([InputStream])
        return self.constructor.newInstance([stream])


def verify_chunk(archive, chunk_entry, chunk_name, expected_size, expected_crc,
                 compression, zstd, buffer):
    if compression == "none" and chunk_entry.getSize() != expected_size:
        raise RuntimeError(
            "Native chunk %s has size %d, expected %d"
            % (chunk_name, chunk_entry.getSize(), expected_size))
    chunk_input = archive.getInputStream(chunk_entry)
    chunk_bytes = 0
    chunk_crc = CRC32()
    try:
        if compression == "zstd":
            chunk_input = zstd.open(chunk_input)
        while True:
            count = chunk_input.read(buffer)
            if count < 0:
                break
            if count:
                chunk_crc.update(buffer, 0, count)
                chunk_bytes += count
    except IOException, error:
        raise RuntimeError("Cannot decompress native chunk %s: %s" % (chunk_name, error))
    finally:
        chunk_input.close()
    if chunk_bytes != expected_size:
        raise RuntimeError(
            "Native chunk %s read %d bytes, expected %d"
            % (chunk_name, chunk_bytes, expected_size))
    if chunk_crc.getValue() != expected_crc:
        raise RuntimeError("Native chunk CRC mismatch for %s" % chunk_name)


jar_path = attributes.get("jar_path")
metadata_dir = attributes.get("metadata_dir")
zstd = ZstdDecoder(attributes.get("zstd_jar"))
deflated_entries = read_lines(os.path.join(metadata_dir, "deflated-chunks.list"))
created_manifests = read_lines(os.path.join(metadata_dir, "chunk-manifests.list"))
verified_chunks = set()

archive = ZipFile(jar_path)
buffer = zeros(COPY_BUFFER_SIZE, "b")
try:
    manifest_entries = set()
    chunk_entries = set()
    entries = archive.entries()
    while entries.hasMoreElements():
        name = entries.nextElement().getName()
        if name.endswith(MANIFEST_SUFFIX):
            manifest_entries.add(name)
        elif CHUNK_DIRECTORY_SUFFIX in name and not name.endswith("/"):
            chunk_entries.add(name)

    missing_manifests = created_manifests - manifest_entries
    if missing_manifests:
        raise RuntimeError(
            "Missing native chunk manifest %s" % sorted(missing_manifests)[0])

    for entry_name in sorted(deflated_entries):
        entry = archive.getEntry(entry_name)
        if entry is None:
            raise RuntimeError("Missing DEFLATED native chunk %s" % entry_name)
        if entry.getMethod() != ZipEntry.DEFLATED:
            raise RuntimeError("Native chunk %s is not DEFLATED" % entry_name)

    for manifest_name in sorted(manifest_entries):
        properties = Properties()
        manifest_input = archive.getInputStream(archive.getEntry(manifest_name))
        try:
            properties.load(manifest_input)
        finally:
            manifest_input.close()

        if require_property(properties, "format.version", manifest_name) != "1":
            raise RuntimeError("Unsupported chunk manifest version in %s" % manifest_name)
        library_size = long(require_property(properties, "library.size", manifest_name))
        chunk_size = long(require_property(properties, "chunk.size", manifest_name))
        chunk_count = int(require_property(properties, "chunk.count", manifest_name))
        compression = properties.getProperty("chunk.compression", "none").strip()
        if compression not in ("none", "zstd"):
            raise RuntimeError(
                "Unsupported chunk.compression %s in %s" % (compression, manifest_name))
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

        for index in range(chunk_count):
            chunk_name = "%s.chunks/%05d" % (library_name, index)
            chunk_entry = archive.getEntry(chunk_name)
            if chunk_entry is None:
                raise RuntimeError("Missing native chunk %s" % chunk_name)
            crc_key = "chunk.%05d.crc32" % index
            crc_value = require_property(properties, crc_key, manifest_name)
            if not CRC32_RE.match(crc_value):
                raise RuntimeError("Invalid %s in %s" % (crc_key, manifest_name))
            expected_crc = long(crc_value, 16)
            expected_size = (
                chunk_size if index < chunk_count - 1
                else library_size - chunk_size * index)
            verify_chunk(archive, chunk_entry, chunk_name, expected_size, expected_crc,
                         compression, zstd, buffer)
            verified_chunks.add(chunk_name)
finally:
    archive.close()

unexpected_deflated = deflated_entries - verified_chunks
if unexpected_deflated:
    raise RuntimeError(
        "DEFLATED native chunk %s has no manifest" % sorted(unexpected_deflated)[0])
orphan_chunks = chunk_entries - verified_chunks
if orphan_chunks:
    raise RuntimeError(
        "Native chunk %s is not described by its manifest" % sorted(orphan_chunks)[0])
self.log(
    "Verified %d chunked native libraries and %d chunks"
    % (len(manifest_entries), len(verified_chunks)))
