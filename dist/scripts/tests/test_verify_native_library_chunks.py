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

"""Jython tests for dist/build/verify-native-library-chunks.py.

Usage: jython -B test_verify_native_library_chunks.py <zstd-jni.jar> [unittest args]
"""

from __future__ import print_function

import os
import shutil
import sys
import tempfile
import unittest

from java.io import File, FileOutputStream
from java.lang import Class
from java.net import URL, URLClassLoader
from java.util import Arrays
from java.util.zip import CRC32, ZipEntry, ZipOutputStream
from jarray import array


ZSTD_JAR = sys.argv.pop(1) if len(sys.argv) > 1 else None
VERIFIER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "build", "verify-native-library-chunks.py")
LIBRARY = "amd64/Linux/libfoo.so"
MANIFEST = LIBRARY + ".chunks.properties"
CHUNK_SIZE = 8
CHUNKS = ["a" * CHUNK_SIZE, "b" * CHUNK_SIZE, "cc"]
VERIFIED = "Verified 1 chunked native libraries and 3 chunks"


def to_bytes(data):
    return array([((ord(c) + 128) % 256) - 128 for c in data], "b")


def crc32(payload):
    value = CRC32()
    value.update(payload)
    return value.getValue()


_zstd_class = []


def zstd_compress(data):
    if not _zstd_class:
        loader = URLClassLoader(array([File(ZSTD_JAR).toURI().toURL()], URL), None)
        _zstd_class.append(Class.forName("com.github.luben.zstd.Zstd", True, loader))
    compress = _zstd_class[0].getMethod("compress", [Class.forName("[B")])
    return compress.invoke(None, [to_bytes(data)])


def chunk_name(library, index):
    return "%s.chunks/%05d" % (library, index)


def write_jar(path, entries):
    """Write (name, data, method) entries so each ZIP method is explicit."""
    output = ZipOutputStream(FileOutputStream(path))
    try:
        for name, data, method in entries:
            payload = to_bytes(data) if isinstance(data, str) else data
            entry = ZipEntry(name)
            entry.setMethod(method)
            if method == ZipEntry.STORED:
                entry.setSize(len(payload))
                entry.setCompressedSize(len(payload))
                entry.setCrc(crc32(payload))
            output.putNextEntry(entry)
            output.write(payload)
            output.closeEntry()
    finally:
        output.close()


def manifest(chunks, compression=None, crcs=None):
    lines = ["format.version=1",
             "library.size=%d" % sum(len(chunk) for chunk in chunks),
             "chunk.size=%d" % CHUNK_SIZE,
             "chunk.count=%d" % len(chunks)]
    if compression is not None:
        lines.append("chunk.compression=%s" % compression)
    for index, chunk in enumerate(chunks):
        value = crcs[index] if crcs else "%08x" % crc32(to_bytes(chunk))
        lines.append("chunk.%05d.crc32=%s" % (index, value))
    return "\n".join(lines) + "\n"


class Attributes(object):
    def __init__(self, values):
        self.values = values

    def get(self, key):
        return self.values.get(key)


class Logger(object):
    def __init__(self):
        self.messages = []

    def log(self, message):
        self.messages.append(message)


class VerifyNativeLibraryChunksTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.jar = os.path.join(self.root, "dist.jar")
        self.metadata = os.path.join(self.root, "native-chunks")
        self.missing_zstd_jar = os.path.join(self.root, "missing-zstd-jni.jar")

    def tearDown(self):
        shutil.rmtree(self.root)

    def verify(self, zstd_jar=None, created=(), deflated=()):
        if created or deflated:
            os.makedirs(self.metadata)
            for name, values in (("chunk-manifests.list", created),
                                 ("deflated-chunks.list", deflated)):
                listing = open(os.path.join(self.metadata, name), "w")
                try:
                    listing.write("".join(value + "\n" for value in values))
                finally:
                    listing.close()
        logger = Logger()
        execfile(VERIFIER, {
            "attributes": Attributes({
                "jar_path": self.jar,
                "metadata_dir": self.metadata,
                "zstd_jar": zstd_jar or self.missing_zstd_jar}),
            "self": logger})
        return logger.messages

    def assert_rejected(self, message, **kwargs):
        try:
            self.verify(**kwargs)
        except RuntimeError as error:
            self.assertIn(message, str(error))
        else:
            self.fail("verifier accepted a bad JAR; expected %r" % message)

    def deflated_chunks(self, chunks=CHUNKS, method=ZipEntry.DEFLATED):
        return [(chunk_name(LIBRARY, index), chunk, method)
                for index, chunk in enumerate(chunks)]

    def zstd_chunks(self):
        return [(chunk_name(LIBRARY, index), zstd_compress(chunk), ZipEntry.STORED)
                for index, chunk in enumerate(CHUNKS)]

    def write_uncompressed(self, extra=(), **manifest_args):
        write_jar(self.jar, [(MANIFEST, manifest(CHUNKS, **manifest_args), ZipEntry.DEFLATED)]
                  + self.deflated_chunks() + list(extra))

    def write_zstd(self, chunks=None, compression="zstd"):
        write_jar(self.jar, [(MANIFEST, manifest(CHUNKS, compression), ZipEntry.DEFLATED)]
                  + (chunks or self.zstd_chunks()))

    def test_uncompressed_chunks_pass_without_zstd_jni_or_metadata(self):
        self.write_uncompressed()
        self.assertEqual([VERIFIED], self.verify())

    def test_build_created_chunks_pass(self):
        self.write_uncompressed()
        self.assertEqual([VERIFIED], self.verify(
            created=[MANIFEST], deflated=[name for name, _, _ in self.deflated_chunks()]))

    def test_zstd_chunks_pass(self):
        self.write_zstd()
        self.assertEqual([VERIFIED], self.verify(zstd_jar=ZSTD_JAR))

    def test_jar_without_chunks_passes(self):
        write_jar(self.jar, [("amd64/Linux/libsmall.so", "small", ZipEntry.DEFLATED)])
        self.assertEqual(["Verified 0 chunked native libraries and 0 chunks"], self.verify())

    def test_rejects_orphan_chunk(self):
        self.write_uncompressed(extra=[(chunk_name(LIBRARY, 3), "x", ZipEntry.DEFLATED)])
        self.assert_rejected("is not described by its manifest")

    def test_rejects_orphan_chunk_without_manifest(self):
        write_jar(self.jar, [(chunk_name("amd64/Linux/libbar.so", 0), "x", ZipEntry.DEFLATED)])
        self.assert_rejected("is not described by its manifest")

    def test_rejects_unsupported_compression(self):
        self.write_zstd(compression="lz4")
        self.assert_rejected("Unsupported chunk.compression lz4", zstd_jar=ZSTD_JAR)

    def test_rejects_empty_compression(self):
        self.write_uncompressed(compression="")
        self.assert_rejected("Unsupported chunk.compression")

    def test_rejects_malformed_crc(self):
        crcs = ["0x%x" % crc32(to_bytes(chunk)) for chunk in CHUNKS]
        self.write_uncompressed(crcs=crcs)
        self.assert_rejected("Invalid chunk.00000.crc32")

    def test_rejects_wrong_crc(self):
        crcs = ["%08x" % crc32(to_bytes(chunk)) for chunk in CHUNKS]
        crcs[1] = "%08x" % (long(crcs[1], 16) ^ 1)
        self.write_uncompressed(crcs=crcs)
        self.assert_rejected("CRC mismatch for %s" % chunk_name(LIBRARY, 1))

    def test_rejects_corrupt_zstd_chunk(self):
        chunks = self.zstd_chunks()
        name, data, method = chunks[1]
        data[len(data) - 1] = data[len(data) - 1] ^ 0x55
        chunks[1] = (name, data, method)
        self.write_zstd(chunks)
        self.assert_rejected(chunk_name(LIBRARY, 1), zstd_jar=ZSTD_JAR)

    def test_rejects_truncated_zstd_chunk(self):
        chunks = self.zstd_chunks()
        name, data, method = chunks[0]
        chunks[0] = (name, Arrays.copyOf(data, len(data) - 3), method)
        self.write_zstd(chunks)
        self.assert_rejected("Cannot decompress native chunk %s" % chunk_name(LIBRARY, 0),
                             zstd_jar=ZSTD_JAR)

    def test_rejects_zstd_without_zstd_jni(self):
        self.write_zstd()
        self.assert_rejected("requires zstd-jni")

    def test_rejects_missing_chunk(self):
        write_jar(self.jar, [(MANIFEST, manifest(CHUNKS), ZipEntry.DEFLATED)]
                  + self.deflated_chunks()[:2])
        self.assert_rejected("Missing native chunk %s" % chunk_name(LIBRARY, 2))

    def test_rejects_missing_created_manifest(self):
        self.write_uncompressed()
        self.assert_rejected("Missing native chunk manifest",
                             created=["amd64/Linux/libother.so.chunks.properties"])

    def test_rejects_stored_build_created_chunk(self):
        write_jar(self.jar, [(MANIFEST, manifest(CHUNKS), ZipEntry.DEFLATED)]
                  + self.deflated_chunks(method=ZipEntry.STORED))
        self.assert_rejected("is not DEFLATED", created=[MANIFEST],
                             deflated=[name for name, _, _ in self.deflated_chunks()])


if __name__ == "__main__":
    if not ZSTD_JAR or not os.path.isfile(ZSTD_JAR):
        sys.exit("usage: test_verify_native_library_chunks.py <zstd-jni.jar>")
    unittest.main()
