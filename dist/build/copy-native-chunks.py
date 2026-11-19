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

"""Copy JNI chunks into the distribution JAR without recompressing existing entries."""

import os
import stat
import tempfile

from java.io import File
from org.apache.commons.compress.archivers.zip import (
    ZipArchiveEntry, ZipArchiveOutputStream, ZipFile)


def entries_in_order(archive):
    entries = archive.getEntriesInPhysicalOrder()
    while entries.hasMoreElements():
        yield entries.nextElement()


def copy_raw(source, entry, output):
    stream = source.getRawInputStream(entry)
    if stream is None:
        raise RuntimeError("Cannot read raw ZIP entry %s" % entry.getName())
    try:
        output.addRawArchiveEntry(ZipArchiveEntry(entry), stream)
    finally:
        stream.close()


jar_path = attributes.get("jar_path")
archive_path = attributes.get("archive_path")
source = ZipFile(File(archive_path))
try:
    chunks = [entry for entry in entries_in_order(source)
              if ".chunks/" in entry.getName() and not entry.isDirectory()]
    if chunks:
        replacements = set(entry.getName() for entry in chunks)
        descriptor, temporary = tempfile.mkstemp(
            prefix=os.path.basename(jar_path) + ".tmp-",
            dir=os.path.dirname(os.path.abspath(jar_path)))
        os.close(descriptor)
        try:
            original = ZipFile(File(jar_path))
            try:
                output = ZipArchiveOutputStream(File(temporary))
                try:
                    for entry in entries_in_order(original):
                        if entry.getName() not in replacements:
                            copy_raw(original, entry, output)
                    for entry in chunks:
                        copy_raw(source, entry, output)
                    output.finish()
                finally:
                    output.close()
            finally:
                original.close()
            os.chmod(temporary, stat.S_IMODE(os.stat(jar_path).st_mode))
            os.rename(temporary, jar_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
finally:
    source.close()
