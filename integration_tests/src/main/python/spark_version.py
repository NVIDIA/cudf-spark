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

"""Utilities for parsing and comparing Apache Spark versions."""


def spark_version_tuple(ver=None):
    """Return a numeric tuple of (major, minor, patch) for the given or current Spark version.

    Strips pre-release qualifiers (e.g. -SNAPSHOT, -preview2, -amzn-0) and build metadata (+vendor).
    If ver is omitted or None, the active runtime version is queried from spark_init_internal.
    """
    if isinstance(ver, tuple):
        return ver[:3]
    if ver is None:
        from spark_init_internal import spark_version
        v = spark_version()
    else:
        v = ver
    raw = str(v).split("-", 1)[0].split("+", 1)[0]
    parts = []
    for part in raw.split("."):
        try:
            parts.append(int(part))
        except ValueError:
            break
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])
