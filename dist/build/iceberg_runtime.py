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

import re
import xml.etree.ElementTree as ET


MAVEN_NS = "http://maven.apache.org/POM/4.0.0"
MAVEN_PROPERTY_RE = re.compile(r"\$\{([^}]+)\}")


def resolve_maven_properties(value, overrides, property_lookup):
    for _ in range(10):
        names = MAVEN_PROPERTY_RE.findall(value)
        if not names:
            return value
        for name in names:
            replacement = overrides.get(name) or property_lookup(name)
            if replacement is None:
                raise RuntimeError("unresolved Maven property %s in %s" % (name, value))
            value = value.replace("${%s}" % name, replacement)
    raise RuntimeError("cyclic Maven properties in %s" % value)


def coordinates(zip_handle, buildver, scala_version, property_lookup):
    """Return exact Iceberg runtimes represented by one sparkXYZ aggregator."""
    if len(buildver) < 2 or not buildver[:2].isdigit():
        raise RuntimeError("cannot derive Spark feature version from build version %s" % buildver)
    overrides = {
        "iceberg.artifact.suffix": "%s.%s" % (buildver[0], buildver[1]),
        "scala.binary.version": scala_version,
    }
    result = set()
    prefix = "META-INF/maven/com.nvidia/rapids-4-spark-iceberg-"
    namespace = {"m": MAVEN_NS}
    real_modules = []
    stub_modules = []
    for entry in zip_handle.namelist():
        if not entry.startswith(prefix) or not entry.endswith("/pom.xml"):
            continue
        root = ET.fromstring(zip_handle.read(entry))
        module_artifact_id = root.findtext("m:artifactId", namespaces=namespace)
        if module_artifact_id == "rapids-4-spark-iceberg-stub_%s" % scala_version:
            stub_modules.append(module_artifact_id)
            continue
        if not module_artifact_id or not re.match(
                r"^rapids-4-spark-iceberg-[0-9].*_%s$" % re.escape(scala_version),
                module_artifact_id):
            continue
        real_modules.append(module_artifact_id)
        runtime_dependencies = []
        for dependency in root.findall("./m:dependencies/m:dependency", namespace):
            group_id = dependency.findtext("m:groupId", namespaces=namespace)
            artifact_id = dependency.findtext("m:artifactId", namespaces=namespace)
            version = dependency.findtext("m:version", namespaces=namespace)
            if (group_id == "org.apache.iceberg" and artifact_id and version and
                    artifact_id.startswith("iceberg-spark-runtime-")):
                runtime_dependencies.append((
                    group_id,
                    resolve_maven_properties(artifact_id, overrides, property_lookup),
                    resolve_maven_properties(version, overrides, property_lookup)))
        if len(runtime_dependencies) != 1:
            raise RuntimeError("Iceberg module %s has %d runtime dependencies" %
                               (module_artifact_id, len(runtime_dependencies)))
        result.update(runtime_dependencies)
    if real_modules and stub_modules:
        raise RuntimeError("aggregator contains both real and stub Iceberg modules")
    if not real_modules and len(stub_modules) != 1:
        raise RuntimeError("aggregator must contain real Iceberg module(s) or one stub module")
    return sorted(result)
