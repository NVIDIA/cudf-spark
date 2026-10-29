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

"""Render a shim-deps BOM with system-scoped deps on the jars of a Spark installation.

Every jar found under the given directories (e.g. $SPARK_HOME/jars or
/databricks/jars) becomes a system-scoped dependency so the plugin compiles
against exactly the jars the runtime ships, without installing them into a
Maven repository.
"""

import argparse
import glob
import os
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

MAVEN_POM_NS = {'m': 'http://maven.apache.org/POM/4.0.0'}


def read_project_version(pom_file):
    """Read <version> from a pom, falling back to <parent><version>."""
    root = ET.parse(pom_file).getroot()
    version_el = root.find('m:version', MAVEN_POM_NS)
    if version_el is None or not version_el.text:
        version_el = root.find('m:parent/m:version', MAVEN_POM_NS)
    if version_el is None or not version_el.text:
        raise ValueError(f"No <version> found in {pom_file}")
    return version_el.text.strip()


def collect_jars(jar_dirs, excludes):
    """Return the distinct real paths of the jars under jar_dirs, minus excludes."""
    jars = []
    seen = set()
    for jar_dir in jar_dirs:
        found = sorted(glob.glob(os.path.join(jar_dir, '*.jar')))
        if not found:
            raise ValueError(f"No jars found under {jar_dir}")
        for jar in found:
            real_path = os.path.realpath(jar)
            if (real_path in seen or not os.path.isfile(real_path) or
                    any(pattern in os.path.basename(real_path) for pattern in excludes)):
                continue
            seen.add(real_path)
            jars.append(real_path)
    return jars


def jar_dir_properties(jars):
    """Map each distinct jar directory to a property name, in first-seen order."""
    props = {}
    for jar in jars:
        props.setdefault(os.path.dirname(jar), f"spark.home.jar.dir.{len(props)}")
    return props


def system_properties(dir_props):
    return [f"        <{prop}>{escape(jar_dir)}</{prop}>" for jar_dir, prop in dir_props.items()]


def system_dependencies(jars, dir_props):
    # systemPath references a property so Maven does not warn about hard-coded paths
    return [f"""        <dependency>
            <groupId>com.nvidia.spark_home</groupId>
            <artifactId>jar-{i}</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${{{dir_props[os.path.dirname(jar)]}}}/{escape(os.path.basename(jar))}</systemPath>
        </dependency>""" for i, jar in enumerate(jars)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pom_template', help='Path to pom.xml.template')
    parser.add_argument('--output-pom', required=True, help='Generated pom.xml path')
    parser.add_argument('--project-pom', required=True,
                        help='POM whose project version replaces <!--PROJECT_VERSION_PLACEHOLDER-->')
    parser.add_argument('--scala-binary-version', required=True,
                        help='Scala binary version that replaces @SCALA_BINARY_VERSION@')
    parser.add_argument('--buildver',
                        help='Databricks build version identified by the generated BOM')
    parser.add_argument('--jar-dir', action='append', required=True,
                        help='Directory whose *.jar files become system-scoped dependencies. '
                             'May be repeated.')
    parser.add_argument('--exclude', action='append', default=[],
                        help='Skip jars whose file name contains this substring. May be repeated.')
    args = parser.parse_args()

    with open(args.pom_template) as f:
        pom = f.read()
    jars = collect_jars(args.jar_dir, args.exclude)
    dir_props = jar_dir_properties(jars)
    deps = system_dependencies(jars, dir_props)
    pom = (pom
           .replace('@SCALA_BINARY_VERSION@', args.scala_binary_version)
           .replace('<!--DATABRICKS_BUILDVER_PROPERTY-->',
                    f'<rapids.audit.databricks.buildver>{escape(args.buildver)}</rapids.audit.databricks.buildver>'
                    if args.buildver else '')
           .replace('<!--PROJECT_VERSION_PLACEHOLDER-->', read_project_version(args.project_pom))
           .replace('<!--SYSTEM_PROPERTIES_PLACEHOLDER-->', '\n'.join(system_properties(dir_props)))
           .replace('<!--SYSTEM_DEPENDENCIES_PLACEHOLDER-->', '\n'.join(deps)))
    os.makedirs(os.path.dirname(os.path.abspath(args.output_pom)), exist_ok=True)
    with open(args.output_pom, 'w') as f:
        f.write(pom)
    print(f"Wrote {args.output_pom} with {len(deps)} system dependencies from "
          f"{', '.join(args.jar_dir)}")


if __name__ == '__main__':
    main()
