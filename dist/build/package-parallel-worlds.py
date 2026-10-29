# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import fnmatch
import os
import re
import shlex
import shutil
import subprocess
import zipfile
import xml.etree.ElementTree as ET

def shell_exec(shell_cmd, cwd=None):
    ret_code = subprocess.call(shell_cmd, cwd=cwd)
    if ret_code != 0:
        self.fail("failed to execute %s" % shell_cmd)


def inherited_maven_options():
    """Keep repository-affecting options from the parent Maven invocation."""
    args = shlex.split(os.environ.get("MAVEN_CMD_LINE_ARGS", ""))
    options = []
    value_options = {"-s", "--settings", "-gs", "--global-settings"}
    flag_options = {
        "-o", "--offline", "-U", "--update-snapshots",
        "-nsu", "--no-snapshot-updates", "-C", "--strict-checksums",
        "-c", "--lax-checksums",
    }
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in value_options:
            if index + 1 >= len(args):
                raise Exception("missing value for Maven option %s" % arg)
            value = args[index + 1]
            if not os.path.isabs(value):
                value = os.path.abspath(value)
            options.extend([arg, value])
            index += 2
            continue
        if arg in flag_options:
            options.append(arg)
        elif any(arg.startswith(name + "=") for name in
                 ("--settings", "--global-settings")):
            name, value = arg.split("=", 1)
            if not os.path.isabs(value):
                value = os.path.abspath(value)
            options.append("%s=%s" % (name, value))
        index += 1
    if art_url and not any(option in ("-s", "--settings") or
                           option.startswith("--settings=") for option in options):
        options.extend(["-s", jenkins_settings])
    return options


def remote_repositories_option():
    maven_project = project.getReference("maven.project")
    if maven_project is None:
        raise Exception("maven.project reference is unavailable")
    repositories = []
    seen = set()
    for repository in maven_project.getRemoteArtifactRepositories():
        coordinate = "%s::%s::%s" % (
            repository.getId(), repository.getLayout().getId(), repository.getUrl())
        if coordinate not in seen:
            seen.add(coordinate)
            repositories.append(coordinate)
    return ",".join(repositories)


def maven_get(group_id, artifact_id, version, classifier=None, dest=None):
    mvn_home = project.getProperty('maven.home')
    mvn_cmd = [os.path.join(mvn_home, 'bin', 'mvn')]
    mvn_cmd.extend(inherited_maven_options())
    mvn_cmd.extend([
        'org.apache.maven.plugins:maven-dependency-plugin:2.10:get',
        '-B',
        '-DgroupId=%s' % group_id,
        '-DartifactId=%s' % artifact_id,
        '-Dversion=%s' % version,
        '-Dpackaging=jar',
        '-Dtransitive=false',
        '-Dmaven.repo.local=%s' % maven_repository,
    ])
    repositories = remote_repositories_option()
    if repositories:
        mvn_cmd.append('-DremoteRepositories=%s' % repositories)
    if classifier:
        mvn_cmd.append('-Dclassifier=%s' % classifier)
    if dest:
        # TODO dest property is removed in 3.x, switch to the 'copy' goal.
        mvn_cmd.append('-Ddest=%s' % dest)
    shell_exec(mvn_cmd, project_build_dir)


def has_fnmatch_magic(pattern):
    return "*" in pattern or "?" in pattern or "[" in pattern


def select_matching_members(namelist, patterns):
    if os.environ.get("UNSHIM_FAST") != "1":
        matching_members = []
        for pat in patterns:
            matching_members += fnmatch.filter(namelist, pat)
        return matching_members

    names_by_entry = {}
    for name in namelist:
        names_by_entry.setdefault(name, []).append(name)

    matching_members = []
    for pat in patterns:
        if has_fnmatch_magic(pat):
            matching_members += fnmatch.filter(namelist, pat)
        else:
            matching_members += names_by_entry.get(pat, [])
    return matching_members


def read_patterns(path):
    if not os.path.isfile(path):
        return []
    with open(path, 'r') as f:
        return [
            line.strip()
            for line in f.read().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]


def artifact_file_name(art, classifier):
    art_id = '-'.join(['rapids-4-spark', art + '_' + scala_version])
    return '-'.join([art_id, project_version, classifier]) + '.jar'


def ensure_artifact(art, classifier):
    build_dir = os.sep.join([project_basedir, art, 'target', classifier])
    art_jar = artifact_file_name(art, classifier)
    art_jar_path = os.sep.join([build_dir, art_jar])
    if os.path.isfile(art_jar_path):
        shutil.copy(art_jar_path, deps_dir)
    else:
        art_id = '-'.join(['rapids-4-spark', art + '_' + scala_version])
        maven_get('com.nvidia', art_id, project_version, classifier, deps_dir)
    return os.sep.join([deps_dir, art_jar])


def ensure_external_artifact(group_id, artifact_id, version):
    artifact_path = os.path.join(*(
        [maven_repository] + group_id.split(".") + [
            artifact_id, version, "%s-%s.jar" % (artifact_id, version)]))
    if not os.path.isfile(artifact_path):
        maven_get(group_id, artifact_id, version)
    if not os.path.isfile(artifact_path):
        raise Exception("resolved artifact is missing: %s" % artifact_path)
    return artifact_path


def ensure_system_artifact(path):
    if not os.path.isabs(path):
        raise Exception("system Iceberg runtime path is not absolute: %s" % path)
    resolved_path = os.path.realpath(path)
    if not os.path.isfile(resolved_path):
        raise Exception("system Iceberg runtime is missing: %s" % resolved_path)
    return resolved_path


def root_safe_module_class_members(classifier):
    members = set()
    for module in root_safe_modules:
        module_jar_path = ensure_artifact(module, classifier)
        with zipfile.ZipFile(module_jar_path, 'r') as zip_handle:
            members.update([
                name for name in zip_handle.namelist()
                if name.endswith('.class')
            ])
    return members


artifacts = attributes.get('artifact_csv').split(',')
buildver_list = re.sub(r'\s+', '', project.getProperty('included_buildvers'),
                       flags=re.UNICODE).split(',')
buildver_list = sorted(buildver_list, reverse=True)
source_basedir = project.getProperty('spark.rapids.source.basedir')
project_basedir = project.getProperty('spark.rapids.project.basedir')
project_version = project.getProperty('project.version')
scala_version = project.getProperty('scala.binary.version')
project_build_dir = project.getProperty('project.build.directory')
deps_dir = os.sep.join([project_build_dir, 'deps'])
top_dist_jar_dir = os.sep.join([project_build_dir, 'parallel-world'])
art_url = project.getProperty('env.ART_URL')
jenkins_settings = os.sep.join([source_basedir, 'jenkins', 'settings.xml'])
maven_repository = project.getProperty('maven.local.repository')
dist_dir = os.sep.join([source_basedir, 'dist'])
iceberg_runtime = {}
execfile(os.path.join(dist_dir, 'build', 'iceberg_runtime.py'), iceberg_runtime)
build_info = {}
execfile(os.path.join(dist_dir, 'build', 'build_info.py'), build_info)
read_build_info = build_info['read_build_info']
runtime_jar_entries = {}
execfile(os.path.join(dist_dir, 'build', 'runtime_jar_entries.py'), runtime_jar_entries)
jar_entry_names = runtime_jar_entries['jar_entry_names']
system_iceberg_runtime = iceberg_runtime["system_runtime_path"](project.getProperty)
if system_iceberg_runtime and len(buildver_list) != 1:
    raise Exception("%s is supported only for single-shim builds" %
                    iceberg_runtime["SYSTEM_RUNTIME_PROPERTY"])
runtime_manifest = os.path.join(project_build_dir, 'iceberg-audit-runtimes.txt')
spark_runtime_manifest = os.path.join(project_build_dir, 'spark-audit-runtimes.txt')
with open(os.sep.join([dist_dir, 'unshimmed-common-from-single-shim.txt']), 'r') as f:
    from_single_shim = f.read().splitlines()
with open(os.sep.join([dist_dir, 'unshimmed-from-each-spark3xx.txt']), 'r') as f:
    from_each = f.read().splitlines()
root_safe_modules = read_patterns(os.sep.join([dist_dir, 'root-safe-module-classes.txt']))
from_single_shim_or_each = from_single_shim + from_each
iceberg_audit_runtimes = {}
# Classifiers are processed newest-first. Let older classifiers contribute
# conditional root-safe classes that are absent from newer classifiers, without
# overwriting a newer implementation of the same class path.
promoted_root_safe_members = set()

for bv in buildver_list:
    classifier = 'spark' + bv
    for art in artifacts:
        art_jar_path = ensure_artifact(art, classifier)

        with zipfile.ZipFile(art_jar_path, 'r') as zip_handle:
            if art == 'aggregator':
                coordinates = iceberg_runtime["coordinates"](
                    zip_handle, bv, scala_version, project.getProperty)
                iceberg_audit_runtimes[bv] = [
                    ensure_external_artifact(group_id, artifact_id, version)
                    for group_id, artifact_id, version in coordinates
                ]
                if system_iceberg_runtime:
                    iceberg_audit_runtimes[bv].append(
                        ensure_system_artifact(system_iceberg_runtime))
            if project.getProperty('should.build.conventional.jar'):
                zip_handle.extractall(path=top_dist_jar_dir)
            else:
                zip_handle.extractall(path=os.sep.join([top_dist_jar_dir, classifier]))
                # IMPORTANT unconditional extract from the highest Spark version to the top
                if bv == buildver_list[0] and art == 'sql-plugin-api':
                    zip_handle.extractall(path=top_dist_jar_dir)
                if art == 'aggregator':
                    namelist = zip_handle.namelist()
                    namelist_set = set(namelist)
                    root_safe_members = root_safe_module_class_members(classifier)
                    missing_members = sorted(root_safe_members - namelist_set)
                    if missing_members:
                        raise Exception(
                            "root-safe module classes missing from aggregator: %s" %
                            ", ".join(missing_members))
                    new_root_safe_members = (
                        root_safe_members - promoted_root_safe_members)
                    zip_handle.extractall(
                        path=top_dist_jar_dir,
                        members=[name for name in namelist
                                 if name in new_root_safe_members])
                    promoted_root_safe_members.update(root_safe_members)
                # TODO deprecate
                namelist = zip_handle.namelist()
                glob_list = from_single_shim_or_each if bv == buildver_list[0] else from_each
                matching_members = select_matching_members(namelist, glob_list)
                zip_handle.extractall(path=top_dist_jar_dir, members=matching_members)

private_build_info = 'cudf-spark-private-version-info.properties'
root_private_build_info_path = os.path.join(top_dist_jar_dir, private_build_info)
private_build_info_paths = [
    (buildver, os.path.join(top_dist_jar_dir, 'spark' + buildver, private_build_info))
    for buildver in buildver_list
]
present_private_build_info = [
    (buildver, path) for buildver, path in private_build_info_paths
    if os.path.isfile(path)
]
if present_private_build_info:
    if len(present_private_build_info) != len(private_build_info_paths):
        missing = [
            buildver for buildver, path in private_build_info_paths
            if not os.path.isfile(path)
        ]
        raise Exception(
            'cudf-spark-private build info is missing for: %s' % ', '.join(missing))

    reference_buildver, reference_path = present_private_build_info[0]
    reference_properties = read_build_info(reference_path)
    for key in ('version', 'revision'):
        if not reference_properties.get(key):
            raise Exception('cudf-spark-private build info is missing %s: %s' %
                            (key, reference_path))
    for buildver, path in present_private_build_info[1:]:
        properties = read_build_info(path)
        for key in ('version', 'revision'):
            if properties.get(key) != reference_properties[key]:
                raise Exception(
                    'cudf-spark-private %s differs between spark%s and spark%s: %s != %s'
                    % (key, reference_buildver, buildver,
                       reference_properties[key], properties.get(key)))

    shutil.copyfile(reference_path, root_private_build_info_path)
    for _, path in present_private_build_info:
        os.remove(path)
elif not os.path.isfile(root_private_build_info_path):
    raise Exception('cudf-spark-private build info is missing from the distribution')

root_private_properties = read_build_info(root_private_build_info_path)
for key in ('version', 'revision'):
    if not root_private_properties.get(key):
        raise Exception('cudf-spark-private build info is missing %s: %s' %
                        (key, root_private_build_info_path))

with open(runtime_manifest, 'w') as manifest:
    if set(iceberg_audit_runtimes) != set(buildver_list):
        raise Exception("Iceberg runtime discovery did not cover every build version")
    for buildver in sorted(iceberg_audit_runtimes):
        runtime_paths = iceberg_audit_runtimes[buildver]
        if runtime_paths:
            for runtime_path in sorted(runtime_paths):
                manifest.write("%s\t%s\n" % (buildver, runtime_path))
        else:
            manifest.write("%s\t-\n" % buildver)

def databricks_spark_runtime_paths(buildver):
    # The generated BOM is the build's source of truth for system-scoped jars.
    pom_root = source_basedir if scala_version == '2.12' else os.path.join(
        source_basedir, 'scala2.13')
    bom_path = os.path.join(pom_root, 'shim-deps', 'databricks', 'pom.xml')
    namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
    root = ET.parse(bom_path).getroot()
    properties = root.find('m:properties', namespace)
    directories = {element.tag.rsplit('}', 1)[-1]: element.text.strip()
                   for element in properties} if properties is not None else {}
    if directories.get('rapids.audit.databricks.buildver') != buildver:
        raise Exception('Databricks BOM %s does not identify spark%s' %
                        (bom_path, buildver))
    paths = set()
    found_classes = set()
    required_classes = {
        'org/apache/spark/SparkContext.class',
        'org/apache/spark/sql/catalyst/InternalRow.class',
        'org/apache/spark/sql/SparkSession.class',
    }
    for dependency in root.findall('./m:dependencies/m:dependency', namespace):
        if dependency.findtext('m:scope', namespaces=namespace) != 'system':
            continue
        system_path = dependency.findtext('m:systemPath', namespaces=namespace)
        if not system_path:
            raise Exception('Databricks BOM has a system dependency without systemPath')
        for name in re.findall(r'\$\{([^}]+)\}', system_path):
            if name not in directories:
                raise Exception('Databricks BOM has unresolved systemPath property %s' % name)
            system_path = system_path.replace('${%s}' % name, directories[name])
        path = os.path.realpath(system_path)
        if not os.path.isfile(path):
            raise Exception('Databricks BOM system jar is missing: %s' % path)
        if path in paths:
            continue
        entries = jar_entry_names(path)
        if any(entry.startswith('org/apache/spark/') for entry in entries):
            paths.add(path)
            found_classes.update(required_classes.intersection(entries))
    missing = sorted(required_classes - found_classes)
    if missing:
        raise Exception('Databricks BOM is missing Spark audit classes: %s' %
                        ', '.join(missing))
    return sorted(paths)


# Each classifier was compiled against a distinct Spark version. Resolve the
# same fixed set of Spark modules for every build, independent of local cache contents.
spark_audit_modules = [
    'spark-core', 'spark-catalyst', 'spark-sql', 'spark-hive', 'spark-avro',
    'spark-unsafe', 'spark-network-common', 'spark-network-shuffle',
]
skip_spark_buildvers = set(filter(None, re.split(
    r'[,\s]+', project.getProperty('rapids.spark.audit.skip.buildvers') or '')))
if 'databricks' in skip_spark_buildvers:
    skip_spark_buildvers.remove('databricks')
    skip_spark_buildvers.update(buildver for buildver in buildver_list if 'db' in buildver)
unknown_skips = skip_spark_buildvers - set(buildver_list)
if unknown_skips:
    raise Exception('Spark audit skip build versions are not included: %s' %
                    ', '.join(sorted(unknown_skips)))
databricks_buildvers = [buildver for buildver in buildver_list
                       if 'db' in buildver and buildver not in skip_spark_buildvers]
with open(spark_runtime_manifest, 'w') as manifest:
    for buildver in sorted(buildver_list):
        if buildver in skip_spark_buildvers:
            print('Skipping Spark package-private audit for spark%s by explicit request' %
                  buildver)
            manifest.write('%s\t-\n' % buildver)
            continue
        if 'db' in buildver:
            if len(databricks_buildvers) != 1:
                raise Exception('Databricks Spark audit requires one included Databricks '
                                'build version or explicit skip for the others')
            runtime_paths = databricks_spark_runtime_paths(buildver)
        else:
            spark_version = project.getProperty('spark%s.version' % buildver)
            if not spark_version:
                raise Exception('Spark version is missing for build version %s' % buildver)
            modules = list(spark_audit_modules)
            if tuple(int(part) for part in spark_version.split('.')[:2]) >= (3, 4):
                modules.append('spark-protobuf')
            if tuple(int(part) for part in spark_version.split('.')[:2]) >= (3, 5):
                modules.extend(['spark-sql-api', 'spark-common-utils'])
            runtime_paths = [ensure_external_artifact(
                'org.apache.spark', '%s_%s' % (module, scala_version), spark_version)
                for module in modules]
        for path in runtime_paths:
            manifest.write('%s\t%s\n' % (buildver, path))
