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
runtime_manifest = os.path.join(project_build_dir, 'iceberg-audit-runtimes.txt')
with open(os.sep.join([dist_dir, 'unshimmed-common-from-single-shim.txt']), 'r') as f:
    from_single_shim = f.read().splitlines()
with open(os.sep.join([dist_dir, 'unshimmed-from-each-spark3xx.txt']), 'r') as f:
    from_each = f.read().splitlines()
root_safe_modules = read_patterns(os.sep.join([dist_dir, 'root-safe-module-classes.txt']))
from_single_shim_or_each = from_single_shim + from_each
iceberg_audit_runtimes = {}

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
            if project.getProperty('should.build.conventional.jar'):
                zip_handle.extractall(path=top_dist_jar_dir)
            else:
                zip_handle.extractall(path=os.sep.join([top_dist_jar_dir, classifier]))
                # IMPORTANT unconditional extract from the highest Spark version to the top
                if bv == buildver_list[0] and art == 'sql-plugin-api':
                    zip_handle.extractall(path=top_dist_jar_dir)
                if bv == buildver_list[0] and art == 'aggregator':
                    namelist = zip_handle.namelist()
                    namelist_set = set(namelist)
                    root_safe_members = root_safe_module_class_members(classifier)
                    missing_members = sorted(root_safe_members - namelist_set)
                    if missing_members:
                        raise Exception(
                            "root-safe module classes missing from aggregator: %s" %
                            ", ".join(missing_members))
                    zip_handle.extractall(
                        path=top_dist_jar_dir,
                        members=[name for name in namelist if name in root_safe_members])
                # TODO deprecate
                namelist = zip_handle.namelist()
                glob_list = from_single_shim_or_each if bv == buildver_list[0] else from_each
                matching_members = select_matching_members(namelist, glob_list)
                zip_handle.extractall(path=top_dist_jar_dir, members=matching_members)

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
