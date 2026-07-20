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
import shutil
import subprocess
import zipfile


def shell_exec(shell_cmd):
    ret_code = subprocess.call(shell_cmd)
    if ret_code != 0:
        self.fail("failed to execute %s" % shell_cmd)


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
repo_local = project.getProperty('maven.repo.local')

for bv in buildver_list:
    classifier = 'spark' + bv
    for art in artifacts:
        build_dir = os.sep.join([project_basedir, art, 'target', classifier])
        art_id = '-'.join(['rapids-4-spark', art + '_' + scala_version])
        art_jar = '-'.join([art_id, project_version, classifier]) + '.jar'
        art_jar_path = os.sep.join([build_dir, art_jar])
        if os.path.isfile(art_jar_path):
            shutil.copy(art_jar_path, deps_dir)
        else:
            mvn_home = project.getProperty('maven.home')
            mvn_cmd = [
                os.sep.join([mvn_home, 'bin', 'mvn']),
                # TODO dest property is removed in 3.x, switch to the 'copy' goal
                # however it does not support overriding local repo via property
                # need an issue to sort this out better.
                'org.apache.maven.plugins:maven-dependency-plugin:2.10:get',
                '-B',
                '='.join(['-Ddest', deps_dir]),
                '='.join(['-DgroupId','com.nvidia']),
                '='.join(['-DartifactId', art_id]),
                '='.join(['-Dversion', project_version]),
                '='.join(['-Dpackaging', 'jar']),
                '='.join(['-Dclassifier', classifier]),
                '='.join(['-Dtransitive', 'false'])
            ]
            if art_url:
                mvn_cmd.extend(['-s', jenkins_settings])
            if repo_local:
                mvn_cmd.append('='.join(['-Dmaven.repo.local', repo_local]))
            shell_exec(mvn_cmd)

        dist_dir = os.sep.join([source_basedir, 'dist'])
        with open(os.sep.join([dist_dir, 'unshimmed-common-from-single-shim.txt']), 'r') as f:
            from_single_shim = f.read().splitlines()
        with open(os.sep.join([dist_dir, 'unshimmed-from-each-spark3xx.txt']), 'r') as f:
            from_each = f.read().splitlines()
        with zipfile.ZipFile(os.sep.join([deps_dir, art_jar]), 'r') as zip_handle:
            if project.getProperty('should.build.conventional.jar'):
                zip_handle.extractall(path=top_dist_jar_dir)
            else:
                zip_handle.extractall(path=os.sep.join([top_dist_jar_dir, classifier]))
                # IMPORTANT unconditional extract from the highest Spark version to the top
                if bv == buildver_list[0] and art == 'sql-plugin-api':
                    zip_handle.extractall(path=top_dist_jar_dir)
                # TODO deprecate
                namelist = zip_handle.namelist()
                matching_members = []
                glob_list = from_single_shim + from_each if bv == buildver_list[0] else from_each
                for pat in glob_list:
                    new_matches = fnmatch.filter(namelist, pat)
                    matching_members += new_matches
                zip_handle.extractall(path=top_dist_jar_dir, members=matching_members)
