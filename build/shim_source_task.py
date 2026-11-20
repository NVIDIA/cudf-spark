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

"""Jython Ant task that materializes inherited shim sources."""

import os
import sys


def _property(name):
    value = project.getProperty(name)
    return None if value is None else str(value)


def _fail(message):
    self.fail(message)


def task_impl():
    source_root = _property('spark.rapids.source.basedir')
    sys.path.insert(0, os.path.join(source_root, 'build'))

    from shimpler_source_generator import generate_sources
    from shimpler_source_graph import load_source_graph
    from shimpler_source_targets import EXTERNAL_EXACT_TARGETS

    try:
        graph = load_source_graph(os.path.join(
            _property('spark.rapids.project.basedir'), 'pom.xml'))
        valid_targets = set()
        for pom in ('pom.xml', 'scala2.13/pom.xml'):
            valid_targets.update(load_source_graph(os.path.join(source_root, pom)))
        buildver = _property('buildver')
        if buildver not in graph:
            raise ValueError('Build target %s is not in the shim source graph' % buildver)

        module_root = (_property('shim.source.module.basedir') or
                       str(project.getBaseDir()))
        generated_root = os.path.join(str(project.getBaseDir()), 'target',
                                      'spark%s' % buildver, 'generated', 'src')
        selected = generate_sources(module_root, generated_root, graph, buildver,
                                    valid_targets, EXTERNAL_EXACT_TARGETS,
                                    output_root=str(project.getBaseDir()))
    except ValueError as error:
        _fail('Invalid shim source selection: %s' % error)
        return

    project.log('Generated %d shim sources for %s' % (len(selected), buildver))


task_impl()
