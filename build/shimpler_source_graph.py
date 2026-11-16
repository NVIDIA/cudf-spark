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

"""Read and validate source-parent relationships from Maven build target profiles.

This module is shared with the Jython 2.7 shim-source build task. A source parent
changes source selection only where an annotated source uses an introduction
selector; it does not change dependencies or the generated Spark identity.
"""

import xml.etree.ElementTree as ET


POM_NAMESPACE = '{http://maven.apache.org/POM/4.0.0}'


def load_source_graph(pom_path):
    """Return target -> optional source parent after validating the graph."""
    pom = ET.parse(pom_path).getroot()
    graph = {}
    profiles = pom.find(POM_NAMESPACE + 'profiles')
    if profiles is None:
        raise ValueError('No Maven profiles found in %s' % pom_path)

    for profile in profiles.findall(POM_NAMESPACE + 'profile'):
        profile_id = profile.findtext(POM_NAMESPACE + 'id')
        activation = profile.find(POM_NAMESPACE + 'activation/' + POM_NAMESPACE + 'property')
        if profile_id and profile_id.startswith('release'):
            target = profile_id[len('release'):]
            if activation is not None:
                activation_name = (activation.findtext(POM_NAMESPACE + 'name') or '').strip()
                activation_value = (activation.findtext(POM_NAMESPACE + 'value') or '').strip()
                if activation_name != 'buildver' or activation_value != target:
                    raise ValueError('Profile %s must activate buildver %s, not %s=%s' %
                                     (profile_id, target, activation_name, activation_value))
        else:
            if activation is None or activation.findtext(POM_NAMESPACE + 'name') != 'buildver':
                continue
            target = (activation.findtext(POM_NAMESPACE + 'value') or '').strip()
        if not target:
            raise ValueError('Empty build target in profile %s of %s' % (profile_id, pom_path))
        if target in graph:
            raise ValueError('Duplicate build target %s in %s' % (target, pom_path))
        properties = profile.find(POM_NAMESPACE + 'properties')
        if properties is not None:
            declared_buildver = properties.findtext(POM_NAMESPACE + 'buildver')
            if declared_buildver is not None and declared_buildver.strip() != target:
                raise ValueError('Profile %s activates buildver %s but declares %s' %
                                 (profile_id, target, declared_buildver.strip()))
        parent = None
        if properties is not None:
            parent_element = properties.find(POM_NAMESPACE + 'shim.source.inherits')
            if parent_element is not None:
                parent = (parent_element.text or '').strip()
                if not parent:
                    raise ValueError('Empty source parent for %s in %s' % (target, pom_path))
        graph[target] = parent

    if not graph:
        raise ValueError('No build targets found in %s' % pom_path)
    validate_source_graph(graph)
    return graph


def validate_source_graph(graph):
    """Reject unknown parents and cycles, including self-inheritance."""
    for target, parent in graph.items():
        if parent is not None and parent not in graph:
            raise ValueError('Source parent %s of %s is not a build target' % (parent, target))

    visited = set()
    for target in graph:
        path = []
        positions = {}
        current = target
        while current is not None and current not in visited:
            if current in positions:
                cycle = path[positions[current]:] + [current]
                raise ValueError('Source inheritance cycle: %s' % ' -> '.join(cycle))
            positions[current] = len(path)
            path.append(current)
            current = graph[current]
        visited.update(path)


def source_lineage(graph, target):
    """Return the target followed by its source ancestors, nearest first."""
    validate_source_graph(graph)
    if target not in graph:
        raise ValueError('Unknown build target %s' % target)
    lineage = []
    current = target
    while current is not None:
        lineage.append(current)
        current = graph[current]
    return lineage
