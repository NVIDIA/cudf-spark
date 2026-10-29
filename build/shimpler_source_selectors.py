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

"""Parse Spark source selectors and check proposed inheritance against exact membership.

This module is compatible with the shim-source build task's Jython 2.7 runtime. Source changes
should compare inherited and legacy selections before converting annotations.
"""

from collections import namedtuple
import json
import re

from shimpler_source_graph import source_lineage, validate_source_graph


try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)


SourceSelector = namedtuple('SourceSelector', 'kind targets')


def spark_minor(target):
    """Return the major/minor encoded by the repository's Spark buildver."""
    match = re.match(r'^([0-9])([0-9])[0-9](?:[a-z][a-z0-9]*)?$', target)
    if match is None:
        raise ValueError('sameMinor requires a three-digit Spark build target: %s' % target)
    return match.group(1), match.group(2)


def parse_selector_line(line, graph, allowed_external_targets=()):
    """Parse one shim JSON line, allowing only listed external exact targets.

    Public POMs do not define private/vendor targets already present in exact
    annotations. A consumer may explicitly allow those legacy strings, but
    inheritance selectors must name a known graph target.
    """
    entry = json.loads(line)
    if not isinstance(entry, dict) or 'spark' not in entry:
        raise ValueError('Shim selector must contain a spark field')

    spark = entry['spark']
    carry_forward = entry.get('carryForward')
    if not isinstance(spark, list) or carry_forward not in ('none', 'sameMinor', 'all'):
        raise ValueError('Spark selector requires a target list and '
                         'carryForward value none, sameMinor, or all')
    if 'includeDescendantShims' in entry:
        raise ValueError('Use carryForward instead of includeDescendantShims')
    unknown_keys = set(entry) - {'spark', 'carryForward'}
    if unknown_keys:
        raise ValueError('Unknown shim selector fields: %s' % ', '.join(sorted(unknown_keys)))
    kind = {'none': 'exact', 'sameMinor': 'minor',
            'all': 'descendants'}[carry_forward]
    targets = tuple(spark)

    if not targets or any(not isinstance(target, string_types) or not target for target in targets):
        raise ValueError('Spark selector targets must be nonempty strings')
    if len(set(targets)) != len(targets):
        raise ValueError('Duplicate Spark selector target')
    for target in targets:
        if target not in graph and not (kind == 'exact' and
                                        target in allowed_external_targets):
            raise ValueError('Unknown Spark selector target %s' % target)

    return SourceSelector(kind, targets)


def derive_introductions(graph, exact_targets):
    """Return candidate introduction nodes for a legacy exact-membership set."""
    validate_source_graph(graph)
    exact_targets = set(exact_targets)
    for target in exact_targets:
        if target not in graph:
            raise ValueError('Unknown exact-membership target %s' % target)
    return tuple(sorted(target for target in exact_targets if graph[target] not in exact_targets))


def inherited_membership(graph, introductions):
    """Return targets reached by one or more candidate introduction nodes."""
    validate_source_graph(graph)
    introductions = set(introductions)
    for target in introductions:
        if target not in graph:
            raise ValueError('Unknown introduction target %s' % target)
    membership = set()
    for target in graph:
        current = target
        while current is not None:
            if current in introductions:
                membership.add(target)
                break
            current = graph[current]
    return membership


def selector_membership(graph, selector):
    """Expand a selector to its active build targets."""
    if selector.kind == 'exact':
        return set(selector.targets)
    descendants = inherited_membership(graph, selector.targets)
    if selector.kind == 'descendants':
        return descendants
    if selector.kind == 'minor':
        return set(target for target in descendants
                   if any(root in source_lineage(graph, target) and
                          spark_minor(root) == spark_minor(target)
                          for root in selector.targets))
    raise ValueError('Unknown Spark selector kind %s' % selector.kind)


def inheritance_mismatches(graph, exact_targets):
    """Return targets where candidate inheritance differs from exact membership."""
    exact_targets = set(exact_targets)
    introductions = derive_introductions(graph, exact_targets)
    return tuple(sorted(exact_targets.symmetric_difference(inherited_membership(graph, introductions))))
