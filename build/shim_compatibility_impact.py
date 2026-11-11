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

"""Conservative, offline impact bounds for immutable PR source snapshots.

KNOWN profiles overapproximate affected source selection. UNKNOWN must never
exclude a peer: shared callers, global configuration and graph drift stay open.
"""

import io
import re
import xml.etree.ElementTree as ET

from shimpler_source_graph import load_source_graph, source_lineage
from shimpler_source_resolver import SHIM_COMMENT, VERSIONED_PATH
from shimpler_source_selectors import parse_selector_line, selector_membership


POMS = ('pom.xml', 'scala2.13/pom.xml')
NS = '{http://maven.apache.org/POM/4.0.0}'
METADATA = frozenset(('name', 'description', 'url', 'inceptionYear', 'organization',
                      'scm', 'licenses', 'developers', 'contributors', 'mailingLists',
                      'issueManagement', 'ciManagement', 'reporting'))
VERSIONED = re.compile(r'^.+/src/main/spark([^/]+)/(scala|java)/(.+\.(?:scala|java))$')


def _text(value):
    return value if value and value.strip() else ''


def _canonical(node, ignored=()):
    if node is None:
        return None
    return (node.tag, tuple(sorted(node.attrib.items())), _text(node.text),
            tuple(_canonical(child) for child in node
                  if child.tag not in {NS + name for name in ignored}), _text(node.tail))


def _pom(source):
    if not isinstance(source, str):
        raise ValueError('Missing complete root POM snapshot')
    root = ET.fromstring(source)
    if root.tag != NS + 'project':
        raise ValueError('Unexpected root POM namespace or element')
    tags = [child.tag for child in root]
    if len(tags) != len(set(tags)):
        raise ValueError('Duplicate root POM elements')
    return root, load_source_graph(io.StringIO(source))


def _generation_comments(source):
    """Retain conditional directives and disabled XML hidden inside their blocks."""
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    root = ET.fromstring(source, parser=parser)
    signature, depth = [], [0]

    def visit(node, path):
        index = 0
        for child in node:
            if child.tag is ET.Comment:
                directive = re.match(r'\s*#(if|elif|else|endif)\b', child.text or '')
                if directive or depth[0]:
                    signature.append((path + (index,), child.text))
                    index += 1
                if directive:
                    depth[0] = max(0, depth[0] + (1 if directive[1] == 'if' else
                                                 -1 if directive[1] == 'endif' else 0))
            else:
                visit(child, path + (child.tag, index))
                index += 1

    visit(root, ())
    return signature


def _exact_activation(profile, target):
    expected = ET.Element(NS + 'activation')
    prop = ET.SubElement(expected, NS + 'property')
    ET.SubElement(prop, NS + 'name').text = 'buildver'
    ET.SubElement(prop, NS + 'value').text = target
    return _canonical(profile.find(NS + 'activation')) == _canonical(expected)


def _profile_target(profile):
    identity = profile.findtext(NS + 'id') or ''
    if identity.startswith('release'):
        return identity[len('release'):]
    activation = profile.find(NS + 'activation/' + NS + 'property')
    if activation is not None and activation.findtext(NS + 'name') == 'buildver':
        return (activation.findtext(NS + 'value') or '').strip()
    return None


def _pom_impact(before, after, graphs):
    if _canonical(before) == _canonical(after):
        return set()
    if _canonical(before, {'profiles'}) != _canonical(after, {'profiles'}):
        raise ValueError('Global POM configuration changed')
    old = before.find(NS + 'profiles')
    new = after.find(NS + 'profiles')
    if old is None or new is None or old.attrib != new.attrib:
        raise ValueError('Profile container changed')
    if any(child.tag != NS + 'profile' for root in (old, new) for child in root):
        raise ValueError('Unexpected profile container element')
    old_ids = [profile.findtext(NS + 'id') for profile in old]
    new_ids = [profile.findtext(NS + 'id') for profile in new]
    if old_ids != new_ids or None in old_ids or len(old_ids) != len(set(old_ids)):
        raise ValueError('Profiles were added, removed, reordered or have ambiguous identities')
    affected = set()
    for left, right in zip(old, new):
        if _canonical(left) == _canonical(right):
            continue
        target = _profile_target(left)
        if not target or target != _profile_target(right):
            raise ValueError('Changed profile is not one stable buildver profile')
        if not all(_exact_activation(profile, target) for profile in (left, right)):
            raise ValueError('Changed profile activation is not confined to its buildver')
        for graph in graphs:
            if target in graph:
                affected.update(version for version in graph
                                if target in source_lineage(graph, version))
    return affected


def _enumeration_values(root):
    # get_buildvers searches these properties globally, including inside profiles.
    return [_canonical(node) for node in root.iter()
            if node.tag == NS + 'dyn.shim.excluded.releases' or
            re.fullmatch(re.escape(NS) + r'spark.*\.version', node.tag)]


def _source_impact(path, source, graphs):
    if source is None:
        return set()
    match = VERSIONED.fullmatch(path)
    blocks = SHIM_COMMENT.findall(source)
    if not match or len(blocks) != 1 or source.count('spark-rapids-shim-json-lines') != 2:
        raise ValueError('Versioned source has an unknown directory or selector block: ' + path)
    targets = set().union(*graphs)
    selectors = [parse_selector_line(line.strip(), targets)
                 for line in blocks[0].splitlines() if line.strip()]
    if not selectors:
        raise ValueError('Empty source selector block: ' + path)
    anchors = [min(selector.targets) for selector in selectors]
    if anchors != sorted(set(anchors)) or match[1] != min(anchors):
        raise ValueError('Ambiguous selector ordering or source owner: ' + path)
    if any(VERSIONED_PATH.match(part) for part in match[3].split('/')) and any(
            selector.kind != 'exact' for selector in selectors):
        raise ValueError('Embedded version identity must use exact selectors: ' + path)
    affected = set()
    for graph in graphs:
        for selector in selectors:
            applicable = tuple(target for target in selector.targets if target in graph)
            if applicable:
                affected.update(selector_membership(graph, selector._replace(targets=applicable)))
    return affected


def _irrelevant(path):
    # Selector annotations live in comments; never strip comments from JVM sources.
    if path.endswith('/pom.xml') or path.startswith('.agents/skills/check-shim-compatibility/'):
        return False
    return (path.startswith(('docs/', 'build/tests/')) or '/src/test/' in path
            or path.endswith(('.md', '.rst'))
            or path.startswith('integration_tests/src/main/python/'))


def analyze_impact(before, after):
    """Bound changes between current-target and proposed combined snapshots.

    Both maps include complete root POMs plus every changed path (None for a
    missing side). Metadata/reporting changes and unmodelled input stay UNKNOWN.
    No imported peer code, Git commands, file writes or network access is used.
    """
    try:
        changed = {path for path in set(before) | set(after)
                   if before.get(path) != after.get(path)}
        roots, graphs = {}, {}
        for path in POMS:
            if _generation_comments(before.get(path)) != _generation_comments(after.get(path)):
                raise ValueError('Scala generation directive or conditional comment changed: ' + path)
            old_root, old_graph = _pom(before.get(path))
            new_root, new_graph = _pom(after.get(path))
            if any(_canonical(old_root.find(NS + name)) != _canonical(new_root.find(NS + name))
                   for name in METADATA):
                raise ValueError('Root POM metadata or reporting changed: ' + path)
            if _enumeration_values(old_root) != _enumeration_values(new_root):
                raise ValueError('Global build-version enumeration inputs changed: ' + path)
            if old_graph != new_graph:
                raise ValueError('Source inheritance graph changed: ' + path)
            roots[path] = old_root, new_root
            graphs[path] = old_graph
        graph_list = list(graphs.values())
        for target in set.intersection(*(set(graph) for graph in graph_list)):
            if any(source_lineage(graph, target) != source_lineage(graph_list[0], target)
                   for graph in graph_list[1:]):
                raise ValueError('Ambiguous Scala source lineage: ' + target)
        no_effect = all(_irrelevant(path) or (path in POMS and
                        _canonical(roots[path][0]) == _canonical(roots[path][1])) for path in changed)
        if no_effect:
            return dict(status='KNOWN', profiles=[],
                        reason='Only POM formatting or ordinary documentation/tests changed')
        affected = set()
        for path in sorted(changed):
            if path in POMS:
                affected.update(_pom_impact(*roots[path], graph_list))
            elif path.endswith('/pom.xml'):
                raise ValueError('Module POM changes have unbounded profile impact: ' + path)
            elif _irrelevant(path):
                continue
            elif VERSIONED.fullmatch(path):
                affected.update(_source_impact(path, before.get(path), graph_list))
                affected.update(_source_impact(path, after.get(path), graph_list))
            else:
                raise ValueError('Shared caller, global tooling or unmodelled changed path: ' + path)
        return dict(status='KNOWN', profiles=sorted(affected),
                    reason=('Affected existing source profiles are bounded conservatively' if affected
                            else 'Only POM formatting or ordinary documentation/tests changed'))
    except (ValueError, TypeError, KeyError, AttributeError, ET.ParseError) as error:
        return dict(status='UNKNOWN', profiles=[], reason=str(error))
