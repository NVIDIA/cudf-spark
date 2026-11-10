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

import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

from shim_compatibility_impact import analyze_impact


POM_PATHS = ('pom.xml', 'scala2.13/pom.xml')
DEFAULT_GRAPH = {'355': None, '356': '355', '357': '356', '400': None}
SOURCE = 'sql-plugin/src/main/spark355/scala/pkg/Example.scala'


def dependency(version):
    return ('<dependencies><dependency><groupId>example</groupId>'
            '<artifactId>shim</artifactId><version>%s</version>'
            '</dependency></dependencies>' % version)


def plugin(version):
    return ('<build><plugins><plugin><groupId>example</groupId>'
            '<artifactId>source-generator</artifactId><version>%s</version>'
            '</plugin></plugins></build>' % version)


def pom(graph=None, description='original', reporting='original',
        global_dependency='original', global_property='original', global_plugin='original',
        profile_changes=None):
    graph = DEFAULT_GRAPH if graph is None else graph
    profile_changes = profile_changes or {}
    profiles = []
    for target, parent in graph.items():
        changes = profile_changes.get(target, {})
        properties = '<buildver>%s</buildver>' % target
        if parent:
            properties += '<shim.source.inherits>%s</shim.source.inherits>' % parent
        properties += '<profile.value>%s</profile.value>' % changes.get('property', 'original')
        profiles.append(
            '<profile><id>release%s</id><activation><property><name>buildver</name>'
            '<value>%s</value></property></activation><properties>%s</properties>%s%s'
            '</profile>' % (target, target, properties,
                             dependency(changes.get('dependency', 'original')),
                             plugin(changes.get('plugin', 'original'))))
    versions = ''.join('<spark%s.version>%s.%s.%s</spark%s.version>' %
                       (target, target[0], target[1], target[2], target)
                       for target in graph)
    return ('<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<modelVersion>4.0.0</modelVersion><groupId>example</groupId>'
            '<artifactId>shim-test</artifactId><version>1</version>'
            '<description>%s</description><properties>%s<global.value>%s</global.value>'
            '</properties>%s%s<reporting><outputDirectory>%s</outputDirectory></reporting>'
            '<profiles>%s</profiles></project>' %
            (description, versions, global_property, dependency(global_dependency),
             plugin(global_plugin), reporting, ''.join(profiles)))


def source(targets=('355',), mode='none', body='class Example', extra_selectors=()):
    entries = [json.dumps({'spark': list(targets), 'carryForward': mode})]
    entries.extend(json.dumps(selector) for selector in extra_selectors)
    return ('/*** spark-rapids-shim-json-lines\n%s\n'
            'spark-rapids-shim-json-lines ***/\npackage pkg\n%s\n' %
            ('\n'.join(entries), body))


class ShimCompatibilityImpactTest(unittest.TestCase):
    def setUp(self):
        self.reference = {path: pom() for path in POM_PATHS}

    def snapshot(self, text=None, files=None):
        snapshot = {path: pom() if text is None else text for path in POM_PATHS}
        snapshot.update(files or {})
        return snapshot

    def known(self, before, after, profiles, reference=None):
        result = analyze_impact(before, after, self.reference if reference is None else reference)
        self.assertEqual(result['status'], 'KNOWN', result)
        self.assertEqual(result['profiles'], sorted(profiles), result)
        self.assertIsInstance(result['reason'], str)
        self.assertTrue(result['reason'])
        return result

    def unknown(self, before, after, reference=None):
        result = analyze_impact(before, after, self.reference if reference is None else reference)
        self.assertEqual(result['status'], 'UNKNOWN', result)
        self.assertIsInstance(result['reason'], str)
        self.assertTrue(result['reason'])
        return result

    def test_xml_format_and_comments_have_no_profile_effect(self):
        before = self.snapshot()
        formatted = pom().replace('><', '>\n  <').replace(
            '<profiles>', '<!-- source-layout note -->\n<profiles>')
        self.known(before, self.snapshot(formatted), [])

    def test_root_description_and_reporting_have_no_profile_effect(self):
        for changes in ({'description': 'updated'}, {'reporting': 'updated'}):
            with self.subTest(changes=changes):
                self.known(self.snapshot(), self.snapshot(pom(**changes)), [])

    def test_ignored_metadata_referenced_by_build_semantics_is_unknown(self):
        before = self.snapshot(pom(global_dependency='${project.description}'))
        after = self.snapshot(pom(description='updated',
                                  global_dependency='${project.description}'))
        self.unknown(before, after)

    def test_global_dependency_property_and_plugin_changes_are_unknown(self):
        for field in ('global_dependency', 'global_property', 'global_plugin'):
            with self.subTest(field=field):
                self.unknown(self.snapshot(), self.snapshot(pom(**{field: 'updated'})))

    def test_existing_profile_changes_cover_target_and_descendants(self):
        affected = {'355': ['355', '356', '357'], '356': ['356', '357'],
                    '357': ['357'], '400': ['400']}
        for target, profiles in affected.items():
            for field in ('dependency', 'property', 'plugin'):
                with self.subTest(target=target, field=field):
                    changed = pom(profile_changes={target: {field: 'updated'}})
                    self.known(self.snapshot(), self.snapshot(changed), profiles)

    def test_existing_profile_activation_changes_are_unknown_even_with_stable_graph(self):
        original = ('<activation><property><name>buildver</name><value>355</value>'
                    '</property></activation>')
        activations = (
            original.replace('</activation>', '<jdk>[17,)</jdk></activation>'),
            '<activation><jdk>[17,)</jdk></activation>',
            '<activation><activeByDefault>true</activeByDefault></activation>',
        )
        for activation in activations:
            with self.subTest(activation=activation):
                changed = pom().replace(original, activation, 1)
                self.assertNotEqual(changed, pom())
                self.unknown(self.snapshot(), self.snapshot(changed))

    def test_scala_generation_directive_comment_changes_are_unknown(self):
        for directive in ('#if scala-2.12', '#elif scala-2.13', '#else',
                          '#endif scala-2.12'):
            changed = pom().replace('<profiles>', '<profiles><!-- %s -->' % directive)
            for before, after in ((pom(), changed), (changed, pom())):
                with self.subTest(directive=directive, added=after == changed):
                    self.unknown(self.snapshot(before), self.snapshot(after))

    def test_distinct_stable_profile_changes_have_disjoint_impact(self):
        results = []
        for target in ('355', '400'):
            changed = pom(profile_changes={target: {'dependency': 'updated'}})
            results.append(analyze_impact(self.snapshot(), self.snapshot(changed), self.reference))
        self.assertTrue(all(result['status'] == 'KNOWN' for result in results), results)
        self.assertEqual(results[0]['profiles'], ['355', '356', '357'])
        self.assertEqual(results[1]['profiles'], ['400'])
        self.assertTrue(set(results[0]['profiles']).isdisjoint(results[1]['profiles']))

    def test_graph_disagreement_with_reference_is_unknown(self):
        different = dict(DEFAULT_GRAPH, **{'400': '357'})
        reference = self.snapshot(pom(graph=different))
        before = self.snapshot(files={SOURCE: source()})
        after = self.snapshot(files={SOURCE: source(body='class Updated')})
        self.unknown(before, after, reference)

    def test_proven_metadata_no_effect_does_not_require_matching_reference_graph(self):
        different = dict(DEFAULT_GRAPH, **{'400': '357'})
        reference = self.snapshot(pom(graph=different))
        self.known(self.snapshot(), self.snapshot(pom(description='updated')), [], reference)

    def test_added_deleted_and_reparented_profiles_are_unknown(self):
        added = dict(DEFAULT_GRAPH, **{'358': '357'})
        deleted = {target: parent for target, parent in DEFAULT_GRAPH.items() if target != '357'}
        reparented = dict(DEFAULT_GRAPH, **{'357': '355'})
        for graph in (added, deleted, reparented):
            with self.subTest(graph=graph):
                self.unknown(self.snapshot(), self.snapshot(pom(graph=graph)))

    def test_source_selector_modes_honor_descendants_and_minor_boundaries(self):
        cross_minor = dict(DEFAULT_GRAPH, **{'400': '357'})
        text = pom(graph=cross_minor)
        reference = self.snapshot(text)
        expected = {'none': ['355'], 'sameMinor': ['355', '356', '357'],
                    'all': ['355', '356', '357', '400']}
        for mode, profiles in expected.items():
            with self.subTest(mode=mode):
                before = self.snapshot(text, {SOURCE: source(mode=mode)})
                after = self.snapshot(text, {SOURCE: source(mode=mode, body='class Updated')})
                self.known(before, after, profiles, reference)

    def test_source_selector_before_after_add_delete_and_move_use_union(self):
        other = 'sql-plugin/src/main/spark400/scala/pkg/Example.scala'
        middle = SOURCE.replace('/spark355/', '/spark356/')
        cases = (
            ({SOURCE: source()}, {SOURCE: source(('355', '400'))}, ['355', '400']),
            ({middle: source(('356',), 'all')}, {middle: None}, ['356', '357']),
            ({other: None}, {other: source(('400',))}, ['400']),
            ({SOURCE: source(), other: None},
             {SOURCE: None, other: source(('400',))}, ['355', '400']),
            ({SOURCE: source()}, {SOURCE: source(mode='all')}, ['355', '356', '357']),
        )
        for before_files, after_files, profiles in cases:
            with self.subTest(before=before_files, after=after_files):
                self.known(self.snapshot(files=before_files), self.snapshot(files=after_files),
                           profiles)

    def test_multiple_selector_lines_combine_profile_impact(self):
        selectors = ({'spark': ['400'], 'carryForward': 'none'},)
        before = self.snapshot(files={SOURCE: source(extra_selectors=selectors)})
        after = self.snapshot(files={SOURCE: source(extra_selectors=selectors,
                                                  body='class Updated')})
        self.known(before, after, ['355', '400'])

    def test_versioned_java_source_uses_same_selector_contract(self):
        path = 'sql-plugin/src/main/spark355/java/pkg/Example.java'
        before = self.snapshot(files={path: source(('355',), 'sameMinor',
                                                   'public class Example {}')})
        after = self.snapshot(files={path: source(('355',), 'sameMinor',
                                                  'public class Example { int updated; }')})
        self.known(before, after, ['355', '356', '357'])

    def test_malformed_xml_or_unrecognized_source_selectors_are_unknown(self):
        broken_pom = self.snapshot()
        broken_pom['pom.xml'] = '<project><profiles>'
        self.unknown(self.snapshot(), broken_pom)
        invalid_sources = (
            source(('999',)), source(mode='unknown'), source(('400',)),
            'package pkg\nclass Example\n',
            '/*** spark-rapids-shim-json-lines\n{invalid json}\n'
            'spark-rapids-shim-json-lines ***/\nclass Example',
        )
        for invalid in invalid_sources:
            with self.subTest(source=invalid):
                self.unknown(self.snapshot(files={SOURCE: source()}),
                             self.snapshot(files={SOURCE: invalid}))

    def test_shared_jvm_build_tools_and_module_pom_changes_are_unknown(self):
        paths = (
            'sql-plugin/src/main/scala/pkg/Caller.scala',
            'sql-plugin/src/main/java/pkg/Caller.java',
            'build/shimpler_source_graph.py', 'build/get_buildvers.py',
            'sql-plugin/pom.xml',
        )
        for path in paths:
            with self.subTest(path=path):
                self.unknown(self.snapshot(files={path: 'original'}),
                             self.snapshot(files={path: 'updated'}))

    def test_both_scala_graphs_must_agree_for_common_target_lineages(self):
        mismatched = self.snapshot()
        mismatched['scala2.13/pom.xml'] = pom(graph=dict(DEFAULT_GRAPH, **{'357': '355'}))
        before = dict(mismatched, **{SOURCE: source()})
        after = dict(mismatched, **{SOURCE: source(body='class Updated')})
        self.unknown(before, after, mismatched)

    def test_ordinary_documentation_and_test_changes_have_no_profile_effect(self):
        for path in ('docs/shims.md', 'build/tests/test_example.py',
                     'sql-plugin/src/test/spark355/scala/pkg/ExampleSuite.scala'):
            with self.subTest(path=path):
                self.known(self.snapshot(files={path: 'original'}),
                           self.snapshot(files={path: 'updated'}), [])

    def test_missing_complete_pom_snapshot_is_unknown(self):
        for missing in POM_PATHS:
            with self.subTest(path=missing):
                after = self.snapshot(files={SOURCE: source()})
                del after[missing]
                self.unknown(self.snapshot(files={SOURCE: source(body='class Original')}), after)


if __name__ == '__main__':
    unittest.main()
