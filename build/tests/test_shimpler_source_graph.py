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

from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

from shimpler_source_graph import load_source_graph, source_lineage


def profile(target, parent=None):
    properties = '<shim.source.inherits>%s</shim.source.inherits>' % parent if parent is not None else ''
    return '<profile><id>release%s</id><properties>%s</properties></profile>' % (target, properties)


def activated_profile(profile_id, target, parent=None, declared_buildver=None):
    parent_property = '<shim.source.inherits>%s</shim.source.inherits>' % parent if parent else ''
    declared_property = '<buildver>%s</buildver>' % declared_buildver if declared_buildver else ''
    return ('<profile><id>%s</id><activation><property><name>buildver</name><value>%s</value>'
            '</property></activation><properties>%s%s</properties></profile>' %
            (profile_id, target, declared_property, parent_property))


class SourceGraphTest(unittest.TestCase):
    def load_profiles(self, profiles):
        with tempfile.TemporaryDirectory() as directory:
            pom = Path(directory) / 'pom.xml'
            pom.write_text(
                '<project xmlns="http://maven.apache.org/POM/4.0.0"><profiles>%s</profiles></project>'
                % profiles, encoding='utf-8')
            return load_source_graph(str(pom))

    def test_linear_and_branching_lineage(self):
        graph = self.load_profiles(
            profile('401') + profile('402', '401') + profile('402emr', '402')
            + profile('403', '402'))
        self.assertEqual(source_lineage(graph, '402emr'), ['402emr', '402', '401'])
        self.assertEqual(source_lineage(graph, '403'), ['403', '402', '401'])
        self.assertEqual(source_lineage(graph, '401'), ['401'])

    def test_buildver_activated_target(self):
        graph = self.load_profiles(profile('420') + activated_profile('sparkMaster', '500', '420'))
        self.assertEqual(source_lineage(graph, '500'), ['500', '420'])

    def test_buildver_activated_target_mismatch(self):
        with self.assertRaisesRegex(ValueError, 'activates buildver 500 but declares 501'):
            self.load_profiles(activated_profile('sparkMaster', '500', declared_buildver='501'))

    def test_missing_parent(self):
        with self.assertRaisesRegex(ValueError, 'Source parent 401 of 402 is not a build target'):
            self.load_profiles(profile('402', '401'))

    def test_missing_parent_for_buildver_activated_target(self):
        with self.assertRaisesRegex(ValueError, 'Source parent 420 of 500 is not a build target'):
            self.load_profiles(activated_profile('sparkMaster', '500', '420'))

    def test_cycle(self):
        with self.assertRaisesRegex(ValueError, 'Source inheritance cycle: 401 -> 402 -> 401'):
            self.load_profiles(profile('401', '402') + profile('402', '401'))

    def test_self_inheritance(self):
        with self.assertRaisesRegex(ValueError, 'Source inheritance cycle: 402 -> 402'):
            self.load_profiles(profile('402', '402'))

    def test_duplicate_release_target(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate build target 402'):
            self.load_profiles(profile('402') + profile('402'))

    def test_empty_parent(self):
        with self.assertRaisesRegex(ValueError, 'Empty source parent for 402'):
            self.load_profiles(profile('402', ''))

    def test_unknown_lineage_target(self):
        graph = self.load_profiles(profile('401'))
        with self.assertRaisesRegex(ValueError, 'Unknown build target 402'):
            source_lineage(graph, '402')

    def test_project_poms_have_expected_scala_lineages(self):
        root_graph = load_source_graph(str(ROOT / 'pom.xml'))
        scala_213_graph = load_source_graph(str(ROOT / 'scala2.13' / 'pom.xml'))
        expected_359_lineage = ['359', '358', '357', '356', '355', '354',
                                '353', '352', '351', '350']
        self.assertEqual(source_lineage(root_graph, '359'), expected_359_lineage)
        self.assertEqual(source_lineage(scala_213_graph, '359'), expected_359_lineage)
        self.assertNotIn('402', root_graph)
        self.assertNotIn('500', root_graph)
        self.assertEqual(source_lineage(scala_213_graph, '402'), ['402', '401', '400'])
        self.assertEqual(source_lineage(scala_213_graph, '403'), ['403', '402', '401', '400'])
        self.assertEqual(source_lineage(scala_213_graph, '404'),
                         ['404', '403', '402', '401', '400'])
        self.assertEqual(source_lineage(scala_213_graph, '411'),
                         ['411', '404', '403', '402', '401', '400'])
        self.assertEqual(source_lineage(scala_213_graph, '412'),
                         ['412', '411', '404', '403', '402', '401', '400'])
        self.assertEqual(source_lineage(scala_213_graph, '413'),
                         ['413', '412', '411', '404', '403', '402', '401', '400'])
        self.assertEqual(source_lineage(scala_213_graph, '500'), ['500', '420'])


if __name__ == '__main__':
    unittest.main()
