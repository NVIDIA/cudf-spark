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
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

from shimpler_source_selectors import (derive_introductions, inheritance_mismatches,
                                      parse_selector_line)


class SourceSelectorTest(unittest.TestCase):
    def setUp(self):
        self.graph = {'401': None, '402': '401', '402emr': '402',
                      '420': None, '500': '420'}

    def test_unknown_component_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Unknown shim selector fields: iceberg'):
            parse_selector_line('{"spark":["402"],"carryForward":"none",'
                                '"iceberg":{"introduced":"19x"}}', self.graph)

    def test_exact_and_descendant_membership(self):
        descendants = parse_selector_line(
            '{"spark":["401","420"],"carryForward":"all"}', self.graph)
        exact = parse_selector_line(
            '{"spark":["402","402emr"],"carryForward":"none"}', self.graph)
        self.assertEqual(descendants.kind, 'descendants')
        self.assertEqual(descendants.targets, ('401', '420'))
        self.assertEqual(exact.kind, 'exact')
        self.assertEqual(exact.targets, ('402', '402emr'))

    def test_rejects_unknown_or_ambiguous_selectors(self):
        for line in ('{"spark":["999"],"carryForward":"none"}',
                     '{"spark":{"introduced":"401"}}',
                     '{"spark":{"only":"402"}}',
                     '{"spark":[],"carryForward":"all"}',
                     '{"spark":["401","401"],"carryForward":"all"}',
                     '{"spark":["401","420"]}',
                     '{"spark":["401"],"includeDescendantShims":1}',
                     '{"spark":"401","carryForward":"all"}',
                     '{"spark":"401"}'):
            with self.assertRaises(ValueError):
                parse_selector_line(line, self.graph)

    def test_candidate_introductions_match_contiguous_membership(self):
        exact = {'401', '402', '402emr'}
        self.assertEqual(derive_introductions(self.graph, exact), ('401',))
        self.assertEqual(inheritance_mismatches(self.graph, exact), ())

    def test_master_removal_is_not_silently_inherited(self):
        self.assertEqual(derive_introductions(self.graph, {'420'}), ('420',))
        self.assertEqual(inheritance_mismatches(self.graph, {'420'}), ('500',))

    def test_unknown_exact_target(self):
        with self.assertRaisesRegex(ValueError, 'Unknown exact-membership target 999'):
            derive_introductions(self.graph, {'999'})

    def test_unmapped_legacy_target_can_be_preserved_during_public_scan(self):
        selector = parse_selector_line(
            '{"spark":["330db"],"carryForward":"none"}', self.graph,
            {'330db'})
        self.assertEqual(selector.targets, ('330db',))
        with self.assertRaisesRegex(ValueError, 'Unknown Spark selector target 330db'):
            parse_selector_line('{"spark":["330db"],"carryForward":"all"}',
                                self.graph, {'330db'})
        with self.assertRaisesRegex(ValueError, 'Unknown Spark selector target 4O4'):
            parse_selector_line('{"spark":["4O4"],"carryForward":"none"}',
                                self.graph, {'330db'})


if __name__ == '__main__':
    unittest.main()
