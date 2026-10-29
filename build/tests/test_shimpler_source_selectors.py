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

    def test_legacy_exact_and_component_preservation(self):
        selector = parse_selector_line('{"spark":"402", "iceberg":{"introduced":"19x"}}',
                                       self.graph)
        self.assertEqual(selector.kind, 'exact')
        self.assertEqual(selector.targets, ('402',))
        self.assertEqual(selector.other_components, {'iceberg': {'introduced': '19x'}})

    def test_introduced_and_only(self):
        introduced = parse_selector_line('{"spark":{"introduced":["401","420"]}}', self.graph)
        only = parse_selector_line('{"spark":{"only":"402"}}', self.graph)
        self.assertEqual(introduced.kind, 'introduced')
        self.assertEqual(introduced.targets, ('401', '420'))
        self.assertEqual(only.kind, 'only')
        self.assertEqual(only.targets, ('402',))

    def test_rejects_unknown_or_ambiguous_selectors(self):
        for line in ('{"spark":"999"}', '{"spark":{"introduced":"999"}}',
                     '{"spark":{"introduced":"401","only":"402"}}',
                     '{"spark":{"introduced":[]}}',
                     '{"spark":{"introduced":["401","401"]}}'):
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
        selector = parse_selector_line('{"spark":"350db143"}', self.graph,
                                       allow_unmapped_exact=True)
        self.assertEqual(selector.targets, ('350db143',))
        with self.assertRaisesRegex(ValueError, 'Unknown Spark selector target 350db143'):
            parse_selector_line('{"spark":{"introduced":"350db143"}}', self.graph,
                                allow_unmapped_exact=True)


if __name__ == '__main__':
    unittest.main()
