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
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

import check_shim_compatibility as checker
from shim_compatibility_impact import analyze_impact
from test_shim_compatibility_impact import POM_PATHS, pom, source


FOO = 'sql-plugin/src/main/spark355/scala/pkg/Foo.scala'
CALLER = 'sql-plugin/src/main/spark400/scala/pkg/Caller.scala'
REPO = 'NVIDIA/cudf-spark'


class ShimCompatibilityFilterSafetyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'repo'
        self.root.mkdir()
        self.git('init', '-q')
        text = pom(graph={'355': None, '400': None})
        self.base = self.commit({
            **{path: text for path in POM_PATHS},
            'sql-plugin/src/main/scala/pkg/Base.scala': 'package pkg\nclass Base\n',
        })
        self.reference = checker.git_texts(self.root, self.base, set(POM_PATHS))
        self.snapshot_number = 0

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True,
                                       stderr=subprocess.PIPE).strip()

    def commit(self, files):
        for name, text in files.items():
            path = self.root / name
            if text is None:
                path.unlink(missing_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
        self.git('add', '--all')
        self.git('-c', 'user.name=Shim filter safety test', '-c',
                 'user.email=shim-filter-safety@example.invalid', 'commit', '-q',
                 '--signoff', '-m', 'Temporary source-selection safety fixture')
        return self.git('rev-parse', 'HEAD')

    def branch(self, files, base=None):
        self.git('checkout', '-q', '--detach', self.base if base is None else base)
        return self.commit(files)

    def coverage(self, revision):
        self.snapshot_number += 1
        destination = Path(self.temp.name) / ('coverage-%d' % self.snapshot_number)
        checker.source_snapshot(self.root, revision, destination)
        return checker.coverage(destination, ['sql-plugin'], None)

    def local_impact(self, head, paths):
        inputs = set(POM_PATHS) | set(paths)
        return analyze_impact(checker.git_texts(self.root, self.base, inputs),
                              checker.git_texts(self.root, head, inputs), self.reference)

    def test_new_declarations_create_joint_failure_despite_disjoint_local_profiles(self):
        current = self.branch({FOO: source(('355',), body='class Foo')})
        peer = self.branch({CALLER: source(('400',),
                                           body='import pkg.Foo\nclass Caller')})
        individual = [self.coverage(revision) for revision in (self.base, current, peer)]
        self.assertEqual([result['status'] for result in individual], ['PASS', 'PASS', 'PASS'])
        self.assertTrue(all(not result['findings'] for result in individual))
        joint_tree = checker.merge_snapshot(self.root, current, peer)
        joint = self.coverage(joint_tree)
        self.assertEqual(joint['status'], 'BLOCKED')
        self.assertEqual([(finding['kind'], finding['buildver'], finding['symbol'],
                           finding['caller']) for finding in joint['findings']],
                         [('missing_implementation', '400', 'pkg.Foo', CALLER)])

        # Local selector membership alone cannot prove independence: the new Foo
        # declaration changes which imports the combined-tree model recognizes.
        local_current = self.local_impact(current, {FOO})
        local_peer = self.local_impact(peer, {CALLER})
        self.assertEqual((local_current['status'], local_current['profiles']), ('KNOWN', ['355']))
        self.assertEqual((local_peer['status'], local_peer['profiles']), ('KNOWN', ['400']))
        current_impact = checker.revision_impact(self.root, self.base, current, self.reference)
        self.assertEqual(current_impact['status'], 'UNKNOWN', current_impact)
        with mock.patch.object(checker, 'fetch_peer', side_effect=lambda root, repo, head: head):
            evidence = checker.candidate_filter(self.root, REPO, self.base, current_impact,
                                                {'headRefOid': peer}, {CALLER}, self.reference)
        self.assertNotEqual(evidence['status'], 'FILTERED', evidence)

    def test_removed_renamed_or_unparseable_declarations_keep_impact_unknown(self):
        original = source(('355',), body='class Foo')
        provider_base = self.branch({FOO: original})
        changes = (
            None,
            source(('355',), body='class RenamedFoo'),
            original.replace('package pkg', 'package different'),
            source(('355',), body='val declarationNotUnderstood = 1'),
        )
        for changed in changes:
            with self.subTest(source=changed):
                head = self.branch({FOO: changed}, provider_base)
                impact = checker.revision_impact(self.root, provider_base, head, self.reference)
                self.assertEqual(impact['status'], 'UNKNOWN', impact)

    def test_existing_declaration_body_changes_remain_bounded(self):
        provider_base = self.branch({FOO: source(('355',), body='class Foo { val n = 1 }'),
                                     CALLER: source(('400',), body='class Caller { val n = 1 }')})
        current = self.branch({FOO: source(('355',), body='class Foo { val n = 2 }')}, provider_base)
        peer = self.branch({CALLER: source(('400',), body='class Caller { val n = 2 }')}, provider_base)
        impact = checker.revision_impact(self.root, provider_base, current, self.reference)
        self.assertEqual((impact['status'], impact['profiles']), ('KNOWN', ['355']))
        with mock.patch.object(checker, 'fetch_peer', side_effect=lambda root, repo, head: head):
            evidence = checker.candidate_filter(self.root, REPO, provider_base, impact,
                                                {'headRefOid': peer}, {CALLER}, self.reference)
        self.assertEqual(evidence['status'], 'FILTERED', evidence)
        self.assertEqual(evidence['peer_profiles'], ['400'])


if __name__ == '__main__':
    unittest.main()
