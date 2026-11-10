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
import re
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'build'))

import check_shim_compatibility as checker


class ShimCompatibilityAdvisoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 0
        self.duration = 1
        self.attempted = []
        self.outcomes = {}
        self.peers = []
        self.merge = self.patch('merge_snapshot', return_value='current-tree')
        self.patch('git', return_value='current-commit')
        self.coverage = self.patch('tree_coverage', return_value={
            'findings': [], 'buildvers': ['355']})
        self.patch('time.monotonic', side_effect=lambda: self.now)
        self.combination = self.patch('peer_combination', side_effect=self.check_peer)
        self.graphql = self.patch('graphql', side_effect=self.live_peers)

    def patch(self, name, **kwargs):
        patcher = mock.patch('check_shim_compatibility.' + name, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def peer(self, number, state='OPEN', candidate=True, family=False):
        peer = {
            'number': number,
            'state': state,
            'joint_candidate': candidate,
            'head': '%040x' % number,
            'title': 'Peer %d' % number,
            'url': 'https://github.com/NVIDIA/cudf-spark/pull/%d' % number,
            'files': ['sql-plugin/src/main/spark355/scala/pkg/Example.scala'],
            'reasons': ['same shim family: scala/pkg/Example.scala' if family else
                        'shim implementation and/or build matrix changes'],
        }
        self.peers.append(peer)
        return peer

    def report(self, peers):
        return {
            'status': 'NEEDS_REVIEW',
            'scope': 'cross-PR source-selection advisory',
            'base': 'a' * 40,
            'head': 'b' * 40,
            'base_branch': 'main',
            'related': peers,
        }

    def check_peer(self, root, repo, base, current, coverage, peer, temporary, cache):
        self.attempted.append(peer['number'])
        self.now += self.duration
        return dict(status=self.outcomes.get(peer['number'],
                                            'NO_NEW_INTERACTION_FINDINGS'))

    def live_peers(self, root, query, **variables):
        numbers = {int(number) for number in re.findall(r'p(\d+):', query)}
        return {'p%d' % peer['number']: {
            'state': peer['state'], 'headRefOid': peer['head'], 'baseRefName': 'main'}
            for peer in self.peers if peer['number'] in numbers}

    def written_summary(self, report):
        output = self.root / 'report'
        checker.write_advisory(report, output)
        written = json.loads((output / 'report.json').read_text())
        self.assertEqual(written['joint_check_summary'], report['joint_check_summary'])
        return written['joint_check_summary'], (output / 'report.md').read_text()

    def test_all_fast_candidates_run_beyond_five_in_priority_order(self):
        peers = [self.peer(number) for number in range(2, 9)]
        peers.append(self.peer(1, family=True))
        report = self.report(peers)

        self.assertTrue(checker.check_peer_combinations(self.root, 'NVIDIA/cudf-spark', report))

        self.assertEqual(self.attempted, [1, 8, 7, 6, 5, 4, 3, 2])
        self.assertTrue(all(peer['joint_check']['status'] == 'NO_NEW_INTERACTION_FINDINGS'
                            for peer in peers))
        query = self.graphql.call_args.args[1]
        self.assertEqual({int(number) for number in re.findall(r'p(\d+):', query)},
                         set(range(1, 9)))

    def test_early_conflicts_and_incomplete_peers_do_not_stop_later_checks(self):
        peers = [self.peer(number) for number in range(1, 8)]
        self.outcomes.update({7: 'COMBINATION_CONFLICT', 6: 'INCOMPLETE'})
        report = self.report(peers)

        checker.check_peer_combinations(self.root, 'NVIDIA/cudf-spark', report)

        self.assertEqual(self.attempted, [7, 6, 5, 4, 3, 2, 1])
        self.assertEqual(peers[-1]['joint_check']['status'], 'COMBINATION_CONFLICT')
        self.assertEqual(peers[-2]['joint_check']['status'], 'INCOMPLETE')
        self.assertEqual(peers[0]['joint_check']['status'], 'NO_NEW_INTERACTION_FINDINGS')

    def test_budget_allows_started_check_to_finish_and_revalidates_only_attempted_peers(self):
        self.assertEqual(checker.PEER_CHECK_BUDGET_SECONDS, 150)
        peers = [self.peer(number) for number in range(1, 5)]
        self.duration = 80
        report = self.report(peers)

        checker.check_peer_combinations(self.root, 'NVIDIA/cudf-spark', report)

        self.assertEqual(self.attempted, [4, 3])
        self.assertGreater(self.now, checker.PEER_CHECK_BUDGET_SECONDS)
        for peer in peers[:2]:
            self.assertEqual(peer['joint_check']['status'], 'NOT_CHECKED')
            self.assertIn('time budget', peer['joint_check']['reason'].lower())
        query = self.graphql.call_args.args[1]
        self.assertEqual(set(re.findall(r'p(\d+):', query)), {'4', '3'})
        summary, _ = self.written_summary(report)
        self.assertEqual((summary['attempted'], summary['completed'], summary['not_checked']),
                         (2, 2, 2))

    def test_no_eligible_candidates_avoids_git_setup_and_network(self):
        for peers in ([], [self.peer(1, state='MERGED'), self.peer(2, candidate=False)]):
            with self.subTest(peers=len(peers)):
                report = self.report(peers)
                self.assertFalse(checker.check_peer_combinations(
                    self.root, 'NVIDIA/cudf-spark', report))
                self.merge.assert_not_called()
                self.combination.assert_not_called()
                self.graphql.assert_not_called()
        self.assertEqual(peers[0]['joint_check']['status'], 'ALREADY_IN_TARGET')
        self.assertEqual(peers[1]['joint_check']['status'], 'NOT_CHECKED')

    def test_setup_can_exhaust_budget_without_querying_unattempted_peers(self):
        peers = [self.peer(1), self.peer(2)]
        report = self.report(peers)

        def expensive_setup(*args):
            self.now += checker.PEER_CHECK_BUDGET_SECONDS
            return {'findings': [], 'buildvers': ['355']}

        self.coverage.side_effect = expensive_setup
        self.assertTrue(checker.check_peer_combinations(self.root, 'NVIDIA/cudf-spark', report))

        self.combination.assert_not_called()
        self.graphql.assert_not_called()
        self.assertTrue(all(peer['joint_check']['status'] == 'NOT_CHECKED' for peer in peers))
        summary, _ = self.written_summary(report)
        self.assertEqual((summary['attempted'], summary['not_checked']), (0, 2))

    def test_changed_peer_downgrades_completed_result_before_summary(self):
        peer = self.peer(1)
        report = self.report([peer])
        report['joint_check_summary'] = {'completed': 99}
        self.graphql.return_value = None
        self.graphql.side_effect = lambda *args, **kwargs: {
            'p1': {'state': 'OPEN', 'headRefOid': 'c' * 40, 'baseRefName': 'main'}}

        checker.check_peer_combinations(self.root, 'NVIDIA/cudf-spark', report)

        self.assertEqual(peer['joint_check']['status'], 'INCOMPLETE')
        self.assertIn('stale', peer['joint_check']['error'].lower())
        summary, _ = self.written_summary(report)
        self.assertEqual((summary['attempted'], summary['completed'], summary['incomplete']),
                         (1, 0, 1))

    def test_failed_final_identity_query_retains_incomplete_report_without_success_claim(self):
        peers = [self.peer(1), self.peer(2)]
        self.patch('commit_sha', side_effect=['a' * 40, 'b' * 40])
        self.patch('git_paths', return_value={
            'sql-plugin/src/main/spark355/scala/pkg/Example.scala'})
        self.patch('related_prs', return_value=self.report(peers))
        verify = self.patch('verify_review_refs')
        self.graphql.side_effect = checker.PreflightError('final identity query unavailable')

        report = checker.advisory(self.root, 'NVIDIA/cudf-spark', 'origin/main', 'HEAD',
                                  None, 'main', 99)

        self.assertEqual(report['status'], 'INCOMPLETE')
        self.assertIn('final identity query unavailable', report['error'])
        self.assertEqual(self.attempted, [2, 1])
        self.assertTrue(all(peer['joint_check']['status'] == 'INCOMPLETE' for peer in peers))
        self.assertTrue(all('error' in peer['joint_check'] for peer in peers))
        verify.assert_not_called()
        summary, markdown = self.written_summary(report)
        self.assertEqual((summary['attempted'], summary['completed'], summary['incomplete']),
                         (2, 0, 2))
        self.assertIn('INCOMPLETE', markdown)

    def test_moved_target_downgrades_peer_results_and_preserves_existing_failure(self):
        peers = [self.peer(1), self.peer(2), self.peer(3)]
        self.outcomes.update({2: 'COMBINATION_CONFLICT', 3: 'INCOMPLETE'})
        self.patch('commit_sha', side_effect=['a' * 40, 'b' * 40])
        self.patch('git_paths', return_value={
            'sql-plugin/src/main/spark355/scala/pkg/Example.scala'})
        self.patch('related_prs', return_value=self.report(peers))
        self.patch('verify_review_refs', side_effect=checker.PreflightError(
            'target branch moved during final verification'))

        def checked_peer(*args):
            result = self.check_peer(*args)
            if result['status'] == 'INCOMPLETE':
                result['error'] = 'original peer failure'
            return result

        self.combination.side_effect = checked_peer
        report = checker.advisory(self.root, 'NVIDIA/cudf-spark', 'origin/main', 'HEAD',
                                  None, 'main', 99)

        self.assertEqual(report['status'], 'INCOMPLETE')
        self.assertIn('target branch moved', report['error'])
        self.assertTrue(all(peer['joint_check']['status'] == 'INCOMPLETE' for peer in peers))
        self.assertEqual(peers[2]['joint_check']['error'], 'original peer failure')
        self.assertTrue(all(peer['joint_check'].get('error') for peer in peers[:2]))
        summary, _ = self.written_summary(report)
        self.assertEqual((summary['attempted'], summary['completed'], summary['conflicts'],
                          summary['incomplete']), (3, 0, 0, 3))

    def test_summary_totals_include_unstarted_ineligible_and_merged_peers_at_top(self):
        statuses = ['NEW_INTERACTION_FINDINGS', 'NO_NEW_INTERACTION_FINDINGS',
                    'COMBINATION_CONFLICT', 'INCOMPLETE', 'NOT_CHECKED', None]
        peers = [self.peer(number) for number in range(1, 7)]
        for peer, status in zip(peers, statuses):
            if status:
                peer['joint_check'] = {'status': status}
        ineligible = self.peer(7, candidate=False)
        ineligible['joint_check'] = {'status': 'NOT_CHECKED'}
        merged = self.peer(8, state='MERGED')
        merged['joint_check'] = {'status': 'ALREADY_IN_TARGET'}
        report = self.report(peers + [ineligible, merged])

        summary, markdown = self.written_summary(report)

        self.assertEqual(summary, {
            'related': 8, 'candidates': 6, 'attempted': 4, 'completed': 2,
            'conflicts': 1, 'incomplete': 1, 'not_checked': 3, 'already_in_target': 1, 'filtered': 0})
        top = markdown[:markdown.index('- [#1]')].lower()
        for label in ('related', 'candidates', 'attempted', 'completed', 'conflicts',
                      'incomplete', 'not checked', 'already in target'):
            self.assertIn(label, top)
        self.assertGreaterEqual(top.count('|'), 16)


if __name__ == '__main__':
    unittest.main()
