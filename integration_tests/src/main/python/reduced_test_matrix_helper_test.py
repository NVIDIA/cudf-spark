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

import pytest

from reduced_test_matrix_helper import generate_reduced_test_matrix


def _matrix_values(test_matrix):
    """Return plain value tuples from a generated pytest parameter matrix."""
    return [tuple(test_case.values) for test_case in test_matrix]


def test_generate_reduced_test_matrix():
    """Cover valid matrix generation, metadata transfer, and optional inputs."""
    test_matrix = generate_reduced_test_matrix({
        'a': {
            'values': ['a1', 'a2'],
            'is_primary_dimension': True},
        'b': {
            'values': ['b1', 'b2', 'b3', 'b4'],
            'is_primary_dimension': True},
        'c': {'values': ['c1', 'c2']},
        'd': {'values': ['d1', 'd2']}})

    assert _matrix_values(test_matrix) == [
        ('a1', 'b1', 'c1', 'd1'),
        ('a1', 'b2', 'c1', 'd2'),
        ('a1', 'b3', 'c2', 'd1'),
        ('a1', 'b4', 'c2', 'd2'),
        ('a2', 'b1', 'c1', 'd1'),
        ('a2', 'b2', 'c1', 'd2'),
        ('a2', 'b3', 'c2', 'd1'),
        ('a2', 'b4', 'c2', 'd2')]

    no_secondary_matrix = generate_reduced_test_matrix({
        'a': {'values': ['a1', 'a2'], 'is_primary_dimension': True},
        'b': {'values': ['b1', 'b2'], 'is_primary_dimension': True}})
    assert _matrix_values(no_secondary_matrix) == [
        ('a1', 'b1'), ('a1', 'b2'), ('a2', 'b1'), ('a2', 'b2')]

    def named_value():
        pass

    class PlainValue:
        marks = 'not a pytest parameter set'

        def __str__(self):
            return 'plain-value'

    plain_value = PlainValue()
    metadata_matrix = generate_reduced_test_matrix({
        'secondary': {
            'values': [
                pytest.param('s1', marks=[pytest.mark.order(2)], id=''),
                's2']},
        'primary': {
            'values': [
                pytest.param(
                    'p1', marks=pytest.mark.ignore_order(local=True), id='p1-id'),
                named_value,
                plain_value],
            'is_primary_dimension': True}},
        extra_cases=[pytest.param(
            {'secondary': 's-extra', 'primary': 'p-extra'},
            marks=[pytest.mark.order(2), pytest.mark.ignore_order(local=True)],
            id='extra-case-id')])

    assert _matrix_values(metadata_matrix) == [
        ('s1', 'p1'), ('s2', named_value), ('s1', plain_value), ('s-extra', 'p-extra')]
    assert [test_case.id for test_case in metadata_matrix] == [
        '-p1-id', 's2-named_value', '-plain-value', 'extra-case-id']
    assert [[mark.name for mark in test_case.marks] for test_case in metadata_matrix] == [
        ['order', 'ignore_order'], [], ['order'], ['order', 'ignore_order']]

    plain_extra_matrix = generate_reduced_test_matrix(
        {'primary': {'values': ['p1'], 'is_primary_dimension': True}},
        extra_cases=[{'primary': 'p-extra'}])
    assert _matrix_values(plain_extra_matrix) == [('p1',), ('p-extra',)]


def test_generate_reduced_test_matrix_rejects_one_invalid_condition():
    """Check each rejected input domain with only one invalid condition at a time."""
    parameter_set_type = type(pytest.param(None))
    invalid_cases = [
        ([], None, 'dimensions must be a dict'),
        ({}, None, 'at least one dimension is required'),
        ({1: {'values': ['a1'], 'is_primary_dimension': True}}, None,
         'dimension name must be a string'),
        ({'a': []}, None, 'a dimension config must be a dict'),
        ({'a': {'is_primary_dimension': True}}, None,
         'a dimension config must contain values'),
        ({'a': {'values': ['a1'], 'unknown': True}}, None,
         "a dimension config contains unsupported keys: ['unknown']"),
        ({'a': {'values': []}}, None, 'a values must not be empty'),
        ({'a': {'values': None}}, None, 'a values must be a list'),
        ({'a': {'values': ('a1',), 'is_primary_dimension': True}}, None,
         'a values must be a list'),
        ({'a': {'values': 'a1', 'is_primary_dimension': True}}, None,
         'a values must be a list'),
        ({'a': {'values': ['a1'], 'is_primary_dimension': 'false'}}, None,
         'a is_primary_dimension must be a bool'),
        ({'a': {'values': ['a1']}}, None, 'at least one dimension must be primary'),
        ({
            'a': {'values': ['a1', 'a2', 'a3'], 'is_primary_dimension': True},
            'b': {'values': ['b1', 'b2']},
            'c': {'values': ['c1', 'c2']}}, None,
         '3 primary cases cannot cover all 4 non-primary combinations'),
        ({
            'a': {
                'values': [pytest.param('a1', 'a2')],
                'is_primary_dimension': True}}, None,
         'each dimension value must contain exactly one pytest parameter value'),
        ({
            'a': {
                'values': [pytest.param('a1', marks=pytest.mark.skip(reason='test'))],
                'is_primary_dimension': True}}, None,
         'pytest parameter marks must not contain skip, skipif, or xfail'),
        ({
            'a': {
                'values': [pytest.param(
                    'a1', marks=pytest.mark.skipif(False, reason='test'))],
                'is_primary_dimension': True}}, None,
         'pytest parameter marks must not contain skip, skipif, or xfail'),
        ({
            'a': {
                'values': [pytest.param('a1', marks=pytest.mark.xfail(reason='test'))],
                'is_primary_dimension': True}}, None,
         'pytest parameter marks must not contain skip, skipif, or xfail'),
        *([({
            'a': {
                'values': [pytest.param('a1', id=pytest.HIDDEN_PARAM)],
                'is_primary_dimension': True}}, None,
            'pytest parameter ID must not be pytest.HIDDEN_PARAM')]
          if hasattr(pytest, 'HIDDEN_PARAM') else []),
        ({
            'a': {
                'values': [parameter_set_type(['a1'], (), None)],
                'is_primary_dimension': True}}, None,
         'pytest parameter values must be a tuple'),
        ({
            'a': {
                'values': [parameter_set_type(('a1',), (), 1)],
                'is_primary_dimension': True}}, None,
         'pytest parameter ID must be a string or None'),
        ({
            'a': {
                'values': [parameter_set_type(('a1',), set(), None)],
                'is_primary_dimension': True}}, None,
         'pytest parameter marks must be a list or tuple'),
        ({
            'a': {
                'values': [parameter_set_type(('a1',), ['not a pytest mark'], None)],
                'is_primary_dimension': True}}, None,
         'pytest parameter marks must contain only pytest marks'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, {},
         'extra_cases must be a list or tuple'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [('primary', 'p-extra')],
         'extra case 0 must be a dict'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [pytest.param('not-a-dict')],
         'extra case 0 must be a dict'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [pytest.param(
                {'primary': 'p-extra', 'secondary': 's-extra'}, 'second-value')],
         'each dimension value must contain exactly one pytest parameter value'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [{'primary': 'p-extra'}],
         "extra case 0 dimensions mismatch: missing=['secondary'], unexpected=[]"),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [{
                'primary': 'p-extra', 'secondary': 's-extra', 1: 'unexpected'}],
         'extra case 0 dimension names must be strings'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}},
         [{'primary': 'p-extra', 'secondary': 's-extra', 'unexpected': 'value'}],
         "extra case 0 dimensions mismatch: missing=[], unexpected=['unexpected']"),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [{
            'primary': pytest.param('p-extra', 'p-extra-2'),
            'secondary': 's-extra'}],
         'extra case 0 values must not be pytest.param'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [{
            'primary': pytest.param('p-extra', marks=pytest.mark.xfail(reason='test')),
            'secondary': 's-extra'}],
         'extra case 0 values must not be pytest.param'),
        ({
            'primary': {'values': ['p1'], 'is_primary_dimension': True},
            'secondary': {'values': ['s1']}}, [pytest.param(
                {'primary': 'p-extra', 'secondary': 's-extra'},
                marks=pytest.mark.xfail(reason='test'))],
         'pytest parameter marks must not contain skip, skipif, or xfail'),
    ]

    for dimensions, extra_cases, error_message in invalid_cases:
        with pytest.raises(ValueError) as exception:
            generate_reduced_test_matrix(dimensions, extra_cases=extra_cases)
        assert str(exception.value) == error_message
