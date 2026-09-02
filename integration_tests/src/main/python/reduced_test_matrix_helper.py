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

import itertools

import pytest

_PYTEST_PARAMETER_SET_TYPE = type(pytest.param(None))
_PYTEST_HIDDEN_PARAM = getattr(pytest, 'HIDDEN_PARAM', None)
_PYTEST_MARK_TYPES = (type(pytest.mark.skip), type(pytest.mark.skip.mark))
_DISALLOWED_PARAMETER_MARKS = {'skip', 'skipif', 'xfail'}


def _unwrap_dimension_value(dimension_value):
    """Unwrap one dimension-level ``pytest.param`` so it can be rebuilt at case level."""
    if not isinstance(dimension_value, _PYTEST_PARAMETER_SET_TYPE):
        return dimension_value, [], None
    if not isinstance(dimension_value.values, tuple):
        raise ValueError('pytest parameter values must be a tuple')
    if len(dimension_value.values) != 1:
        raise ValueError('each dimension value must contain exactly one pytest parameter value')
    if (_PYTEST_HIDDEN_PARAM is not None
            and dimension_value.id is _PYTEST_HIDDEN_PARAM):
        raise ValueError('pytest parameter ID must not be pytest.HIDDEN_PARAM')
    if dimension_value.id is not None and not isinstance(dimension_value.id, str):
        raise ValueError('pytest parameter ID must be a string or None')
    if not isinstance(dimension_value.marks, (list, tuple)):
        raise ValueError('pytest parameter marks must be a list or tuple')
    if any(not isinstance(mark, _PYTEST_MARK_TYPES) for mark in dimension_value.marks):
        raise ValueError('pytest parameter marks must contain only pytest marks')
    if any(mark.name in _DISALLOWED_PARAMETER_MARKS for mark in dimension_value.marks):
        raise ValueError('pytest parameter marks must not contain skip, skipif, or xfail')
    return dimension_value.values[0], dimension_value.marks, dimension_value.id


def _test_value_id(value, explicit_id):
    """Return a stable ID, preferring an explicit pytest ID."""
    if explicit_id is not None:
        return explicit_id
    if hasattr(value, '__name__'):
        return value.__name__
    return str(value)


def _create_matrix_case(normalized_dimensions, selected_values):
    """Create one outer pytest parameter, combining marks and IDs from all dimensions."""
    case_values = []
    case_marks = []
    case_ids = []
    for dimension in normalized_dimensions:
        value, marks, explicit_id = _unwrap_dimension_value(
            selected_values[dimension['name']])
        case_values.append(value)
        case_marks.extend(marks)
        case_ids.append(_test_value_id(value, explicit_id))
    return pytest.param(*case_values, marks=case_marks, id='-'.join(case_ids))


def generate_reduced_test_matrix(dimensions, extra_cases=None):
    """Reduce the Cartesian product across all test dimensions.

    Instead of generating the full Cartesian product of every dimension, this function generates
    the Cartesian product only from primary dimensions. It then distributes all combinations of
    secondary dimensions across that reduced primary matrix. Every primary combination and every
    secondary combination is retained, but redundant interactions between them are removed.

    For example, ``a`` and ``b`` below are primary dimensions, while ``c`` and ``d`` are secondary
    dimensions. The exhaustive Cartesian product would generate ``2 * 4 * 2 * 2 = 32`` cases.
    Retaining all ``2 * 4 = 8`` primary combinations and distributing the ``2 * 2 = 4``
    non-primary combinations reduces this to 8 cases, a reduction of 24 (75%)::

        test_matrix = generate_reduced_test_matrix({
            'a': {
                'values': ['a1', 'a2'],
                'is_primary_dimension': True},
            'b': {
                'values': ['b1', 'b2', 'b3', 'b4'],
                'is_primary_dimension': True},
            'c': {'values': ['c1', 'c2']},
            'd': {'values': ['d1', 'd2']}})

        for test_case in test_matrix:
            print(test_case.values)

    The printed tests are::

        ('a1', 'b1', 'c1', 'd1')
        ('a1', 'b2', 'c1', 'd2')
        ('a1', 'b3', 'c2', 'd1')
        ('a1', 'b4', 'c2', 'd2')
        ('a2', 'b1', 'c1', 'd1')
        ('a2', 'b2', 'c1', 'd2')
        ('a2', 'b3', 'c2', 'd1')
        ('a2', 'b4', 'c2', 'd2')

    ``dimensions`` must be a dict whose keys are string dimension names and whose values are
    dimension-config dicts. A dimension config must contain a non-empty list named ``values`` and
    may contain the boolean ``is_primary_dimension``. No other keys are accepted.
    Every item in ``values`` must be either a plain value or ``pytest.param`` containing exactly one
    value. For a ``pytest.param``, ``values`` must be a tuple, ``marks`` must be a list or tuple of
    pytest marks, and ``id`` must be a string or ``None``.
    Pytest does not recursively interpret a ``pytest.param`` nested inside a multi-argument case,
    so this function unwraps each dimension-level ``pytest.param`` and applies its marks and
    explicit ID to the generated outer ``pytest.param``. A dimension-level ``pytest.param`` must
    not have a ``skip``, ``skipif``, or ``xfail`` mark because reducing the matrix cannot preserve
    the original marked-case semantics safely. Its ID must not be ``pytest.HIDDEN_PARAM`` because
    IDs from all dimensions are combined into one outer parameter ID.

    * ``is_primary_dimension``: include its values in the primary Cartesian product.

    An explicit ``pytest.param`` ID is retained. Otherwise, a function's ``__name__`` or
    ``str(value)`` is used.

    The number of generated cases is
    ``len(primary_1) * len(primary_2) * ...``. Every concrete primary combination is retained.
    ``extra_cases`` may contain fully specified case dicts, or outer ``pytest.param`` values that
    each contain exactly one such dict, to append after the generated cases. Every key in an extra
    case dict must be a string, and the dict must provide exactly one plain value for every named
    dimension. Extra-case dict values must not be ``pytest.param``; case-level marks and IDs belong
    on the outer ``pytest.param``. Extra values do not need to appear in the dimension's normal
    ``values`` list.

    The Cartesian product of non-primary dimensions is cycled across the resulting primary cases.
    Marks from selected ``pytest.param`` values are kept. There must be at least as many primary
    cases as non-primary combinations; otherwise, some non-primary combinations cannot be covered
    and ``ValueError`` is raised. Returned value order matches the insertion order of the
    ``dimensions`` mapping.

    Returns a list of ``pytest.param`` values ready for one multi-argument ``parametrize`` marker.
    """
    if not isinstance(dimensions, dict):
        raise ValueError('dimensions must be a dict')
    if not dimensions:
        raise ValueError('at least one dimension is required')
    if extra_cases is None:
        extra_cases = []
    if not isinstance(extra_cases, (list, tuple)):
        raise ValueError('extra_cases must be a list or tuple')

    normalized_dimensions = []
    for name, dimension_config in dimensions.items():
        if not isinstance(name, str):
            raise ValueError('dimension name must be a string')
        if not isinstance(dimension_config, dict):
            raise ValueError('{} dimension config must be a dict'.format(name))
        unsupported_keys = set(dimension_config) - {'values', 'is_primary_dimension'}
        if unsupported_keys:
            raise ValueError(
                '{} dimension config contains unsupported keys: {}'.format(
                    name, sorted(unsupported_keys, key=str)))
        if 'values' not in dimension_config:
            raise ValueError('{} dimension config must contain values'.format(name))
        values = dimension_config['values']
        if not isinstance(values, list):
            raise ValueError('{} values must be a list'.format(name))
        if not values:
            raise ValueError('{} values must not be empty'.format(name))
        for dimension_value in values:
            _unwrap_dimension_value(dimension_value)
        is_primary_dimension = dimension_config.get('is_primary_dimension', False)
        if not isinstance(is_primary_dimension, bool):
            raise ValueError('{} is_primary_dimension must be a bool'.format(name))

        normalized_dimension = {
            'name': name,
            'values': values,
            'is_primary_dimension': is_primary_dimension}
        normalized_dimensions.append(normalized_dimension)

    primary_dimensions = [
        dimension for dimension in normalized_dimensions if dimension['is_primary_dimension']]
    if not primary_dimensions:
        raise ValueError('at least one dimension must be primary')
    secondary_dimensions = [
        dimension for dimension in normalized_dimensions if not dimension['is_primary_dimension']]
    secondary_value_ranges = [range(len(dimension['values']))
                              for dimension in secondary_dimensions]
    secondary_value_combinations = list(itertools.product(*secondary_value_ranges))

    primary_value_ranges = [range(len(dimension['values']))
                            for dimension in primary_dimensions]
    primary_value_combinations = list(itertools.product(*primary_value_ranges))
    if len(primary_value_combinations) < len(secondary_value_combinations):
        raise ValueError(
            '{} primary cases cannot cover all {} non-primary combinations'.format(
                len(primary_value_combinations), len(secondary_value_combinations)))

    test_matrix = []
    for case_index, primary_value_indices in enumerate(primary_value_combinations):
        selected_primary_values = {
            primary_dimension['name']: primary_dimension['values'][value_index]
            for primary_dimension, value_index in zip(
                primary_dimensions, primary_value_indices)}
        secondary_value_indices = secondary_value_combinations[
            case_index % len(secondary_value_combinations)]
        selected_secondary_values = {
            secondary_dimension['name']: secondary_dimension['values'][value_index]
            for secondary_dimension, value_index in zip(
                secondary_dimensions, secondary_value_indices)}

        selected_values = {**selected_primary_values, **selected_secondary_values}
        test_matrix.append(_create_matrix_case(normalized_dimensions, selected_values))

    dimension_names = set(dimensions)
    for case_index, extra_case_value in enumerate(extra_cases):
        extra_case_marks = []
        extra_case_id = None
        if isinstance(extra_case_value, _PYTEST_PARAMETER_SET_TYPE):
            extra_case, extra_case_marks, extra_case_id = _unwrap_dimension_value(
                extra_case_value)
        else:
            extra_case = extra_case_value
        if not isinstance(extra_case, dict):
            raise ValueError('extra case {} must be a dict'.format(case_index))
        if any(not isinstance(name, str) for name in extra_case):
            raise ValueError('extra case {} dimension names must be strings'.format(case_index))
        extra_dimension_names = set(extra_case)
        if extra_dimension_names != dimension_names:
            missing_names = sorted(dimension_names - extra_dimension_names)
            unexpected_names = sorted(extra_dimension_names - dimension_names)
            raise ValueError(
                'extra case {} dimensions mismatch: missing={}, unexpected={}'.format(
                    case_index, missing_names, unexpected_names))
        if any(isinstance(value, _PYTEST_PARAMETER_SET_TYPE) for value in extra_case.values()):
            raise ValueError('extra case {} values must not be pytest.param'.format(case_index))
        matrix_case = _create_matrix_case(normalized_dimensions, extra_case)
        if extra_case_marks or extra_case_id is not None:
            matrix_case = pytest.param(
                *matrix_case.values,
                marks=extra_case_marks,
                id=extra_case_id if extra_case_id is not None else matrix_case.id)
        test_matrix.append(matrix_case)
    return test_matrix
