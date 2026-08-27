# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
import math
import os
import pytest
import random
import warnings

# TODO redo _spark stuff using fixtures
#
# Don't import pyspark / _spark directly in conftest globally
# import as a plugin to do a lazy per-pytest-session initialization
#
pytest_plugins = [
    'spark_init_internal'
]

_approximate_float_args = None
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

def get_float_check():
    if not _approximate_float_args is None:
        return lambda lhs,rhs: lhs == pytest.approx(rhs, **_approximate_float_args)
    else:
        return lambda lhs,rhs: lhs == rhs

_incompat = False

def is_incompat():
    return _incompat

_sort_on_spark = False
_sort_locally = False
_sort_array_columns_locally = []

def should_sort_on_spark():
    return _sort_on_spark

def should_sort_locally():
    return _sort_locally

def array_columns_to_sort_locally():
    return _sort_array_columns_locally

_allow_any_non_gpu = False
_non_gpu_allowed = []
_per_test_ansi_mode_enabled = None
_current_test_has_delta_marker = False
_current_test_allow_non_gpu_delta_write = False


_random_select_config = None

def is_allowing_any_non_gpu():
    return _allow_any_non_gpu

def get_non_gpu_allowed():
    return _non_gpu_allowed


def is_per_test_ansi_mode_enabled():
    return _per_test_ansi_mode_enabled


def current_test_has_delta_marker():
    """Check if the current test has the @delta_lake marker."""
    return _current_test_has_delta_marker


def current_test_allows_non_gpu_delta_write():
    """Check if the current test allows non-GPU delta write operations."""
    return _current_test_allow_non_gpu_delta_write


def get_validate_execs_in_gpu_plan():
    return _validate_execs_in_gpu_plan

_runtime_env = "apache"

def runtime_env():
    return _runtime_env.lower()

def is_apache_runtime():
    return runtime_env() == "apache"

def is_databricks_runtime():
    return runtime_env() == "databricks"

def is_emr_runtime():
    return runtime_env() == "emr"

def is_dataproc_runtime():
    return runtime_env() == "dataproc"

def is_dataproc_serverless_runtime():
    return runtime_env() == "dataproc_serverless"

def get_test_tz():
    return os.environ.get('TZ', 'UTC')

def is_utc():
    return get_test_tz() == "UTC"

def is_not_utc():
    return not is_utc()

def is_iceberg_remote_catalog():
    v = os.environ.get('ICEBERG_TEST_REMOTE_CATALOG')
    return v == "1"

def is_iceberg_rest_catalog():
    v = os.environ.get('ICEBERG_TEST_CATALOG_TYPE')
    return v == "rest"

# key is time zone, value is recorded boolean value
_support_info_cache_for_time_zone = {}

def is_supported_time_zone():
    """
    Is current TZ supported, forward to Java TimeZoneDB to check
    """
    tz = get_test_tz()
    if tz in _support_info_cache_for_time_zone:
        # already cached
        return _support_info_cache_for_time_zone[tz]
    else:
        jvm = spark_jvm()
        support = jvm.com.nvidia.spark.rapids.jni.GpuTimeZoneDB.isSupportedTimeZone(tz)
        # cache support info
        _support_info_cache_for_time_zone[tz] = support
        return support

_is_nightly_run = False
_is_precommit_run = False

def is_nightly_run():
    return _is_nightly_run

def is_precommit_run():
    return _is_precommit_run


def is_reduced_it_run():
    return os.environ.get('REDUCED_IT', 'false').lower() == 'true'


def is_at_least_precommit_run():
    return _is_nightly_run or _is_precommit_run

def skip_unless_nightly_tests(description):
    if (_is_nightly_run):
        raise AssertionError(description + ' during nightly test run')
    else:
        pytest.skip(description)

def skip_unless_precommit_tests(description):
    if (_is_nightly_run):
        raise AssertionError(description + ' during nightly test run')
    elif (_is_precommit_run):
        raise AssertionError(description + ' during pre-commit test run')
    else:
        pytest.skip(description)

_is_parquet_testing_tests_forced = False

def is_parquet_testing_tests_forced():
    return _is_parquet_testing_tests_forced

_limit = -1

_inject_oom = None


def get_inject_oom_conf():
    return _inject_oom


# For datagen: we expect a seed to be provided by the environment, or default to 0.
# Note that tests can override their seed when calling into datagen by setting seed= in their tests.
_test_datagen_random_seed = int(os.getenv("SPARK_RAPIDS_TEST_DATAGEN_SEED", 0))
_test_datagen_random_seed_user_provided = os.getenv("DATAGEN_SEED") is not None
provided_by_msg = "Provided by user with DATAGEN_SEED" if _test_datagen_random_seed_user_provided else "Automatically set"
_test_datagen_random_seed_init = _test_datagen_random_seed
print(f"Starting with datagen test seed: {_test_datagen_random_seed_init} ({provided_by_msg}). "
      "Set env variable DATAGEN_SEED to override.")

def get_datagen_seed():
    return _test_datagen_random_seed

def get_limit():
    return _limit

def _get_limit_from_mark(mark):
    if mark.args:
        return mark.args[0]
    else:
        return mark.kwargs.get('num_rows', 100000)

_std_input_path = None
def get_std_input_path():
    return _std_input_path

def _get_java_available_charsets():
    charset_names = spark_jvm().java.nio.charset.Charset.availableCharsets().keySet()
    iter_charsets = charset_names.iterator()
    charset_list = []
    while iter_charsets.hasNext():
        charset_list.append(iter_charsets.next())
    return charset_list

_jvm_available_charsets = None
def is_gbk_supported():
    global _jvm_available_charsets
    if _jvm_available_charsets is None:
        _jvm_available_charsets = _get_java_available_charsets()
    return 'GBK' in _jvm_available_charsets

def pytest_runtest_setup(item):
    global _sort_on_spark
    global _sort_locally
    global _sort_array_columns_locally
    global _inject_oom
    global _test_datagen_random_seed
    _inject_oom = item.get_closest_marker('inject_oom')
    datagen_overrides = item.get_closest_marker('datagen_overrides')
    _test_datagen_random_seed, _ = get_effective_seed(item, datagen_overrides)
    order = item.get_closest_marker('ignore_order')
    if order:
        if order.kwargs.get('local', False):
            _sort_on_spark = False
            _sort_locally = True
            _sort_array_columns_locally = order.kwargs.get('arrays', [])
        else:
            _sort_on_spark = True
            _sort_locally = False
    else:
        _sort_on_spark = False
        _sort_locally = False

    global _incompat
    if item.get_closest_marker('incompat'):
        _incompat = True
    else:
        _incompat = False

    global _approximate_float_args
    app_f = item.get_closest_marker('approximate_float')
    if app_f:
        _approximate_float_args = app_f.kwargs
    else:
        _approximate_float_args = None

    global _allow_any_non_gpu
    global _non_gpu_allowed
    global _per_test_ansi_mode_enabled
    _non_gpu_allowed_databricks = []
    _non_gpu_allowed_conditional = []
    _allow_any_non_gpu_databricks = False
    _allow_any_non_gpu_conditional = False
    non_gpu_databricks = item.get_closest_marker('allow_non_gpu_databricks')
    non_gpu = item.get_closest_marker('allow_non_gpu')
    _per_test_ansi_mode_enabled = None if item.get_closest_marker('disable_ansi_mode') is None \
      else not item.get_closest_marker('disable_ansi_mode')

    if non_gpu_databricks:
        if is_databricks_runtime():
            if non_gpu_databricks.kwargs and non_gpu_databricks.kwargs['any']:
                _allow_any_non_gpu_databricks = True
            elif non_gpu_databricks.args:
                _non_gpu_allowed_databricks = non_gpu_databricks.args
            else:
                warnings.warn('allow_non_gpu_databricks marker without anything allowed')
    if non_gpu:
        if non_gpu.kwargs and non_gpu.kwargs['any']:
            _allow_any_non_gpu = True
            _non_gpu_allowed = []
        elif non_gpu.args:
            _allow_any_non_gpu = False
            _non_gpu_allowed = non_gpu.args
        else:
            warnings.warn('allow_non_gpu marker without anything allowed')
            _allow_any_non_gpu = False
            _non_gpu_allowed = []
    else:
        _allow_any_non_gpu = False
        _non_gpu_allowed = []

    for non_gpu_conditional in item.iter_markers('allow_non_gpu_conditional'):
        # Validate the marker deterministically on BOTH condition branches so a
        # malformed marker fails everywhere, not only where its condition is true.
        unknown_kwargs = set(non_gpu_conditional.kwargs) - {'any'}
        if unknown_kwargs:
            raise TypeError(
                "allow_non_gpu_conditional got unexpected keyword argument(s) "
                f"{sorted(unknown_kwargs)}; only 'any' is supported.")
        allow_any = non_gpu_conditional.kwargs.get('any', False)
        if not isinstance(allow_any, bool):
            raise TypeError(
                "The 'any' parameter of 'allow_non_gpu_conditional' must be a Boolean.")
        if not non_gpu_conditional.args:
            raise TypeError(
                "The 'allow_non_gpu_conditional' marker requires a Boolean condition "
                "as its first argument.")
        condition = non_gpu_conditional.args[0]
        if not isinstance(condition, bool):
            raise TypeError(
                "The first parameter of 'allow_non_gpu_conditional' must be a Boolean.")
        op_args = non_gpu_conditional.args[1:]
        if not all(isinstance(arg, str) for arg in op_args):
            raise TypeError("allow_non_gpu_conditional op names must be strings.")
        ops = [op.strip() for arg in op_args for op in arg.split(',')]
        ops = [op for op in ops if op]
        if op_args and not ops:
            warnings.warn('allow_non_gpu_conditional marker with an empty ops payload')
        if condition:
            if allow_any:
                _allow_any_non_gpu_conditional = True
            elif ops:
                for op in ops:
                    if op not in _non_gpu_allowed_conditional:
                        _non_gpu_allowed_conditional.append(op)
            elif not op_args:
                warnings.warn('allow_non_gpu_conditional marker without anything allowed')


    _allow_any_non_gpu = _allow_any_non_gpu | _allow_any_non_gpu_databricks | _allow_any_non_gpu_conditional
    if _non_gpu_allowed and _non_gpu_allowed_databricks:
        _non_gpu_allowed = _non_gpu_allowed + _non_gpu_allowed_databricks
    elif _non_gpu_allowed_databricks:
        _non_gpu_allowed = _non_gpu_allowed_databricks
    if _non_gpu_allowed_conditional:
        _non_gpu_allowed = list(_non_gpu_allowed) + _non_gpu_allowed_conditional

    global _validate_execs_in_gpu_plan
    validate_execs = item.get_closest_marker('validate_execs_in_gpu_plan')
    if validate_execs and validate_execs.args:
        _validate_execs_in_gpu_plan = validate_execs.args
    else:
        _validate_execs_in_gpu_plan = []

    global _limit
    limit_mrk = item.get_closest_marker('limit')
    if limit_mrk:
        _limit = _get_limit_from_mark(limit_mrk)
    else:
        _limit = -1

    if item.get_closest_marker('iceberg'):
        if not item.config.getoption('iceberg'):
            pytest.skip('Iceberg tests not configured to run')
        elif is_databricks_runtime():
            pytest.skip('Iceberg tests skipped on Databricks')

    global _current_test_has_delta_marker
    _current_test_has_delta_marker = item.get_closest_marker('delta_lake') is not None
    global _current_test_allow_non_gpu_delta_write
    _current_test_allow_non_gpu_delta_write = False
    if _current_test_has_delta_marker:
        allow_non_gpu_delta_write_marker = item.get_closest_marker('allow_non_gpu_delta_write_if')
        if allow_non_gpu_delta_write_marker:
            # check argument length
            if len(allow_non_gpu_delta_write_marker.args) < 1:
                raise RuntimeError("The 'allow_non_gpu_delta_write_if' marker requires at least one argument.")
            cond = allow_non_gpu_delta_write_marker.args[0]
            if not isinstance(cond, bool):
                raise TypeError("The first parameter of 'allow_non_gpu_delta_write_if' must be a Boolean.")
            _current_test_allow_non_gpu_delta_write = cond
            if _current_test_allow_non_gpu_delta_write:
                reason = allow_non_gpu_delta_write_marker.kwargs.get('reason', 'no reason provided')
                warnings.warn(f'Delta Lake tests allowing non-GPU delta write operations: {reason}')

    if _current_test_has_delta_marker:
        if not item.config.getoption('delta_lake'):
            pytest.skip('delta lake tests not configured to run')

    if item.get_closest_marker('large_data_test'):
        if not item.config.getoption('large_data_test'):
            pytest.skip('tests for large data not configured to run')

    if item.get_closest_marker('pyarrow_test'):
        if not item.config.getoption('pyarrow_test'):
            pytest.skip('tests for pyarrow not configured to run')

def pytest_configure(config):
    global _runtime_env
    _runtime_env = config.getoption('runtime_env')
    global _std_input_path
    _std_input_path = config.getoption("std_input_path")
    global _is_nightly_run
    global _is_precommit_run
    test_type = config.getoption('test_type').lower()
    if "nightly" == test_type:
        _is_nightly_run = True
    elif "pre-commit" == test_type:
        _is_precommit_run = True
    elif "developer" != test_type:
        raise Exception("not supported test type {}".format(test_type))
    global _is_parquet_testing_tests_forced
    _is_parquet_testing_tests_forced = config.getoption("force_parquet_testing_tests")

# For OOM injection: we expect a seed to be provided by the environment, or default to 1.
# This is done such that any worker started by the xdist plugin for pytest will
# have the same seed. Since each worker creates a list of tests independently and then
# pytest expects this starting list to match for all workers, it is important that the same seed
# is set for all, either from the environment or as a constant.
oom_random_injection_seed = int(os.getenv("SPARK_RAPIDS_TEST_INJECT_OOM_SEED", 1))
print(f"Starting with OOM injection seed: {oom_random_injection_seed}. "
      "Set env variable SPARK_RAPIDS_TEST_INJECT_OOM_SEED to override.")

# Returns a tuple (seed, permanent) with the seed that test `item` should use given a 
# possibly defined `datagen_overrides`, and if the seed choice is due to an override, 
# whether that override is marked as `permanent`
def get_effective_seed(item, datagen_overrides):
    if datagen_overrides:
        # if the override is marked as permanent it will always override its seed
        # else, if the user provides a seed via DATAGEN_SEED, we will override.
        is_permanent = datagen_overrides.kwargs.get("permanent", False)

        override_condition = datagen_overrides.kwargs.get('condition', True)
        do_override = (
            # if the override condition is satisfied, we consider it
            override_condition and (
                # if the override is permanent, we always override
                # if it is not permanent, we consider it only if the user didn't
                #   set DATAGEN_SEED
                is_permanent or not _test_datagen_random_seed_user_provided))

        if do_override:
            try:
                seed = datagen_overrides.kwargs["seed"]
            except KeyError:
                raise Exception("datagen_overrides requires an override seed value")
            return (seed, is_permanent)

    return (_test_datagen_random_seed_init, False)

def _parse_random_select_config():
    value = os.getenv("RANDOM_SELECT")
    if value is None or value.strip() == "":
        return None
    value = value.strip()
    try:
        numeric_value = float(value)
    except ValueError:
        warnings.warn(f"Ignoring RANDOM_SELECT value '{value}': not a number")
        return None
    if numeric_value < 0:
        warnings.warn(f"Ignoring RANDOM_SELECT value '{value}': must be non-negative")
        return None
    config = {"raw": value}
    if numeric_value >= 1:
        config["mode"] = "count"
        config["target"] = int(numeric_value)
    else:
        if numeric_value == 0:
            config["mode"] = "count"
            config["target"] = 0
        else:
            config["mode"] = "fraction"
            config["target"] = numeric_value
    seed_value = os.getenv("RANDOM_SELECT_SEED")
    if seed_value is None or seed_value.strip() == "":
        seed = 0
    else:
        try:
            seed = int(seed_value)
        except ValueError:
            warnings.warn(f"Ignoring RANDOM_SELECT_SEED value '{seed_value}': not an int")
            seed = 0
    config["seed"] = seed
    return config

def _maybe_apply_random_select(config, items):
    if not _random_select_config:
        return
    total = len(items)
    if total == 0:
        return
    mode = _random_select_config["mode"]
    target = _random_select_config["target"]
    seed = _random_select_config["seed"]
    selected_count = total
    if mode == "count":
        selected_count = max(0, min(target, total))
    elif mode == "fraction":
        selected_count = min(total, max(0, math.ceil(total * target)))
    if selected_count >= total:
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        if reporter:
            reporter.write_line(
                f"RANDOM_SELECT active but requested {selected_count} tests >= total {total}; running all tests."
            )
        return
    rng = random.Random(seed)
    if selected_count == 0:
        selected_indices = []
    else:
        selected_indices = sorted(rng.sample(range(total), selected_count))
    selected_idx_set = set(selected_indices)
    deselected = [item for idx, item in enumerate(items) if idx not in selected_idx_set]
    items[:] = [item for idx, item in enumerate(items) if idx in selected_idx_set]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter:
        reporter.write_line(
            f"RANDOM_SELECT active: running {len(items)} of {total} tests "
            f"(seed={seed}, value={_random_select_config['raw']})."
        )

_random_select_config = _parse_random_select_config()


def _precommit_parametrize_factors(item):
    factors = []
    for position, mark in enumerate(item.iter_markers(name='parametrize')):
        argvalues = mark.args[1] if len(mark.args) > 1 else mark.kwargs['argvalues']
        try:
            size = len(argvalues)
        except TypeError:
            # Pytest accepts iterators here, but their size cannot be recovered after collection.
            return []
        factors.append((size, position))
    factors.sort(key=lambda factor: (-factor[0], factor[1]))
    return factors


def _combination_for_position(position, factors):
    # Reconstruct the per-decorator value indices from pytest's Cartesian-product ordering.
    indices = {}
    for size, original_position in sorted(factors, key=lambda factor: factor[1], reverse=True):
        indices[original_position] = position % size
        position //= size
    return tuple(indices[factor[1]] for factor in factors)


def _reduced_it_required_items(items):
    """Return (required_items, each_choice_test_count) for reduced IT selection.

    ``required_items`` is the subset of ``items`` to keep. Every value of every
    stacked ``parametrize`` decorator is guaranteed to appear in at least one
    kept item. Tests with fewer than two parametrize decorators, or whose
    collected cases do not form the expected Cartesian product (iterator,
    fixture, or otherwise dynamic parametrization), are kept in full. This is
    each-choice (1-wise) coverage, not pairwise: parameter interactions are not
    guaranteed. Kept as a pure helper so it can be unit tested without pytest
    config or pre-commit gating (see reduced_it_selection_test.py)."""
    groups = {}
    for item in items:
        groups.setdefault(item.nodeid.split('[', 1)[0], []).append(item)

    required = set()
    each_choice_test_count = 0
    for group_items in groups.values():
        first_item = group_items[0]
        factors = _precommit_parametrize_factors(first_item)
        expected_group_size = math.prod(factor[0] for factor in factors)
        if (len(factors) < 2
                or any(factor[0] == 0 for factor in factors)
                or len(group_items) != expected_group_size):
            # Preserve all cases when there is nothing to combine or collection includes
            # dynamic/fixture parametrization that cannot be mapped safely.
            required.update(group_items)
            continue

        selected_combinations = {
            # Factors are largest-first, so this produces the minimum number of combinations
            # needed to include every value from every factor.
            tuple(index % factor[0] for factor in factors)
            for index in range(factors[0][0])
        }
        each_choice_test_count += 1

        for position, item in enumerate(group_items):
            combination = _combination_for_position(position, factors)
            if combination in selected_combinations:
                required.add(item)
    return required, each_choice_test_count


def _select_precommit_cases(config, items):
    """Select each-choice combinations while covering every parameter value at least once."""
    if not is_precommit_run():
        return

    original_items = list(items)
    required, each_choice_test_count = _reduced_it_required_items(original_items)
    items[:] = [item for item in original_items if item in required]
    deselected = [item for item in original_items if item not in required]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        reporter = config.pluginmanager.get_plugin('terminalreporter')
        if reporter:
            reporter.write_line(
                f"REDUCED_IT active: running {len(items)} of {len(original_items)} tests "
                f"({len(items) / len(original_items):.1%}); "
                f"each-choice tests={each_choice_test_count}.")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    if is_precommit_run() and is_reduced_it_run():
        _select_precommit_cases(config, items)
    else:
        _maybe_apply_random_select(config, items)
    r = random.Random(oom_random_injection_seed)
    for item in items:
        extras = []
        order = item.get_closest_marker('ignore_order')
        # decide if OOMs should be injected, and when
        injection_mode_and_conf = config.getoption('test_oom_injection_mode').split(":")
        injection_mode = injection_mode_and_conf[0].lower()
        injection_conf = injection_mode_and_conf[1] if len(injection_mode_and_conf) == 2 else None
        inject_choice = False
        datagen_overrides = item.get_closest_marker('datagen_overrides')
        test_datagen_random_seed_choice, is_permanent = get_effective_seed(item, datagen_overrides)
        qualifier = ""
        if datagen_overrides:
            is_override = test_datagen_random_seed_choice != _test_datagen_random_seed_init
            qual_list = []
            # i.e. a @datagen_overrides(seed=x, permanent=True) would see:
            # DATAGEN_SEED_OVERRIDE_PERMANENT=x, and if it's not permanent
            # it would just be tagged as DATAGEN_SEED_OVERRIDE=x
            if is_override:
                qual_list += ["OVERRIDE"]
            if is_permanent:
                qual_list += ["PERMANENT"]
            qualifier = "_".join(qual_list)
            if len(qualifier) != 0:
                qualifier = "_" + qualifier # prefix separator for formatting purposes
        extras.append('DATAGEN_SEED%s=%s' % (qualifier, str(test_datagen_random_seed_choice)))
        extras.append('TZ=%s' % get_test_tz())

        if injection_mode == 'random':
            inject_choice = r.randrange(0, 2) == 1
        elif injection_mode == 'always':
            inject_choice = True
        if inject_choice:
            extras.append('INJECT_OOM_%s' % injection_conf if injection_conf else 'INJECT_OOM')
            item.add_marker(
                pytest.mark.inject_oom(injection_conf) if injection_conf else 'inject_oom',
                append=True)
        if order:
            if order.kwargs:
                extras.append('IGNORE_ORDER(' + str(order.kwargs) + ')')
            else:
                extras.append('IGNORE_ORDER')
        if item.get_closest_marker('incompat'):
            extras.append('INCOMPAT')
        app_f = item.get_closest_marker('approximate_float')
        if app_f:
            if app_f.kwargs:
                extras.append('APPROXIMATE_FLOAT(' + str(app_f.kwargs) + ')')
            else:
                extras.append('APPROXIMATE_FLOAT')
        non_gpu = item.get_closest_marker('allow_non_gpu')
        if non_gpu:
            if non_gpu.kwargs and non_gpu.kwargs['any']:
                extras.append('ALLOW_NON_GPU(ANY)')
            elif non_gpu.args:
                extras.append('ALLOW_NON_GPU(' + ','.join(non_gpu.args) + ')')

        limit_mrk = item.get_closest_marker('limit')
        if limit_mrk:
            extras.append('LIMIT({})'.format(_get_limit_from_mark(limit_mrk)))

        if extras:
            # This is not ideal because we are reaching into an internal value
            item._nodeid = item.nodeid + '[' + ', '.join(extras) + ']'

@pytest.fixture(scope="session")
def std_input_path(request):
    path = request.config.getoption("std_input_path")
    if path is None:
        skip_unless_precommit_tests("std_input_path is not configured")
    else:
        yield path

def get_worker_id(request):
    try:
        import xdist
        return xdist.plugin.get_xdist_worker_id(request)
    except ImportError:
        return 'main'

@pytest.fixture
def spark_tmp_path(request):
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    debug = request.config.getoption('debug_tmp_path')
    ret = request.config.getoption('tmp_path')
    if ret is None:
        ret = '/tmp/pyspark_tests/'
    worker_id = get_worker_id(request)
    pid = os.getpid()
    hostname = os.uname()[1]
    ret = f'{ret}/{hostname}-{worker_id}-{pid}-{random.randrange(0, 1<<31)}/'
    # Make sure it is there and accessible
    sc = get_spark_i_know_what_i_am_doing().sparkContext
    config = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(ret)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    fs.mkdirs(path)
    yield ret
    if not debug:
        fs.delete(path)

class TmpTableFactory:
  def __init__(self, base_id):
      self.base_id = base_id
      self.running_id = 0

  def get(self):
      ret = '{}_{}'.format(self.base_id, self.running_id)
      self.running_id = self.running_id + 1
      return ret

@pytest.fixture
def spark_tmp_table_factory(request):
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    worker_id = get_worker_id(request)
    table_id = random.getrandbits(31)
    base_id = f'tmp_table_{worker_id}_{table_id}'
    yield TmpTableFactory(base_id)
    # Drop table doesn't work spark sql with aws s3tables.
    if not is_iceberg_remote_catalog():
        sp = get_spark_i_know_what_i_am_doing()
        tables = sp.sql("SHOW TABLES").collect()
        for row in tables:
            t_name = row['tableName']
            if (t_name.startswith(base_id)):
                sp.sql("DROP TABLE IF EXISTS {} ".format(t_name))


def _get_jvm_session(spark):
    return spark._jsparkSession

def _get_jvm(spark):
    return spark.sparkContext._jvm

def spark_jvm():
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    return _get_jvm(get_spark_i_know_what_i_am_doing())

class MortgageRunner:
  def __init__(self, mortgage_format, mortgage_acq_path, mortgage_perf_path):
    self.mortgage_format = mortgage_format
    self.mortgage_acq_path = mortgage_acq_path
    self.mortgage_perf_path = mortgage_perf_path

  def do_test_query(self, spark):
    from pyspark.sql.dataframe import DataFrame
    jvm_session = _get_jvm_session(spark)
    jvm = _get_jvm(spark)
    acq = self.mortgage_acq_path
    perf = self.mortgage_perf_path
    run = jvm.com.nvidia.spark.rapids.tests.mortgage.Run
    if self.mortgage_format == 'csv':
        df = run.csv(jvm_session, perf, acq)
    elif self.mortgage_format == 'parquet':
        df = run.parquet(jvm_session, perf, acq)
    elif self.mortgage_format == 'orc':
        df = run.orc(jvm_session, perf, acq)
    else:
        raise AssertionError('Not Supported Format {}'.format(self.mortgage_format))

    return DataFrame(df, spark.getActiveSession())

@pytest.fixture(scope="session")
def mortgage(request):
    mortgage_format = request.config.getoption("mortgage_format")
    mortgage_path = request.config.getoption("mortgage_path")
    if mortgage_path is None:
        std_path = request.config.getoption("std_input_path")
        if std_path is None:
            skip_unless_precommit_tests("Mortgage tests are not configured to run")
        else:
            yield MortgageRunner('parquet', std_path + '/parquet_acq', std_path + '/parquet_perf')
    else:
        yield MortgageRunner(mortgage_format, mortgage_path + '/acq', mortgage_path + '/perf')

@pytest.fixture(scope="session")
def enable_cudf_udf(request):
    enable_udf_cudf = request.config.getoption("cudf_udf")
    if not enable_udf_cudf:
        # cudf_udf tests are not required for any test runs
        pytest.skip("cudf_udf not configured to run")

@pytest.fixture(scope="session")
def enable_fuzz_test(request):
    enable_fuzz_test = request.config.getoption("fuzz_test")
    if not enable_fuzz_test:
        # fuzz tests are not required for any test runs
        pytest.skip("fuzz_test not configured to run")

@pytest.fixture(scope="session")
def register_iceberg_add_eq_deletes_udf(request):
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    sp = get_spark_i_know_what_i_am_doing()
    from pyspark.sql.types import NullType
    sp.udf.registerJavaFunction("iceberg_add_eq_deletes",
                                   "com.nvidia.spark.rapids.iceberg.testutils.AddEqDeletes",
                                   NullType())
