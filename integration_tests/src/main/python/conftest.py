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
    non_gpu_conditional = item.get_closest_marker('allow_non_gpu_conditional')
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

    if non_gpu_conditional:
        condition = non_gpu_conditional.args[0]
        _non_gpu_allowed_conditional = non_gpu_conditional.args[1]
        if not isinstance(condition, bool):
            raise TypeError("The first parameter of 'allow_non_gpu_conditional' must be a Boolean.")
        if condition:
            if non_gpu_conditional.kwargs and non_gpu_conditional.kwargs['any']:
                _allow_any_non_gpu_conditional = True
                _non_gpu_allowed_conditional = []
            elif _non_gpu_allowed_conditional:
                _allow_any_non_gpu_conditional = False
            else:
                warnings.warn('allow_non_gpu_conditional marker without anything allowed')
                _allow_any_non_gpu_conditional = False
                _non_gpu_allowed_conditional = []


    _allow_any_non_gpu = _allow_any_non_gpu | _allow_any_non_gpu_databricks | _allow_any_non_gpu_conditional
    if _non_gpu_allowed and _non_gpu_allowed_databricks:
        _non_gpu_allowed = _non_gpu_allowed + _non_gpu_allowed_databricks
    elif _non_gpu_allowed_databricks:
        _non_gpu_allowed = _non_gpu_allowed_databricks
    if _non_gpu_allowed_conditional:
        _non_gpu_allowed = list(_non_gpu_allowed) + _non_gpu_allowed_conditional.split(",")

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

@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
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