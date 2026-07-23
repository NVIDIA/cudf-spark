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

import os
import calendar, time
from datetime import date, datetime
from contextlib import contextmanager, ExitStack
from conftest import is_allowing_any_non_gpu, get_non_gpu_allowed, get_validate_execs_in_gpu_plan, is_databricks_runtime, is_at_least_precommit_run, get_inject_oom_conf, is_per_test_ansi_mode_enabled
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType, DateType, _acceptable_types
from spark_init_internal import get_spark_i_know_what_i_am_doing, spark_version
from unittest.mock import patch

def _from_scala_map(scala_map):
    ret = {}
    # The value we get is a scala map, not a java map, so we need to jump through some hoops
    keys = scala_map.keys().iterator()
    while keys.hasNext():
        key = keys.next()
        ret[key] = scala_map.get(key).get()
    return ret

_spark = get_spark_i_know_what_i_am_doing()
# Have to reach into a private member to get access to the API we need
_orig_conf = _from_scala_map(_spark.conf._jconf.getAll())
_orig_conf_keys = _orig_conf.keys()

# Default settings that should apply to CPU and GPU sessions.
# These settings can be overridden by specific tests if necessary.
# Many of these are redundant with default settings for the configs but are set here explicitly
# to ensure any cluster settings do not interfere with tests that assume the defaults.
_default_conf = {
    'spark.rapids.sql.test.retryContextCheck.enabled': 'true',
    'spark.rapids.sql.castDecimalToFloat.enabled': 'false',
    'spark.rapids.sql.castFloatToDecimal.enabled': 'false',
    'spark.rapids.sql.castFloatToIntegralTypes.enabled': 'false',
    'spark.rapids.sql.castFloatToString.enabled': 'false',
    'spark.rapids.sql.castStringToFloat.enabled': 'false',
    'spark.rapids.sql.fast.sample': 'false',
    'spark.rapids.sql.hasExtendedYearValues': 'true',
    'spark.rapids.sql.hashOptimizeSort.enabled': 'false',
    'spark.rapids.sql.improvedFloatOps.enabled': 'false',
    'spark.rapids.sql.incompatibleDateFormats.enabled': 'false',
    'spark.rapids.sql.incompatibleOps.enabled': 'false',
    'spark.rapids.sql.mode': 'executeongpu',
    'spark.rapids.sql.variableFloatAgg.enabled': 'false',
    'spark.sql.legacy.allowNegativeScaleOfDecimal': 'true',
}

def _set_all_confs(conf):
    newconf = _default_conf.copy()
    inject_oom = get_inject_oom_conf()
    if inject_oom:
        _spark.conf.set("spark.rapids.sql.test.injectRetryOOM",
                         inject_oom.args[0] if len(inject_oom.args) > 0 else True)
    newconf.update(conf)
    for key, value in newconf.items():
        if _spark.conf.get(key, None) != value:
            _spark.conf.set(key, value)

def reset_spark_session_conf():
    """Reset all of the configs for a given spark session."""
    _set_all_confs(_orig_conf)
    #We should clear the cache
    _spark.catalog.clearCache()
    # Have to reach into a private member to get access to the API we need
    current_keys = _from_scala_map(_spark.conf._jconf.getAll()).keys()
    for key in current_keys:
        if key not in _orig_conf_keys:
            _spark.conf.unset(key)

def _check_for_proper_return_values(something):
    """We don't want to return an DataFrame or Dataset from a with_spark_session. You will not get what you expect"""
    if (isinstance(something, DataFrame)):
        raise RuntimeError("You should never return a DataFrame from a with_*_session, you will not get the results that you expect")

@contextmanager
def pyspark_compatibility_fixes():
    def timestampToInternal(_, dt):
        if isinstance(dt, int):
            return dt
        if isinstance(dt, datetime):
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            return int(seconds) * 1000000 + dt.microsecond

    def dateToInternal(self, d):
        if isinstance(d, int):
            return d
        if isinstance(d, (datetime, date)):
            return d.toordinal() - self.EPOCH_ORDINAL

    pyspark_compatibility_fixes = []
    # Patch timestamp and date types so that we only compare in testing to the internal integral representations
    pyspark_compatibility_fixes.append(patch.object(TimestampType, 'fromInternal', lambda _, ts: ts))
    pyspark_compatibility_fixes.append(patch.object(TimestampType, 'toInternal', timestampToInternal))
    pyspark_compatibility_fixes.append(patch.dict(_acceptable_types, { TimestampType: (datetime, int)}))
    try:
        from pyspark.sql.types import TimestampNTZType
        pyspark_compatibility_fixes.append(patch.object(TimestampNTZType, 'fromInternal', lambda _, ts: ts))
        pyspark_compatibility_fixes.append(patch.object(TimestampNTZType, 'toInternal', timestampToInternal))
        pyspark_compatibility_fixes.append(patch.dict(_acceptable_types, { TimestampNTZType: (datetime, int)}))
    except ImportError:
        pass
    pyspark_compatibility_fixes.append(patch.object(DateType, 'fromInternal', lambda _, v: v))
    pyspark_compatibility_fixes.append(patch.object(DateType, 'toInternal', dateToInternal))
    pyspark_compatibility_fixes.append(patch.dict(_acceptable_types, { DateType: (datetime, date, int)}))
    with ExitStack() as stack:
        for cm in pyspark_compatibility_fixes:
            stack.enter_context(cm)
        yield

@pyspark_compatibility_fixes()
def with_spark_session(func, conf={}):
    """Run func that takes a spark session as input with the given configs set."""
    reset_spark_session_conf()
    _add_job_description(conf)
    # Only set the ansi conf if not set by the test explicitly by setting the value in the dict
    if "spark.sql.ansi.enabled" not in conf and is_per_test_ansi_mode_enabled() is not None:
        conf["spark.sql.ansi.enabled"] = is_per_test_ansi_mode_enabled()
    _set_all_confs(conf)
    ret = func(_spark)
    _check_for_proper_return_values(ret)
    return ret


def _add_job_description(conf):
    is_gpu_job = conf.get('spark.rapids.sql.enabled', False)
    job_type = 'GPU' if str(is_gpu_job).lower() == str(True).lower() else 'CPU'
    job_desc = '{}[{}]'.format(os.environ.get('PYTEST_CURRENT_TEST'), job_type)
    _spark.sparkContext.setJobDescription(job_desc)


def with_cpu_session(func, conf={}):
    """Run func that takes a spark session as input with the given configs set on the CPU."""
    copy = dict(conf)
    copy['spark.rapids.sql.enabled'] = 'false'
    return with_spark_session(func, conf=copy)

def with_gpu_session(func, conf={}):
    """
    Run func that takes a spark session as input with the given configs set on the GPU.
    Note that this forces you into test mode unless.  It is not a requirement, but is
    simplest for right now.
    """
    copy = dict(conf)
    copy['spark.rapids.sql.enabled'] = 'true'
    if is_allowing_any_non_gpu():
        copy['spark.rapids.sql.test.enabled'] = 'false'
    else:
        copy['spark.rapids.sql.test.enabled'] = 'true'
        copy['spark.rapids.sql.test.allowedNonGpu'] = ','.join(get_non_gpu_allowed())

    copy['spark.rapids.sql.test.validateExecsInGpuPlan'] = ','.join(get_validate_execs_in_gpu_plan())
    return with_spark_session(func, conf=copy)

def is_before_spark_312():
    return spark_version() < "3.1.2"

def is_before_spark_313():
    return spark_version() < "3.1.3"

def is_before_spark_314():
    return spark_version() < "3.1.4"

def is_before_spark_320():
    return spark_version() < "3.2.0"

def is_before_spark_321():
    return spark_version() < "3.2.1"

def is_before_spark_322():
    return spark_version() < "3.2.2"

def is_before_spark_323():
    return spark_version() < "3.2.3"

def is_before_spark_331():
    return spark_version() < "3.3.1"

def is_before_spark_334():
    return spark_version() < "3.3.4"

def is_before_spark_340():
    return spark_version() < "3.4.0"

def is_before_spark_341():
    return spark_version() < "3.4.1"

def is_before_spark_342():
    return spark_version() < "3.4.2"

def is_before_spark_350():
    return spark_version() < "3.5.0"

def is_before_spark_351():
    return spark_version() < "3.5.1"

def is_before_spark_353():
    return spark_version() < "3.5.3"

def is_spark_350_or_351():
    return spark_version() >= "3.5.0" and spark_version() <= "3.5.1"

def is_before_spark_400():
    return spark_version() < "4.0.0"

def is_spark_320_or_later():
    return spark_version() >= "3.2.0"

def is_spark_330_or_later():
    return spark_version() >= "3.3.0"

def is_spark_340_or_later():
    return spark_version() >= "3.4.0"

def is_spark_341():
    return spark_version() == "3.4.1"

def is_spark_341_or_later():
    return spark_version() >= "3.4.1"

def is_spark_350_or_later():
    return spark_version() >= "3.5.0"

def is_spark_353_or_later():
    return spark_version() >= "3.5.3"

def is_spark_351_or_later():
    return spark_version() >= "3.5.1"

def is_spark_356_or_later():
    return spark_version() >= "3.5.6"

def is_spark_35x():
    return "3.5.0" <= spark_version() < "3.6.0"

def is_spark_40x():
    return "4.0.0" <= spark_version() < "4.1.0"

def is_spark_41x():
    return "4.1.0" <= spark_version() < "4.2.0"

def is_spark_420_or_later():
    return spark_version() >= "4.2.0"

def is_iceberg_supported_spark():
    return is_spark_35x() or is_spark_40x() or is_spark_41x()

def is_spark_400_or_later():
    return spark_version() >= "4.0.0"

def is_spark_401_or_later():
    return spark_version() >= "4.0.1"

def is_spark_403():
    return spark_version() == "4.0.3"

def is_spark_404():
    return spark_version() == "4.0.4"

def is_spark_403_or_404():
    return is_spark_403() or is_spark_404()

def is_spark_411_or_later():
    return spark_version() >= "4.1.1"

def is_spark_412_or_later():
    return spark_version() >= "4.1.2"

def is_spark_330():
    return spark_version() == "3.3.0"

def is_spark_350():
    return spark_version() == "3.5.0"

def is_spark_33X():
    return "3.3.0" <= spark_version() < "3.4.0"

def is_spark_321cdh():
    return "3.2.1.3.2.717" in spark_version()

def is_spark_330cdh():
    return "3.3.0.3.3.718" in spark_version()

def is_spark_332cdh():
    return "3.3.2.3.3.719" in spark_version()

def is_spark_cdh():
    return is_spark_321cdh() or is_spark_330cdh() or is_spark_332cdh()

def __databricks_version(major, minor):
    spark = get_spark_i_know_what_i_am_doing()
    version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "0.0")
    parts = version.split(".")
    if (len(parts) < 2):
        raise RuntimeError("Unable to determine Databricks version from version string: " + version)
    return int(parts[0]), int(parts[1])

def is_databricks_version(major, minor):
    db_major, db_minor = __databricks_version(major, minor)
    return db_minor == minor and db_major == major

def is_databricks_version_or_later(major, minor):
    db_major, db_minor = __databricks_version(major, minor)
    return db_minor >= minor if (db_major == major) else db_major >= major

def is_databricks104_or_later():
    return is_databricks_version_or_later(10, 4)

def is_databricks113_or_later():
    return is_databricks_version_or_later(11, 3)

def is_databricks122_or_later():
    return is_databricks_version_or_later(12, 2)

def is_databricks133_or_later():
    return is_databricks_version_or_later(13, 3)

def is_databricks133():
    return is_databricks_version(13, 3)

def is_databricks143():
    return is_databricks_version(14, 3)

def is_databricks143_or_later():
    return is_databricks_version_or_later(14, 3)

def is_databricks173_or_later():
    return is_databricks_version_or_later(17, 3)

def supports_delta_lake_deletion_vectors():
    if is_databricks_runtime():
        return is_databricks122_or_later()
    else:
        return is_spark_340_or_later()

def is_support_default_values_in_schema():
    # Spark 340 + and Databricks 330 + support
    return is_spark_340_or_later() or is_databricks113_or_later()

def is_spark_testing_enabled():
    # True when Spark testing mode is on (-Dspark.testing or the SPARK_TESTING env var), mirroring
    # org.apache.spark.util.Utils.isTesting. NVIDIA/spark-rapids#15022 enables it for the nightly,
    # which disables WholeStageCodegenExec's fallback from a codegen CompileException to interpreted
    # execution, so latent Spark codegen bugs become hard failures instead of being swallowed.
    return (_spark.sparkContext._jvm.System.getProperty("spark.testing") is not None
            or _spark.sparkContext._jvm.System.getenv("SPARK_TESTING") is not None)

def get_java_major_version():
    ver = _spark.sparkContext._jvm.System.getProperty("java.version")
    # Allow these formats:
    # 1.8.0_72-ea
    # 9-ea
    # 9
    # 11.0.1
    if ver.startswith('1.'):
        ver = ver[2:]
    dot_pos = ver.find('.')
    dash_pos = ver.find('-')
    if dot_pos != -1:
        ver = ver[0:dot_pos]
    elif dash_pos != -1:
        ver = ver[0:dash_pos]
    return int(ver)

def get_jvm_charset():
    sc = _spark.sparkContext
    return str(sc._jvm.java.nio.charset.Charset.defaultCharset())

def get_scala_version():
    sc = _spark.sparkContext
    return str(sc._jvm.scala.tools.nsc.Properties.versionNumberString())

def get_scala_binary_version():
    ".".join(get_scala_version().split(".")[:2])

def is_scala212():
    return get_scala_version().startswith("2.12")

def is_scala213():
    return get_scala_version().startswith("2.13")

def is_jvm_charset_utf8():
    return get_jvm_charset() == 'UTF-8'

def is_hive_available():
    # precommit and nightly runs are supposed to have Hive,
    # so tests should fail if Hive is missing in those environments.
    if is_at_least_precommit_run():
        return True
    return _spark.conf.get("spark.sql.catalogImplementation") == "hive"

def is_hybrid_backend_loaded():
    return _spark.conf.get("spark.rapids.sql.hybrid.loadBackend", "false") == "true"
