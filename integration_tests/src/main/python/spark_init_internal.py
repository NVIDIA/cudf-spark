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

import logging
import os
import pytest
import re
import stat
import traceback

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

_CONF_ENV_PREFIX = 'PYSP_TEST_'
_EXECUTOR_ENV_PREFIX = 'spark_executorEnv_'

def env_for_conf(spark_conf_name):
    # escape underscores
    escaped_conf = spark_conf_name.replace('_', r'__')
    return _CONF_ENV_PREFIX + escaped_conf.replace('.', '_')

def conf_for_env(env_name):
    conf_key = env_name[len(_CONF_ENV_PREFIX):]
    if conf_key.startswith(_EXECUTOR_ENV_PREFIX):
        res = _EXECUTOR_ENV_PREFIX.replace('_', '.') + conf_key[len(_EXECUTOR_ENV_PREFIX):]
    else:
        # replace standalone underscores
        res1 = re.sub(r'(?<!_)_(?!_)', '.', conf_key)
        # unescape: remove duplicate underscores
        res = res1.replace('__', '_')
    return res

_DRIVER_ENV = env_for_conf('spark.driver.extraJavaOptions')
_SPARK_JARS = env_for_conf("spark.jars")
_SPARK_JARS_PACKAGES = env_for_conf("spark.jars.packages")
spark_jars_env = {
    _SPARK_JARS,
    _SPARK_JARS_PACKAGES
}
# Initialized in pytest_sessionstart; declared here so pytest_sessionfinish
# does not raise NameError if session startup fails before assignment.
_spark = None

def findspark_init():
    import findspark
    findspark.init()
    logging.info("Checking if add_jars/packages to findspark required")
    spark_jars = os.getenv(_SPARK_JARS)
    spark_jars_packages = os.getenv(_SPARK_JARS_PACKAGES)
    if spark_jars is not None:
        logging.info(f"Adding to findspark jars: {spark_jars}")
        findspark.add_jars(spark_jars)

    if spark_jars_packages is not None:
        logging.info(f"Adding to findspark packages: {spark_jars_packages}")
        findspark.add_packages(spark_jars_packages)

def running_with_xdist(session, is_worker):
    try:
        import xdist
        return xdist.is_xdist_worker(session) if is_worker\
            else xdist.is_xdist_master(session)
    except ImportError:
        return False


def pyspark_ready():
    try:
        import pyspark
        return True
    except ImportError:
        return False


def global_init():
    logging.info("Executing global initialization tasks before test launches")
    create_tmp_hive()

def create_tmp_hive():
    path = os.environ.get('PYSP_TEST_spark_hadoop_hive_exec_scratchdir', '/tmp/hive')
    mode = stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
    logging.info(f"Creating directory {path} with permissions {oct(mode)}")
    try:
        os.makedirs(path, mode, exist_ok=True)
        os.chmod(path, mode)
    except Exception as e:
        logging.warn(f"Failed to setup the hive scratch dir {path}. Error {e}")

# Entry point into this file
def pytest_sessionstart(session):
    # initializations that must happen globally once before tests start
    # if xdist in the coordinator, if not xdist in the pytest process
    if not running_with_xdist(session, is_worker=True):
        global_init()

    if running_with_xdist(session, is_worker = True):
        logging.info("Initializing findspark because running with xdist worker")
        findspark_init()
    elif running_with_xdist(session, is_worker = False):
        logging.info("Skipping findspark init because on xdist master")
        return
    elif not pyspark_ready():
        logging.info("Initializing findspark because pyspark unimportable on a standalone Pytest instance")
        findspark_init()

    import pyspark
    from py4j.java_gateway import java_import

    # Force the RapidsPlugin to be enabled, so it blows up if the classpath is not set properly
    # DO NOT SET ANY OTHER CONFIGS HERE!!!
    # due to bugs in pyspark/pytest it looks like any configs set here
    # can be reset in the middle of a test if specific operations are done (some types of cast etc)
    _sb = pyspark.sql.SparkSession.builder
    _sb.config('spark.plugins', 'com.nvidia.spark.SQLPlugin') \
            .config("spark.sql.adaptive.enabled", "true") \
            .config('spark.sql.queryExecutionListeners', 'org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback')

    for key, value in os.environ.items():
        if key.startswith(_CONF_ENV_PREFIX) and key != _DRIVER_ENV and key not in spark_jars_env:
            _sb.config(conf_for_env(key), value)

    driver_opts = os.environ.get(_DRIVER_ENV, "")

    if ('PYTEST_XDIST_WORKER' in os.environ):
        wid = os.environ['PYTEST_XDIST_WORKER']
        _handle_event_log_dir(_sb, wid)
        driver_opts += _get_driver_opts_for_worker_logs(_sb, wid)
        _handle_derby_dir(_sb, driver_opts, wid)
        _handle_ivy_cache_dir(_sb, wid)
    else:
        driver_opts += _get_driver_opts_for_worker_logs(_sb, 'gw0')
        _sb.config('spark.driver.extraJavaOptions', driver_opts)
        _handle_event_log_dir(_sb, 'gw0')

    # enableHiveSupport() is needed for parquet bucket tests
    _s = _sb.enableHiveSupport() \
            .appName('rapids spark plugin integration tests (python)').getOrCreate()
    #TODO catch the ClassNotFound error that happens if the classpath is not set up properly and
    # make it a better error message
    _s.sparkContext.setLogLevel("WARN")
    java_import(_s._jvm, 'org.apache.spark.rapids.tests.TimeoutSparkListener')
    # TODO dial down after identifying all long tests
    # and set exceptions there
    global default_timeout_seconds
    global default_dump_threads
    default_timeout_seconds = 60 * 60
    default_dump_threads = True
    _s._jvm.org.apache.spark.rapids.tests.TimeoutSparkListener.init(_s._jsc)
    global _spark
    _spark = _s


def _handle_derby_dir(sb, driver_opts, wid):
    d = "./derby_{}".format(wid)
    if not os.path.exists(d):
        os.makedirs(d)
    sb.config('spark.driver.extraJavaOptions', driver_opts + ' -Dderby.system.home={}'.format(d))

def _use_worker_logs():
    return os.environ.get('USE_WORKER_LOGS') == '1'

# Create a named logger to be used for only logging test name in `log_test_name`
logger = logging.getLogger('__pytest_worker_logger__')
def _get_driver_opts_for_worker_logs(_sb, wid):
    if not _use_worker_logs():
        logging.info("Not setting worker logs. Worker logs on non-local mode are sent to the location pre-configured "
                     "by the user")
        return ""

    current_directory = os.path.abspath(os.path.curdir)
    log_file = '{}/{}_worker_logs.log'.format(current_directory, wid)

    from conftest import get_std_input_path
    std_input_path = get_std_input_path()
    # This is not going to take effect when TEST_PARALLEL=1 as it's set as a conf when calling spark-submit
    driver_opts = ' -Dlog4j.configuration=file://{}/pytest_log4j.properties '.format(std_input_path) + \
        ' -Dlogfile={}'.format(log_file)

    # Set up Logging to the WORKERID_worker_logs
    # Note: This logger is only used for logging the test name in method `log_test_name`.
    global logger
    logger.setLevel(logging.INFO)
    # Create file handler to output logs into corresponding worker log file
    # This file_handler is modifying the worker_log file that the plugin will also write to
    # The reason for doing this is to get all test logs in one place from where we can do other analysis
    # that might be needed in future to look at the execs that were used in our integration tests
    file_handler = logging.FileHandler(log_file)
    # Set the formatter for the file handler, we match the formatter from the basicConfig for consistency in logs
    formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")

    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    return driver_opts

def _handle_event_log_dir(sb, wid):
    if os.environ.get('SPARK_EVENTLOG_ENABLED', str(True)).lower() in [
        str(False).lower(), 'off', '0'
    ]:
        logging.info('Automatic configuration for spark event log disabled')
        return

    import pyspark
    spark_conf = pyspark.SparkConf()
    master_url = os.environ.get(env_for_conf('spark.master'),
                                spark_conf.get("spark.master", 'local'))
    event_log_config = os.environ.get(env_for_conf('spark.eventLog.enabled'),
                                      spark_conf.get('spark.eventLog.enabled', str(False).lower()))
    event_log_codec = os.environ.get(env_for_conf('spark.eventLog.compression.codec'), 'zstd')

    if not master_url.startswith('local') or event_log_config != str(False).lower():
        logging.info("SPARK_EVENTLOG_ENABLED is ignored for non-local Spark master and when "
              "it's pre-configured by the user")
        return
    d = "./eventlog_{}".format(wid)
    if not os.path.exists(d):
        # Set 'exist_ok' as True to avoid raising 'FileExistsError' as the folder might be created
        # by other tests when they are executed in parallel
        os.makedirs(d, exist_ok=True)

    logging.info('Spark event logs will appear under {}. Set the environmnet variable '
          'SPARK_EVENTLOG_ENABLED=false if you want to disable it'.format(d))

    sb\
        .config('spark.eventLog.dir', "file://{}".format(os.path.abspath(d))) \
        .config('spark.eventLog.compress', True) \
        .config('spark.eventLog.enabled', True) \
        .config('spark.eventLog.compression.codec', event_log_codec)

def _handle_ivy_cache_dir(sb, wid):
    if os.environ.get('SPARK_IVY_CACHE_ENABLED', str(True)).lower() in [
        str(False).lower(), 'off', '0'
    ]:
        logging.info('Automatic configuration for spark ivy cache dir disabled')
        return
    sb.config('spark.jars.ivy', '/tmp/.ivy2_{}'.format(wid))

def get_spark_i_know_what_i_am_doing():
    """
    Get the current SparkSession.
    This should almost never be called directly instead you should call
    with_spark_session, with_cpu_session, or with_gpu_session for spark_session.
    This is to guarantee that the session and it's config is setup in a repeatable way.
    """
    return _spark

def spark_version():
    return _spark.version

@pytest.fixture(scope='function', autouse=_use_worker_logs())
def log_test_name(request):
    logger.info("Running test '{}'".format(request.node.nodeid))


def _set_job_timeout_and_crash_when_failed(spark_timeout, dump_threads):
    try:
        _spark._jvm.org.apache.spark.rapids.tests.TimeoutSparkListener.setSparkJobTimeout(
            spark_timeout,
            dump_threads
        )
    except Exception as e:
        logger.error(f"""set_spark_job_timeout: Unusable Spark Driver JVM detected!
                     Crashing pytest worker to mitigate: {traceback.format_exc()}""")
        os._exit(os.EX_UNAVAILABLE)


@pytest.fixture(scope="function", autouse=True)
def set_spark_job_timeout(request):
    logger.debug("set_spark_job_timeout: BEFORE TEST\n")
    tm = request.node.get_closest_marker("spark_job_timeout")
    if tm:
        spark_timeout = tm.kwargs.get('seconds', default_timeout_seconds)
        dump_threads = tm.kwargs.get('dump_threads', default_dump_threads)
    else:
        spark_timeout = default_timeout_seconds
        dump_threads = default_dump_threads
    # before the test
    _set_job_timeout_and_crash_when_failed(spark_timeout, dump_threads)
    # yield for test
    yield
    # after the test
    _set_job_timeout_and_crash_when_failed(spark_timeout, dump_threads)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    # Shut down SparkSession after all tests and report generation complete.
    # On Spark 4.0+ / Databricks 17.3+, SparkConnectPlugin starts gRPC/Netty
    # threads that keep the JVM alive even after Python exits.  An explicit
    # spark.stop() triggers the plugin shutdown hook which cleans up those threads.
    if running_with_xdist(session, is_worker=False):
        logging.info("Skipping SparkSession stop on xdist coordinator")
        return
    global _spark
    if _spark is not None:
        logging.info("Stopping SparkSession at end of test session")
        try:
            _spark.stop()
        except Exception as e:
            logging.warning(f"Exception while stopping SparkSession: {e}")
        finally:
            _spark = None

