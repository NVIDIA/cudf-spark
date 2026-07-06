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

from conftest import (is_incompat, should_sort_on_spark, should_sort_locally, array_columns_to_sort_locally, get_float_check,
                      get_limit, spark_jvm, current_test_has_delta_marker, current_test_allows_non_gpu_delta_write)
from datetime import date, datetime, timedelta
from decimal import Decimal
import math
from pyspark.sql import Row

import pytest
from spark_session import with_cpu_session, with_gpu_session
import time
import types as pytypes
import data_gen
import difflib
import sys
import re

def _assert_equal(cpu, gpu, float_check, path):
    t = type(cpu)
    if (t is Row):
        assert len(cpu) == len(gpu), "CPU and GPU row have different lengths at {} CPU: {} GPU: {}".format(path, len(cpu), len(gpu))
        if hasattr(cpu, "__fields__") and hasattr(gpu, "__fields__"):
            assert cpu.__fields__ == gpu.__fields__, "CPU and GPU row have different fields at {} CPU: {} GPU: {}".format(path, cpu.__fields__, gpu.__fields__)
            for field in cpu.__fields__:
                _assert_equal(cpu[field], gpu[field], float_check, path + [field])
        else:
            for index in range(len(cpu)):
                _assert_equal(cpu[index], gpu[index], float_check, path + [index])
    elif (t is list):
        assert len(cpu) == len(gpu), "CPU and GPU list have different lengths at {} CPU: {} GPU: {}".format(path, len(cpu), len(gpu))
        for index in range(len(cpu)):
            _assert_equal(cpu[index], gpu[index], float_check, path + [index])
    elif (t is tuple):
        assert len(cpu) == len(gpu), "CPU and GPU list have different lengths at {} CPU: {} GPU: {}".format(path, len(cpu), len(gpu))
        for index in range(len(cpu)):
            _assert_equal(cpu[index], gpu[index], float_check, path + [index])
    elif (t is pytypes.GeneratorType):
        index = 0
        # generator has no zip :( so we have to do this the hard way
        done = False
        while not done:
            sub_cpu = None
            sub_gpu = None
            try:
                sub_cpu = next(cpu)
            except StopIteration:
                done = True

            try:
                sub_gpu = next(gpu)
            except StopIteration:
                done = True

            if done:
                assert sub_cpu == sub_gpu and sub_cpu == None, "CPU and GPU generators have different lengths at {}".format(path)
            else:
                _assert_equal(sub_cpu, sub_gpu, float_check, path + [index])

            index = index + 1
    elif (t is dict):
        # The order of key/values is not guaranteed in python dicts, nor are they guaranteed by Spark
        # so sort the items to do our best with ignoring the order of dicts
        cpu_items = list(cpu.items()).sort(key=_RowCmp)
        gpu_items = list(gpu.items()).sort(key=_RowCmp)
        _assert_equal(cpu_items, gpu_items, float_check, path + ["map"])
    elif (t is int):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) int values are different at {path}"
    elif (t is float):
        if (math.isnan(cpu)):
            assert math.isnan(gpu), f"GPU ({gpu}) and CPU (nan) float values are different at {path}"
        else:
            assert float_check(cpu, gpu), f"GPU ({gpu}) and CPU ({cpu}) float values are different at {path}"
    elif isinstance(cpu, str):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) string values are different at {path}"
    elif isinstance(cpu, datetime):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) timestamp values are different at {path}"
    elif isinstance(cpu, date):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) date values are different at {path}"
    elif isinstance(cpu, bool):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) boolean values are different at {path}"
    elif isinstance(cpu, Decimal):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) decimal values are different at {path}"
    elif isinstance(cpu, bytearray):
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) bytearray values are different at {path}"
    elif isinstance(cpu, bytes):
        # Spark 4.1.0+ returns bytes instead of bytearray for binary data
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) bytes values are different at {path}"
    elif isinstance(cpu, timedelta):
        # Used by interval type DayTimeInterval for Pyspark 3.3.0+
        assert cpu == gpu, f"GPU ({gpu}) and CPU ({cpu}) timedelta values are different at {path}"
    elif (cpu == None):
        assert cpu == gpu, f"GPU ({gpu}) and CPU (null) values are different at {path}"
    else:
        assert False, "Found unexpected type {} at {}".format(t, path)

def assert_equal_with_local_sort(cpu, gpu):
    _sort_locally(cpu, gpu)
    assert_equal(cpu, gpu)

def assert_equal(cpu, gpu):
    """Verify that the result from the CPU and the GPU are equal"""
    try:
        _assert_equal(cpu, gpu, float_check=get_float_check(), path=[])
    except:
        def to_txt(data):
            try:
                # Try to iterate over the data
                return [f"{x}\n" for x in data]
            except TypeError:
                # If not iterable, convert to string directly
                return [f"{data}\n"]
            
        sys.stdout.writelines(difflib.unified_diff(
            a=to_txt(cpu),
            b=to_txt(gpu),
            fromfile='CPU OUTPUT',
            tofile='GPU OUTPUT'))
        raise

def _has_incompat_conf(conf):
    return ('spark.rapids.sql.incompatibleOps.enabled' in conf and
            conf['spark.rapids.sql.incompatibleOps.enabled'].lower() == 'true')

class _RowCmp(object):
    """Allows for sorting Rows in a consistent way"""
    def __init__(self, wrapped):
        if isinstance(wrapped, Row) or isinstance(wrapped, list) or isinstance(wrapped, tuple):
            self.wrapped = [_RowCmp(c) for c in wrapped]
        elif isinstance(wrapped, dict):
            def sort_dict(e):
                return _RowCmp(e)
            tmp = [(k, v) for k, v in wrapped.items()]
            tmp.sort(key=sort_dict)
            self.wrapped = [_RowCmp(c) for c in tmp]
        else:
            self.wrapped = wrapped

        if isinstance(wrapped, float):
            self.is_nan = math.isnan(wrapped)
        else:
            self.is_nan = False

    def cmp(self, other):
        try:
            #None comes before anything else
            #NaN comes next
            if (self.wrapped is None and other.wrapped is None):
                return 0
            elif (self.wrapped is None):
                return -1
            elif (other.wrapped is None):
                return 1
            elif self.is_nan and other.is_nan:
                return 0
            elif self.is_nan:
                return -1
            elif other.is_nan:
                return 1
            elif self.wrapped == other.wrapped:
                return 0
            elif self.wrapped < other.wrapped:
                return -1
            else:
                return 1
        except TypeError as te:
            print("ERROR TRYING TO COMPARE {} to {} {}".format(self.wrapped, other.wrapped, te))
            raise te


    def __lt__(self, other):
        return self.cmp(other) < 0

    def __gt__(self, other):
        return self.cmp(other) > 0

    def __eq__(self, other):
        return self.cmp(other) == 0

    def __le__(self, other):
        return self.cmp(other) <= 0

    def __ge__(self, other):
        return self.cmp(other) >= 0

    def __ne__(self, other):
        return self.cmp(other) != 0

def _prep_func_for_compare(func, mode):
    sort_locally = should_sort_locally()
    if should_sort_on_spark():
        def with_sorted(spark):
            df = func(spark)
            return df.sort([f"`{x}`" for x in df.columns])

        sorted_func = with_sorted
    else:
        sorted_func = func

    limit_val = get_limit()
    if limit_val > 0:
        def with_limit(spark):
            df = sorted_func(spark)
            return df.limit(limit_val)
        limit_func = with_limit
    else:
        limit_func = sorted_func

    if mode == 'COLLECT':
        bring_back = lambda spark: limit_func(spark).collect()
        collect_type = 'COLLECT'
    elif mode == 'COUNT':
        bring_back = lambda spark: limit_func(spark).count()
        collect_type = 'COUNT'
    elif mode == 'COLLECT_WITH_DATAFRAME':
        def bring_back(spark):
            df = limit_func(spark)
            return (df.collect(), df)
        collect_type = 'COLLECT'
        return (bring_back, collect_type)
    else:
        bring_back = lambda spark: limit_func(spark).toLocalIterator()
        collect_type = 'ITERATOR'
        if sort_locally:
            raise RuntimeError('Local Sort is only supported on a collect')
    return (bring_back, collect_type)

# Sort each of the result sets. If there are array columns to sort, 
# then sort each of those values in each row
def _sort_locally(*results):
    array_columns = array_columns_to_sort_locally()
    def sort_rows(rows):
        if array_columns:
            for r in rows:
                for col in array_columns:
                    r[col].sort(key=_RowCmp)
        rows.sort(key=_RowCmp)

    for rows in results:
        sort_rows(rows)

def _prep_incompat_conf(conf):
    if is_incompat():
        conf = dict(conf) # Make a copy before we change anything
        conf['spark.rapids.sql.incompatibleOps.enabled'] = 'true'
    elif _has_incompat_conf(conf):
        raise AssertionError("incompat must be enabled by the incompat fixture")
    return conf

def _assert_gpu_and_cpu_writes_are_equal(
        write_func,
        read_func,
        base_path,
        mode,
        conf={}):
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_path = base_path + '/CPU'
    cpu_result = with_cpu_session(lambda spark : write_func(spark, cpu_path), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    gpu_path = base_path + '/GPU'

    # Check if current test has delta_lake marker
    if current_test_has_delta_marker() and not current_test_allows_non_gpu_delta_write():
        print("Delta Lake test detected - applying Delta write validation")
        from delta_lake_utils import assert_rapids_delta_write
        gpu_result = assert_rapids_delta_write(lambda spark: write_func(spark, gpu_path), conf=conf)
    else:
        gpu_result = with_gpu_session(lambda spark : write_func(spark, gpu_path), conf=conf)
    gpu_end = time.time()
    assert_equal(cpu_result, gpu_result)
    print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
        gpu_end - gpu_start, cpu_end - cpu_start))

    (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, cpu_path), mode)
    (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, gpu_path), mode)

    from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
    from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    assert_equal(from_cpu, from_gpu)

def assert_gpu_and_cpu_writes_are_equal_collect(write_func, read_func, base_path, conf={}):
    """
    Assert when running write_func on both the CPU and the GPU and reading using read_func
    on the CPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    _assert_gpu_and_cpu_writes_are_equal(write_func, read_func, base_path, 'COLLECT', conf=conf)

def assert_gpu_and_cpu_writes_are_equal_iterator(write_func, read_func, base_path, conf={}):
    """
    Assert when running write_func on both the CPU and the GPU and reading using read_func
    on the CPU that the results are equal.
    In this case the data is pulled back to the driver in chunks and compared here
    so any amount of data can work, just be careful about how long it might take.
    """
    _assert_gpu_and_cpu_writes_are_equal(write_func, read_func, base_path, 'ITERATOR', conf=conf)

def assert_gpu_and_cpu_save_as_table_are_equal_collect(table_name_factory, write_func, conf={}):
    """
    Assert when running write_func on both the CPU and the GPU and reading
    on the CPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_table = table_name_factory.get() + '_cpu'
    with_cpu_session(lambda spark : write_func(spark, cpu_table), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    gpu_table = table_name_factory.get() + '_gpu'
    # Check if current test has delta_lake marker
    if current_test_has_delta_marker() and not current_test_allows_non_gpu_delta_write():
        print("Delta Lake test detected - applying Delta write validation")
        from delta_lake_utils import assert_rapids_delta_write
        assert_rapids_delta_write(lambda spark : write_func(spark, gpu_table), conf=conf)
    else:
        with_gpu_session(lambda spark : write_func(spark, gpu_table), conf=conf)

    gpu_end = time.time()
    print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
        gpu_end - gpu_start, cpu_end - cpu_start))

    mode = "COLLECT"
    (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
        lambda spark: spark.sql("SELECT * FROM {}".format(cpu_table)), mode)
    (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
        lambda spark: spark.sql("SELECT * FROM {}".format(gpu_table)), mode)

    from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
    from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    assert_equal(from_cpu, from_gpu)

    return (cpu_table, gpu_table)

def assert_gpu_and_cpu_sql_writes_are_equal_collect(table_name_factory, write_sql_func, conf={}):
    """
    Assert when running SQL text from write_sql_func on both the CPU and the GPU and reading
    both resulting tables on the CPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    """
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_table = table_name_factory.get()
    cpu_start = time.time()
    def do_write(spark, table_name):
        sql_texts = write_sql_func(spark, table_name)
        sql_text_list = [sql_texts] if isinstance(sql_texts, str) else sql_texts
        for sql_text in sql_text_list:
            spark.sql(sql_text)
        return None
    with_cpu_session(lambda spark : do_write(spark, cpu_table), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    gpu_table = table_name_factory.get()
    with_gpu_session(lambda spark : do_write(spark, gpu_table), conf=conf)
    gpu_end = time.time()
    print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
        gpu_end - gpu_start, cpu_end - cpu_start))

    mode = "COLLECT"
    (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
        lambda spark: spark.sql("SELECT * FROM {}".format(cpu_table)), mode)
    (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
        lambda spark: spark.sql("SELECT * FROM {}".format(gpu_table)), mode)

    from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
    from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    assert_equal(from_cpu, from_gpu)

def assert_gpu_fallback_write(write_func,
        read_func,
        base_path,
        cpu_fallback_class_name,
        conf={}):
    assert_gpu_fallback_write(write_func,
                              read_func,
                              base_path,
                              [cpu_fallback_class_name], # make a single item list
                              conf)

# Similar to
def assert_gpu_fallback_write(write_func,
                              read_func,
                              base_path,
                              cpu_fallback_class_name_list,
                              conf={}):
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_path = base_path + '/CPU'
    with_cpu_session(lambda spark : write_func(spark, cpu_path), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    jvm = spark_jvm()
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.startCapture()
    gpu_start = time.time()
    gpu_path = base_path + '/GPU'
    try:
        with_gpu_session(lambda spark : write_func(spark, gpu_path), conf=conf)
        gpu_end = time.time()
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertCapturedAndGpuFellBack(cpu_fallback_class_name_list, 10000)
        print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
            gpu_end - gpu_start, cpu_end - cpu_start))

        (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
                lambda spark: read_func(spark, cpu_path), 'COLLECT')
        (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
                lambda spark: read_func(spark, gpu_path), 'COLLECT')

        from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
        from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
        if should_sort_locally():
            _sort_locally(from_cpu, from_gpu)

        assert_equal(from_cpu, from_gpu)
    finally:
        # Ensure `shouldCapture` state is restored. This may happen when GpuPlan is failed to be executed,
        # then `shouldCapture` state is failed to restore in `assertCapturedAndGpuFellBack` method.
        # This mostly happen within a xfail case where error may be ignored.
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.endCapture()


def assert_gpu_fallback_write_sql(write_func,
                                  read_func,
                              base_table_name,
                              cpu_fallback_class_name_list,
                              conf={}):
    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_table_name = f'{base_table_name}_cpu'
    with_cpu_session(lambda spark : write_func(spark, cpu_table_name), conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    jvm = spark_jvm()
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.startCapture()
    gpu_start = time.time()
    gpu_table_name = f'{base_table_name}_gpu'
    try:
        with_gpu_session(lambda spark : write_func(spark, gpu_table_name), conf=conf)
        gpu_end = time.time()
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertCapturedAndGpuFellBack(cpu_fallback_class_name_list, 10000)
        print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
            gpu_end - gpu_start, cpu_end - cpu_start))

        (cpu_bring_back, cpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, cpu_table_name), 'COLLECT')
        (gpu_bring_back, gpu_collect_type) = _prep_func_for_compare(
            lambda spark: read_func(spark, gpu_table_name), 'COLLECT')

        from_cpu = with_cpu_session(cpu_bring_back, conf=conf)
        from_gpu = with_cpu_session(gpu_bring_back, conf=conf)
        if should_sort_locally():
            _sort_locally(from_cpu, from_gpu)

        assert_equal(from_cpu, from_gpu)
    finally:
        # Ensure `shouldCapture` state is restored. This may happen when GpuPlan is failed to be executed,
        # then `shouldCapture` state is failed to restore in `assertCapturedAndGpuFellBack` method.
        # This mostly happen within a xfail case where error may be ignored.
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.endCapture()


def assert_cpu_and_gpu_are_equal_collect_with_capture(func,
        exist_classes='',
        non_exist_classes='',
        conf={},
        require_non_empty=False):
    (bring_back, collect_type) = _prep_func_for_compare(func, 'COLLECT_WITH_DATAFRAME')

    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu, cpu_df = with_cpu_session(bring_back, conf=conf)
    cpu_end = time.time()
    if require_non_empty:
        assert len(from_cpu) > 0, "Expected non-empty result"
    print('### GPU RUN ###')
    gpu_start = time.time()
    from_gpu, gpu_df = with_gpu_session(bring_back, conf=conf)
    gpu_end = time.time()
    jvm = spark_jvm()
    if exist_classes:
        for clz in exist_classes.split(','):
            jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertContains(gpu_df._jdf, clz)
    if non_exist_classes:
        for clz in non_exist_classes.split(','):
            jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertNotContain(gpu_df._jdf, clz)
    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type,
        gpu_end - gpu_start, cpu_end - cpu_start))
    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    assert_equal(from_cpu, from_gpu)

def assert_cpu_and_gpu_are_equal_sql_with_capture(df_fun,
        sql,
        table_name,
        exist_classes='',
        non_exist_classes='',
        conf=None,
        debug=False):
    if conf is None:
        conf = {}
    def do_it_all(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView(table_name)
        if debug:
            return data_gen.debug_df(spark.sql(sql))
        else:
            return spark.sql(sql)
    assert_cpu_and_gpu_are_equal_collect_with_capture(do_it_all, exist_classes, non_exist_classes, conf)

def assert_gpu_fallback_collect(func,
        cpu_fallback_class_name,
        conf={}):
    (bring_back, collect_type) = _prep_func_for_compare(func, 'COLLECT_WITH_DATAFRAME')

    conf = _prep_incompat_conf(conf)

    print('### CPU RUN ###')
    cpu_start = time.time()
    from_cpu, cpu_df = with_cpu_session(bring_back, conf=conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    from_gpu, gpu_df = with_gpu_session(bring_back, conf=conf)
    gpu_end = time.time()
    jvm = spark_jvm()
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertDidFallBack(gpu_df._jdf, cpu_fallback_class_name)
    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type,
        gpu_end - gpu_start, cpu_end - cpu_start))
    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    assert_equal(from_cpu, from_gpu)

def assert_gpu_sql_fallback_collect(df_fun, cpu_fallback_class_name, table_name, sql, conf=None, debug=False):
    if conf is None:
        conf = {}
    def do_it_all(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView(table_name)
        if debug:
            return data_gen.debug_df(spark.sql(sql))
        else:
            return spark.sql(sql)
    assert_gpu_fallback_collect(do_it_all, cpu_fallback_class_name, conf)

def _assert_gpu_and_cpu_are_equal(func,
    mode,
    conf={},
    is_cpu_first=True,
    result_canonicalize_func_before_compare=None):
    (bring_back, collect_type) = _prep_func_for_compare(func, mode)
    conf = _prep_incompat_conf(conf)

    def run_on_cpu():
        print('### CPU RUN ###')
        global cpu_start
        cpu_start = time.time()
        from_cpu = with_cpu_session(bring_back, conf=conf)
        global cpu_end
        cpu_end = time.time()
        return from_cpu

    def run_on_gpu():
        print('### GPU RUN ###')
        global gpu_start
        gpu_start = time.time()
        from_gpu = with_gpu_session(bring_back, conf=conf)
        global gpu_end
        gpu_end = time.time()
        return from_gpu

    if is_cpu_first:
        from_cpu = run_on_cpu()
        from_gpu = run_on_gpu()
    else:
        from_gpu = run_on_gpu()
        from_cpu = run_on_cpu()

    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type,
        gpu_end - gpu_start, cpu_end - cpu_start))

    if result_canonicalize_func_before_compare is not None:
        (from_cpu, from_gpu) = result_canonicalize_func_before_compare(from_cpu, from_gpu)

    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    assert_equal(from_cpu, from_gpu)

def run_with_cpu(func,
    mode,
    conf={}):
    (bring_back, collect_type) = _prep_func_for_compare(func, mode)
    conf = _prep_incompat_conf(conf)

    print("run_with_cpu")

    def run_on_cpu():
        print('### CPU RUN ###')
        global cpu_start
        cpu_start = time.time()
        global from_cpu
        from_cpu = with_cpu_session(bring_back, conf=conf)
        global cpu_end
        cpu_end = time.time()

    run_on_cpu()

    print('### {}: CPU TOOK {} ###'.format(collect_type,
        cpu_end - cpu_start))
    if should_sort_locally():
        _sort_locally(from_cpu)

    return from_cpu

def run_with_cpu_and_gpu(func,
    mode,
    conf={}):
    (bring_back, collect_type) = _prep_func_for_compare(func, mode)
    conf = _prep_incompat_conf(conf)

    def run_on_cpu():
        print('### CPU RUN ###')
        global cpu_start
        cpu_start = time.time()
        global from_cpu
        from_cpu = with_cpu_session(bring_back, conf=conf)
        global cpu_end
        cpu_end = time.time()

    def run_on_gpu():
        print('### GPU RUN ###')
        global gpu_start
        gpu_start = time.time()
        global from_gpu
        from_gpu = with_gpu_session(bring_back, conf=conf)
        global gpu_end
        gpu_end = time.time()

    run_on_cpu()
    run_on_gpu()

    print('### {}: GPU TOOK {} CPU TOOK {} ###'.format(collect_type,
        gpu_end - gpu_start, cpu_end - cpu_start))
    if should_sort_locally():
        _sort_locally(from_cpu, from_gpu)

    return (from_cpu, from_gpu)

def assert_gpu_and_cpu_are_equal_collect(func, conf={}, is_cpu_first=True, result_canonicalize_func_before_compare=None):
    """
    Assert when running func on both the CPU and the GPU that the results are equal.
    In this case the data is collected back to the driver and compared here, so be
    careful about the amount of data returned.
    :param result_canonicalize_func_before_compare: specify a func to canonicalize the cpu and gpu results before compare.
                                                 It's needed when comparing fastparquet and GPU.
                                                 The result of fastparquet is different from the result of GPU, e.g.:
                                                    --- CPU OUTPUT
                                                    +++ GPU OUTPUT
                                                    @@ -1 +1 @@
                                                    -Row(a.first=-341142443, a.second=3.333994866005594e-37)
                                                    +Row(a=Row(first=-341142443, second=3.333994866005594e-37))
                                                  Use this func to canonicalize the results.
                                                  Usage of this func is: (cpu, gpu) = result_canonicalize_func_before_compare(original_cpu_result, original_gpu_result)
    """
    _assert_gpu_and_cpu_are_equal(func, 'COLLECT', conf=conf, is_cpu_first=is_cpu_first, result_canonicalize_func_before_compare=result_canonicalize_func_before_compare)

def assert_gpu_and_cpu_are_equal_iterator(func, conf={}, is_cpu_first=True):
    """
    Assert when running func on both the CPU and the GPU that the results are equal.
    In this case the data is pulled back to the driver in chunks and compared here
    so any amount of data can work, just be careful about how long it might take.
    """
    _assert_gpu_and_cpu_are_equal(func, 'ITERATOR', conf=conf, is_cpu_first=is_cpu_first)

def assert_gpu_and_cpu_row_counts_equal(func, conf={}, is_cpu_first=True):
    """
    Assert that the row counts from running the func are the same on both the CPU and GPU.
    This function runs count() to only get the number of rows and compares that count
    between the CPU and GPU. It does NOT compare any underlying data.
    """
    _assert_gpu_and_cpu_are_equal(func, 'COUNT', conf=conf, is_cpu_first=is_cpu_first)

def assert_gpu_and_cpu_are_equal_sql(df_fun, table_name, sql, conf=None, debug=False,
        is_cpu_first=True, validate_execs_in_gpu_plan=[],
        result_canonicalize_func_before_compare=None):
    """
    Assert that the specified SQL query produces equal results on CPU and GPU.
    :param df_fun: a function that will create the dataframe
    :param table_name: Name of table to be created with the dataframe
    :param sql: SQL query to be run on the specified table
    :param conf: Any user-specified confs. Empty by default.
    :param debug: Boolean to indicate if the SQL output should be printed
    :param is_cpu_first: Boolean to indicate if the CPU should be run first or not
    :param validate_execs_in_gpu_plan: String list of expressions to be validated in the GPU plan.
    :param result_canonicalize_func_before_compare: Function to canonicalize the CPU and GPU
        results before comparison.
    :return: Assertion failure, if results from CPU and GPU do not match.
    """
    if conf is None:
        conf = {}
    def do_it_all(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView(table_name)
        # we hold off on setting the validate execs until after creating the temp view

        spark.conf.set('spark.rapids.sql.test.validateExecsInGpuPlan', ','.join(validate_execs_in_gpu_plan))
        if debug:
            return data_gen.debug_df(spark.sql(sql))
        else:
            return spark.sql(sql)
    assert_gpu_and_cpu_are_equal_collect(do_it_all, conf, is_cpu_first=is_cpu_first,
        result_canonicalize_func_before_compare=result_canonicalize_func_before_compare)


def check_exception(actual_error, error_message):
    """
    Assert that a specific Java exception is thrown
    :param actual_error: the error that was caught when executing the query
    :param error_message: a string such as the one produce by java.lang.Exception.toString
    Assertion failure if:
        - no exception matching error_message has occurred
        - the error isn't a string or a regex
    """
    if isinstance(error_message, re.Pattern):
        assert error_message.search(actual_error), f"Expected error '{error_message}' to match '{actual_error}'"
    elif isinstance(error_message, str):
        assert error_message in actual_error, f"Expected error '{error_message}' did not appear in '{actual_error}'"
    else:
        assert False, f"expected str or Pattern found {type(error_message)} {error_message}"


def make_exception_info(ex):
    """
    Simulate the pytest.raises exconly call to get the same string we used to, and allowing
    us to optionally fail (pytest.raises always calls `fail` failing the test outright).

    We have comparison code in tests that rely on this output to include the classname, so we
    want to keep the text exactly the same as pytest.raises.

    We will either use the from_exc_info API, which exists since pytest 5.1, or choose
    the newer from_exception API (pytest 7.4) if it exists.
    """
    try:
        return pytest.ExceptionInfo.from_exception(ex)
    except AttributeError:
        exc_info = (type(ex), ex, ex.__traceback__)
        return pytest.ExceptionInfo.from_exc_info(exc_info)


def collect_data_or_exception(func, error_message, expect_exception=None):
    """
    Assert that a specific Java exception is thrown
    :param func: a function to be verified
    :param error_message: a string such as the one produce by java.lang.Exception.toString
    :param expect_exception:
            - if none, we check the error if and only if it was raised
            - if true, we will assert False if func() doesn't raise an exception,
            - if false, we fail if we raise an exception

    :return: (true, None) if the code failed with an exception and the text matches `error_message`.
             (false, result) if `expect_exception` is false and the function doesn't throw
             Assertion failure if:
             - no exception matching error_message has occurred, or
             - the code is expected to throw and doesn't, or
             - the code throws and is not expected to throw.
    """
    thrown_ex = None
    result = None
    try:
        result = func()
    except Exception as ex:
        thrown_ex = make_exception_info(ex).exconly()

    if thrown_ex is not None:
        if expect_exception is not False:
            check_exception(thrown_ex, error_message)
            return (True, None)
        else:
            assert False, f"no exception was expected, but the test failed with {thrown_ex}"

    elif expect_exception:
        # we expected an exception, but func() didn't throw
        assert False, f"expected exception with error message {error_message}, but the test did not throw."
    return (False, result)

def assert_spark_exception(df_fun, error_message):
    collect_data_or_exception(df_fun, error_message, expect_exception=True)

def assert_consistent_gpu_cpu_behavior(df_fun, conf, error_message, only_if_cpu_fails=False):
    """
    Assert that GPU and CPU execution behave consistently - either both fail with the same error
    or both succeed with identical results.
    
    :param df_fun: a function to be verified
    :param conf: Spark config
    :param error_message: a string such as the one produce by java.lang.Exception.toString
    :param only_if_cpu_fails: if true, we only expect the GPU job to fail if the CPU job fails,
           otherwise we expect both CPU and GPU to fail when error_message is provided.
    :return: Assertion failure if GPU and CPU behaviors are inconsistent (different errors or 
             different results when both succeed)
    """
    (cpu_failed, from_cpu) = collect_data_or_exception(
        lambda: with_cpu_session(df_fun, conf), error_message)
    (gpu_failed, from_gpu) = collect_data_or_exception(
        lambda: with_gpu_session(df_fun, conf), error_message, 
        expect_exception=cpu_failed or not only_if_cpu_fails)

    if cpu_failed is False and gpu_failed is False:
        assert_equal(from_cpu, from_gpu)

# Backward compatibility alias - use assert_consistent_gpu_cpu_behavior instead
def assert_gpu_and_cpu_error(df_fun, conf, error_message): 
    assert_consistent_gpu_cpu_behavior(df_fun, conf, error_message, only_if_cpu_fails=False)

def assert_gpu_and_cpu_same_data_or_error(df_fun, conf, error_message): 
    assert_consistent_gpu_cpu_behavior(df_fun, conf, error_message, only_if_cpu_fails=True)

def with_cpu_sql(df_fun, table_name, sql, conf=None, debug=False):
    if conf is None:
        conf = {}
    def do_it_all(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView(table_name)
        if debug:
            return data_gen.debug_df(spark.sql(sql))
        else:
            return spark.sql(sql)
    assert_gpu_and_cpu_are_equal_collect(do_it_all, conf, is_cpu_first=is_cpu_first)
