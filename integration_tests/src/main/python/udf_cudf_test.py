# Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

from conftest import is_at_least_precommit_run
from data_gen import copy_and_update

from pyspark.sql.pandas.utils import require_minimum_pyarrow_version, require_minimum_pandas_version

try:
    require_minimum_pandas_version()
except Exception as e:
    if is_at_least_precommit_run():
        raise AssertionError("incorrect pandas version during required testing " + str(e))
    pytestmark = pytest.mark.skip(reason=str(e))

try:
    require_minimum_pyarrow_version()
except Exception as e:
    if is_at_least_precommit_run():
        raise AssertionError("incorrect pyarrow version during required testing " + str(e))
    pytestmark = pytest.mark.skip(reason=str(e))

import pandas as pd
import time
from typing import Iterator
from pyspark.sql import Window
from pyspark.sql.functions import pandas_udf, PandasUDFType
from spark_session import is_databricks_runtime, is_spark_340_or_later, with_cpu_session, with_gpu_session
from marks import cudf_udf


_conf = {
        'spark.rapids.sql.exec.AggregateInPandasExec': 'true',
        'spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec': 'true',
        'spark.rapids.sql.exec.WindowInPandasExec': 'true',
        'spark.rapids.sql.python.gpu.enabled': 'true'
        }

small_data = [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)]

large_data = list(map(lambda i: (i, i/1.0), range(1, 512))) * 2


def _create_df(spark, data=large_data):
    return spark.createDataFrame(data, ("id", "v"))


# since this test requires to run different functions on CPU and GPU(need cudf),
# create its own assert function
def _assert_cpu_gpu(cpu_func, gpu_func, cpu_conf={}, gpu_conf={}, is_sort=False):
    print('### CPU RUN ###')
    cpu_start = time.time()
    cpu_ret = with_cpu_session(cpu_func, conf=cpu_conf)
    cpu_end = time.time()
    print('### GPU RUN ###')
    gpu_start = time.time()
    gpu_ret = with_gpu_session(gpu_func, conf=gpu_conf)
    gpu_end = time.time()
    print('### WRITE: GPU TOOK {} CPU TOOK {} ###'.format(
        gpu_end - gpu_start, cpu_end - cpu_start))
    if is_sort:
        assert cpu_ret.sort() == gpu_ret.sort()
    else:
        assert cpu_ret == gpu_ret


# ======= Test Scalar =======
@cudf_udf
@pytest.mark.parametrize('data', [small_data, large_data], ids=['small data', 'large data'])
def test_with_column(enable_cudf_udf, data):
    @pandas_udf('int')
    def _plus_one_cpu_func(v: pd.Series) -> pd.Series:
        return v + 1

    @pandas_udf('int')
    def _plus_one_gpu_func(v: pd.Series) -> pd.Series:
        import cudf
        gpu_series = cudf.Series(v)
        gpu_series = gpu_series + 1
        return gpu_series.to_pandas()
    def cpu_run(spark):
        df = _create_df(spark, data)
        return df.withColumn("v1", _plus_one_cpu_func(df.v)).collect()

    def gpu_run(spark):
        df = _create_df(spark, data)
        return df.withColumn("v1", _plus_one_gpu_func(df.v)).collect()

    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf)


@cudf_udf
def test_sql(enable_cudf_udf):
    @pandas_udf('int')
    def _plus_one_cpu_func(v: pd.Series) -> pd.Series:
        return v + 1

    @pandas_udf('int')
    def _plus_one_gpu_func(v: pd.Series) -> pd.Series:
        import cudf
        gpu_series = cudf.Series(v)
        gpu_series = gpu_series + 1
        return gpu_series.to_pandas()

    def cpu_run(spark):
        _ = spark.udf.register("add_one_cpu", _plus_one_cpu_func)
        _create_df(spark).createOrReplaceTempView("test_table_cpu")
        return spark.sql("SELECT add_one_cpu(id) FROM test_table_cpu").collect()

    def gpu_run(spark):
        _ = spark.udf.register("add_one_gpu", _plus_one_gpu_func)
        _create_df(spark).createOrReplaceTempView("test_table_gpu")
        return spark.sql("SELECT add_one_gpu(id) FROM test_table_gpu").collect()

    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf)


# ======= Test Scalar Iterator =======
@cudf_udf
def test_select(enable_cudf_udf):
    @pandas_udf("long")
    def _plus_one_cpu_iter_func(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for s in iterator:
            yield s + 1

    @pandas_udf("long")
    def _plus_one_gpu_iter_func(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
        import cudf
        for s in iterator:
            gpu_serises = cudf.Series(s)
            gpu_serises = gpu_serises + 1
            yield gpu_serises.to_pandas()

    def cpu_run(spark):
        df = _create_df(spark)
        return df.select(_plus_one_cpu_iter_func(df.v)).collect()

    def gpu_run(spark):
        df = _create_df(spark)
        return df.select(_plus_one_gpu_iter_func(df.v)).collect()
    
    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf)


# ======= Test Flat Map In Pandas =======
@cudf_udf
def test_map_in_pandas(enable_cudf_udf):
    def cpu_run(spark):
        def _filter_cpu_func(iterator):
            for pdf in iterator:
                yield pdf[pdf.id == 1]
        df = _create_df(spark)
        return df.mapInPandas(_filter_cpu_func, df.schema).collect()

    def gpu_run(spark):
        def _filter_gpu_func(iterator):
            import cudf
            for pdf in iterator:
                gdf = cudf.from_pandas(pdf)
                yield gdf[gdf.id == 1].to_pandas()
        df = _create_df(spark)
        return df.mapInPandas(_filter_gpu_func, df.schema).collect()
    
    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf)


# ======= Test Grouped Map In Pandas =======
# To solve: Invalid udf: the udf argument must be a pandas_udf of type GROUPED_MAP
# need to add udf type
@cudf_udf
def test_group_apply(enable_cudf_udf):
    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def _normalize_cpu_func(df):
        v = df.v
        return df.assign(v=(v - v.mean()) / v.std())

    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def _normalize_gpu_func(df):
        import cudf
        gdf = cudf.from_pandas(df)
        v = gdf.v
        return gdf.assign(v=(v - v.mean()) / v.std()).to_pandas()

    def cpu_run(spark):
        df = _create_df(spark)
        return df.groupby("id").apply(_normalize_cpu_func).collect()

    def gpu_run(spark):
        df = _create_df(spark)
        return df.groupby("id").apply(_normalize_gpu_func).collect()

    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf, is_sort=True)


@cudf_udf
def test_group_apply_in_pandas(enable_cudf_udf):
    def cpu_run(spark):
        def _normalize_cpu_in_pandas_func(df):
            v = df.v
            return df.assign(v=(v - v.mean()) / v.std())
        df = _create_df(spark)
        return df.groupby("id").applyInPandas(_normalize_cpu_in_pandas_func, df.schema).collect()

    def gpu_run(spark):
        def _normalize_gpu_in_pandas_func(df):
            import cudf
            gdf = cudf.from_pandas(df)
            v = gdf.v
            return gdf.assign(v=(v - v.mean()) / v.std()).to_pandas()
        df = _create_df(spark)
        return df.groupby("id").applyInPandas(_normalize_gpu_in_pandas_func, df.schema).collect()
    
    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf, is_sort=True)


# ======= Test Aggregate In Pandas =======
@cudf_udf
def test_group_agg(enable_cudf_udf):
    @pandas_udf("int")
    def _sum_cpu_func(v: pd.Series) -> int:
        return v.sum()

    @pandas_udf("integer")
    def _sum_gpu_func(v: pd.Series) -> int:
        import cudf
        gpu_series = cudf.Series(v)
        return gpu_series.sum()

    def cpu_run(spark):
        df = _create_df(spark)
        return df.groupby("id").agg(_sum_cpu_func(df.v)).collect()

    def gpu_run(spark):
        df = _create_df(spark)
        return df.groupby("id").agg(_sum_gpu_func(df.v)).collect()

    conf = copy_and_update(_conf, {
        # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
        'spark.sql.adaptive.enabled': 'false'
    })
    
    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=conf, is_sort=True)


@cudf_udf
def test_sql_group(enable_cudf_udf):
    @pandas_udf("int")
    def _sum_cpu_func(v: pd.Series) -> int:
        return v.sum()

    @pandas_udf("integer")
    def _sum_gpu_func(v: pd.Series) -> int:
        import cudf
        gpu_series = cudf.Series(v)
        return gpu_series.sum()

    def cpu_run(spark):
        _ = spark.udf.register("sum_cpu_udf", _sum_cpu_func)
        q = "SELECT sum_cpu_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
        return spark.sql(q).collect()

    def gpu_run(spark):
        _ = spark.udf.register("sum_gpu_udf", _sum_gpu_func)
        q = "SELECT sum_gpu_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
        return spark.sql(q).collect()

    conf = copy_and_update(_conf, {
        # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
        'spark.sql.adaptive.enabled': 'false'
    })

    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=conf, is_sort=True)


# ======= Test Window In Pandas =======
@cudf_udf
def test_window(enable_cudf_udf):
    @pandas_udf("int")
    def _sum_cpu_func(v: pd.Series) -> int:
        return v.sum()

    @pandas_udf("integer")
    def _sum_gpu_func(v: pd.Series) -> int:
        import cudf
        gpu_series = cudf.Series(v)
        return gpu_series.sum()

    def cpu_run(spark):
        df = _create_df(spark)
        w = Window.partitionBy('id').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        return df.withColumn('sum_v', _sum_cpu_func('v').over(w)).collect()

    def gpu_run(spark):
        df = _create_df(spark)
        w = Window.partitionBy('id').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        return df.withColumn('sum_v', _sum_gpu_func('v').over(w)).collect()

    conf = copy_and_update(_conf, {
        # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
        'spark.sql.adaptive.enabled': 'false'
    })

    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=conf, is_sort=True)


# ======= Test CoGroup Map In Pandas =======
@cudf_udf
def test_cogroup(enable_cudf_udf):
    def cpu_run(spark):
        def _cpu_join_func(l, r):
            return pd.merge(l, r, on="time")
        df1 = spark.createDataFrame(
                [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
                ("time", "id", "v1"))
        df2 = spark.createDataFrame(
                [(20000101, 1, "x"), (20000101, 2, "y")],
                ("time", "id", "v2"))
        return df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(_cpu_join_func,
            schema="time int, id_x int, id_y int, v1 double, v2 string").collect()

    def gpu_run(spark):
        def _gpu_join_func(l, r):
            import cudf
            gl = cudf.from_pandas(l)
            gr = cudf.from_pandas(r)
            return gl.merge(gr, on="time").to_pandas()
        df1 = spark.createDataFrame(
                [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
                ("time", "id", "v1"))
        df2 = spark.createDataFrame(
                [(20000101, 1, "x"), (20000101, 2, "y")],
                ("time", "id", "v2"))
        return df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(_gpu_join_func,
            schema="time int, id_x int, id_y int, v1 double, v2 string").collect()

    _assert_cpu_gpu(cpu_run, gpu_run, gpu_conf=_conf, is_sort=True)


