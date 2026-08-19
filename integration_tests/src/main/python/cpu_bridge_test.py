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

from pyspark.sql.functions import col
import pyspark.sql.functions as f
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_sql_with_capture
from conftest import is_dataproc_runtime, is_dataproc_serverless_runtime
from data_gen import *
from marks import allow_non_gpu, ignore_order
from spark_session import is_databricks_runtime, is_spark_341_or_later, with_cpu_session


# Helper function to create config that forces specific expressions to CPU bridge
def create_cpu_bridge_fallback_conf(disabled_gpu_expressions, disallowed_bridge_expressions=[]):
    """Create config that enables CPU bridge and disables specific GPU expressions"""
    conf = {
        'spark.rapids.sql.expression.cpuBridge.enabled': True,
        'spark.sql.ansi.enabled': False  # Disable ANSI mode to prevent arithmetic overflow exceptions
    }
    # Disable specific GPU expressions to force CPU bridge fallback
    for expr_name in disabled_gpu_expressions:
        conf[f'spark.rapids.sql.expression.{expr_name}'] = False

    if disallowed_bridge_expressions:
        conf['spark.rapids.sql.expression.cpuBridge.disallowList'] = ','.join(disallowed_bridge_expressions)
    return conf

# gen_df's leaf is a row-based RDDScanExec. Under AQE, fixUpExchangeOverhead keeps a shuffle on
# the CPU when its only child is non-columnar, because it cannot see the GPU consumer in the next
# query stage. Real workloads read columnar files, so the shuffle has a columnar child and stays
# on the GPU; this trivial always-true GPU filter (monotonically_increasing_id() >= 0, which is
# nondeterministic so it is not constant-folded) gives the shuffle a columnar child to match.
def with_columnar_source(df):
    return df.filter(f.monotonically_increasing_id() >= f.lit(0))

# Allow only Add to fall back so this test isolates the CPU bridge path.
@allow_non_gpu('Add')
def test_cpu_bridge_add_fallback():
    """Test CPU bridge when Add expression is forced to fall back to CPU"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen)], length=1024)
        # This Add will be forced to use CPU bridge due to config
        return df.selectExpr("a", "b", "a + a as s1", "b * b as p1")
    
    # Force Add to fall back to CPU bridge
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


# MIN_ROWS_PER_SUBBATCH is 500,000: these values exercise one and two sub-batch results.
@allow_non_gpu('Add')
@pytest.mark.parametrize('row_count', [500000, 500001], ids=idfn)
def test_cpu_bridge_add_large_batch_parallel_path(row_count):
    def test_func(spark):
        return spark.range(0, row_count, 1, 1) \
            .selectExpr("id + 1 as bridged") \
            .agg(f.sum("bridged"))

    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        test_func, exist_classes="GpuCpuBridgeExpression", conf=conf)


@allow_non_gpu('CreateMap', 'Add')
def test_cpu_bridge_stateful_map_large_batch_parallel_path():
    def test_func(spark):
        return spark.range(0, 500001, 1, 1) \
            .selectExpr("id", "map(id, id + 1) as bridged") \
            .selectExpr("element_at(bridged, id) as value") \
            .agg(f.sum("value"))

    conf = create_cpu_bridge_fallback_conf(['CreateMap'])
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        test_func, exist_classes="GpuCpuBridgeExpression", conf=conf)

# Allow only Multiply to fall back so this test isolates the CPU bridge path.
@allow_non_gpu('Multiply')
def test_cpu_bridge_multiply_fallback():
    """Test CPU bridge when Multiply expression is forced to fall back to CPU"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', double_gen)], length=1024)
        # This Multiply will be forced to use CPU bridge due to config
        return df.selectExpr("a", "b", "a + a as s1", "b * b as p1")
    
    # Force Multiply to fall back to CPU bridge
    conf = create_cpu_bridge_fallback_conf(['Multiply'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


# Csc has no GPU implementation and is a deterministic UnaryMathExpression, so unlike the
# Add/Multiply tests above it covers the RuleNotFoundExprMeta -> CPU bridge path. Keep the bridge
# explicitly enabled so this remains a targeted bridge test even if the default changes again.
@allow_non_gpu('Csc')
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_cpu_bridge_csc_select(aqe_enabled):
    conf = {
        'spark.rapids.sql.expression.cpuBridge.enabled': True,
        'spark.sql.adaptive.enabled': aqe_enabled,
    }
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark,
            [('x', DoubleGen(min_exp=-3, max_exp=5, no_nans=True))],
            length=100
        ).selectExpr("x", "csc(x) AS csc_x"),
        exist_classes='GpuCpuBridgeExpression',
        conf=conf)


@allow_non_gpu('Csc')
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_cpu_bridge_csc_filter(aqe_enabled):
    conf = {
        'spark.rapids.sql.expression.cpuBridge.enabled': True,
        'spark.sql.adaptive.enabled': aqe_enabled,
    }
    # Keep Csc only in the predicate so a Project bridge cannot mask a Filter bridge regression.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark,
            [('x', DoubleGen(min_exp=-1, max_exp=2, no_nans=True))],
            length=200
        ).filter("csc(x) > 1.5").select("x"),
        exist_classes='GpuCpuBridgeExpression',
        conf=conf)


@allow_non_gpu('ProjectExec')
@pytest.mark.skipif(not is_spark_341_or_later(),
                    reason='TimestampNTZType is only supported in PySpark 3.4.1+')
def test_cpu_bridge_timestamp_ntz_output_falls_back():
    """ProjectExec should fall back instead of bridging TimestampNTZ output."""
    def test_func(spark):
        seconds = "lpad(cast(id % 60 as string), 2, '0')"
        return spark.range(1024).selectExpr(
            f"cast(concat('2021-01-01 00:00:', {seconds}) as timestamp_ntz) as ts_ntz")

    conf = create_cpu_bridge_fallback_conf(['Cast'])
    assert_gpu_fallback_collect(test_func, 'ProjectExec', conf=conf)


@allow_non_gpu('ProjectExec')
@pytest.mark.skipif(not is_spark_341_or_later(),
                    reason='TimestampNTZType is only supported in PySpark 3.4.1+')
def test_cpu_bridge_timestamp_ntz_input_falls_back():
    """ProjectExec should fall back instead of bridging TimestampNTZ input."""
    def test_func(spark):
        from datetime import datetime
        from pyspark.sql.types import StructField, StructType, TimestampNTZType

        schema = StructType([StructField('ts_ntz', TimestampNTZType(), True)])
        df = spark.createDataFrame([
            (datetime(2021, 1, 1, 0, 0, 0),),
            (datetime(2021, 1, 1, 0, 0, 1, 123456),),
            (None,)
        ], schema)
        return df.selectExpr("cast(ts_ntz as string) as ts_string")

    conf = create_cpu_bridge_fallback_conf(['Cast'])
    assert_gpu_fallback_collect(test_func, 'ProjectExec', conf=conf)

@allow_non_gpu('Add', 'Multiply', 'CaseWhen', 'GreaterThan')
def test_cpu_bridge_complex_expression_tree():
    """Test CPU bridge with complex expression trees containing multiple fallbacks"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', int_gen)], length=1000)
        return df.selectExpr(
            "a", "b", "c",
            # Complex expression mixing CPU bridge (Add) and GPU (Multiply) operations
            "a + (b * c) as mixed",
            "a + b + 5 as add_1",
            "a + c + 2 as add_2",
            "a > 0 as pred",
            "case when a > 0 then a + b + 5 else a + c + 2 end as conditional"
        )
    
    # Force Add to CPU bridge, keep other expressions on GPU
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

# Currently we cannot cross the boundary of a Lambda function. That may change in the future.
# but for now we only include what is required to work
@allow_non_gpu('Add')
def test_cpu_bridge_higher_order_function_fallback():
    """Test CPU bridge with higher order functions where inner expressions fall back to CPU"""
    def test_func(spark):
        df = gen_df(spark, [('arr', ArrayGen(int_gen, min_length=3, max_length=5))], length=1000)
        return df.selectExpr(
            "arr",
            # transform where the lambda contains Add (forced to CPU bridge)
            "transform(arr, x -> x + 1) as arr_plus_one",
            # filter where the lambda contains Add (forced to CPU bridge) 
            "filter(arr, x -> x + 2 > 5) as filtered_arr",
            # exists where the lambda contains Add (forced to CPU bridge)
            "exists(arr, x -> (x + 3) > 10) as has_large_element",
            "transform(arr, x -> (x + 2) * 3) as transformed"
        )
    
    # Force Add to CPU bridge - this will affect expressions inside the lambda functions
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
def test_cpu_bridge_nondeterministic_works_next_to_bridge():
    """Test mixed scenario: some expressions use CPU bridge, others stay on GPU"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen)], length=1000)
        # Add should use CPU bridge, rand(42) should stay on GPU - this should work fine
        return df.selectExpr("a", "b", "a + b as sum", "rand(42) * 100 as scaled_random")
    
    # Force Add to CPU bridge, rand() stays on GPU - should work with mixed execution
    conf = create_cpu_bridge_fallback_conf(['Add'])
    
    # This should succeed with mixed GPU/CPU bridge execution, not fall back entirely
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

# This is borrowed partly from join_test.py
bloom_filter_confs = {
    "spark.sql.autoBroadcastJoinThreshold": 1,
    "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold": 1,
    "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold": "100GB",
    "spark.sql.optimizer.runtime.bloomFilter.enabled": True
}

# Dataproc uses Cast instead of XxHash64 for runtime bloom filter probe keys.
dataproc_bloom_filter_probe_allow = (
    ("Cast",)
    if is_dataproc_runtime() or is_dataproc_serverless_runtime()
    else ()
)

def check_bloom_filter_join(confs, is_multi_column):
    def do_join(spark):
        if is_multi_column:
            left = spark.range(100000).withColumn("second_id", col("id") % 5)
            right = spark.range(10).withColumn("id2", col("id").cast("string")).withColumn("second_id", col("id") % 5)
            return right.filter("cast(id2 as bigint) % 3 = 0").join(left, (left.id == right.id) & (left.second_id == right.second_id), "inner")
        else:
            left = spark.range(100000)
            right = spark.range(10).withColumn("id2", col("id").cast("string"))
            return right.filter("cast(id2 as bigint) % 3 = 0").join(left, left.id == right.id, "inner")
    bridge_conf = create_cpu_bridge_fallback_conf([])
    partial_conf = copy_and_update(bridge_conf, confs)
    all_confs = copy_and_update(bloom_filter_confs, partial_conf)
    assert_gpu_and_cpu_are_equal_collect(do_join, conf=all_confs)

@allow_non_gpu(
    "ShuffleExchangeExec", "BloomFilterMightContain", "ScalarSubquery",
    "XxHash64", "GetStructField", "Remainder", "And",
    *dataproc_bloom_filter_probe_allow)
@ignore_order(local=True)
@pytest.mark.parametrize("is_multi_column", [False, True], ids=["SINGLE_COLUMN", "MULTI_COLUMN"])
@pytest.mark.skipif(is_databricks_runtime(), reason="https://github.com/NVIDIA/spark-rapids/issues/8921")
def test_bloom_filter_join_cpu_probe(is_multi_column):
    conf = {"spark.rapids.sql.expression.BloomFilterMightContain": "false"}
    check_bloom_filter_join(confs=conf, is_multi_column=is_multi_column)

# ==============================================================================
# NEGATIVE TEST CASES - Verify expressions that should NOT use CPU bridge
# These should cause full CPU fallback instead of using the bridge
# ==============================================================================

@allow_non_gpu('ProjectExec')
def test_cpu_bridge_rand_disabled_fallback():
    """Test that when rand() is disabled via config, ProjectExec falls back to CPU entirely"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen)], length=1000)
        # rand(42) with seed for deterministic results - disabled via config should cause full CPU fallback
        return df.selectExpr("a", "b", "a + b as sum", "rand(42) as random_val")
    
    # Enable CPU bridge but disable rand() - should cause full ProjectExec fallback
    conf = create_cpu_bridge_fallback_conf(['Rand'])
    
    # Verify that ProjectExec falls back to CPU (doesn't use GPU or bridge)
    assert_gpu_fallback_collect(test_func, 'ProjectExec', conf=conf)


@allow_non_gpu('HashAggregateExec', 'ShuffleExchangeExec')
@ignore_order(local=True)
def test_cpu_bridge_aggregation_sum_disabled_fallback():
    """Test that when sum() is disabled via config, HashAggregateExec falls back to CPU entirely"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', string_gen)], length=2000)
        # sum() disabled via config should cause full HashAggregateExec fallback, not bridge
        return df.groupBy('b').agg(f.sum('a').alias('total'))
    
    # Enable CPU bridge but disable sum() - should cause full HashAggregateExec fallback
    conf = create_cpu_bridge_fallback_conf(['Sum'])
    
    # Verify that HashAggregateExec falls back to CPU (doesn't use GPU or bridge)
    assert_gpu_fallback_collect(test_func, 'HashAggregateExec', conf=conf)


@allow_non_gpu('WindowExec')
def test_cpu_bridge_window_lag_disabled_fallback():
    """Test that when lag() is disabled via config, WindowExec falls back to CPU entirely"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', string_gen)], length=1000)
        # Give the window's partition-by shuffle a columnar child so it stays on GPU under AQE.
        df = with_columnar_source(df)
        # lag() disabled via config should cause full WindowExec fallback, not bridge
        return df.selectExpr(
            "a", "b",
            "lag(a, 1) over (partition by b order by a) as prev_a",
            "row_number() over (partition by b order by a) as row_num"
        )
    
    # Enable CPU bridge but disable lag() - should cause full WindowExec fallback
    conf = create_cpu_bridge_fallback_conf(['Lag'])
    
    # Verify that WindowExec falls back to CPU (doesn't use GPU or bridge)
    assert_gpu_fallback_collect(test_func, 'WindowExec', conf=conf)

@allow_non_gpu('ProjectExec')
def test_disallowed_bridge_fallback():
    """Test that when an expression is not on the GPU and is disallowed to use
    the cpu_bridge that it is honored"""
    conf = create_cpu_bridge_fallback_conf(['Add'],
            disallowed_bridge_expressions=['org.apache.spark.sql.catalyst.expressions.Add'])
    assert_gpu_fallback_collect(lambda spark: binary_op_df(spark, byte_gen).selectExpr("a + b"),
                                'ProjectExec', conf=conf)

@allow_non_gpu("GenerateExec", "ShuffleExchangeExec")
@ignore_order(local=True)
def test_generate_outer_fallback():
    conf = create_cpu_bridge_fallback_conf([])
    assert_gpu_fallback_collect(
        lambda spark: spark.sql("SELECT array(struct(1, 'a'), struct(2, 'b')) as x")\
            .repartition(1).selectExpr("inline_outer(x)"),
        "GenerateExec", conf = conf)

# ==============================================================================
# Join tests in join condition
# ==============================================================================

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_inner_join_post_filter_works():
    """Inner join with bridge expressions in condition should work with post-filtering"""
    def test_func(spark):
        # Use small range with overlap to ensure matches
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=10)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=50)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=10)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=20)
        
        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # True conditional join: equality condition + non-equality condition with bridge expression
        # This requires complex AST analysis that cannot use bridge expressions
        return spark.sql("""
            SELECT /*+ BROADCAST(right_table) */ 
                   left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table 
            JOIN right_table ON left_table.id = right_table.id 
                              AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    # Should work: Inner joins support post-filtering with disabled expressions  
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_outer_join_condition_works():
    """Left outer join with a non-AST-able (bridged) condition runs on GPU: the bridged
    sub-expressions are extracted into pre-join projections and the remaining comparison is
    evaluated as the AST join condition (AstUtil.extractNonAstFromJoinCond)."""
    def test_func(spark):
        # Use small range with overlap to ensure matches
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=10)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=50)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=10)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=20)
        
        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Left outer join with disabled (bridged) Add expressions in the condition. The
        # bridged sub-expressions are pulled into pre-join projections so the join itself
        # stays on the GPU.
        return spark.sql("""
            SELECT /*+ BROADCAST(right_table) */ 
                   left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table 
            LEFT JOIN right_table ON left_table.id = right_table.id 
                                   AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_simple_equality_join_works():
    """Simple equality joins with bridge expressions in select should work"""
    def test_func(spark):
        # Use small range with overlap to ensure matches
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=5)), 
                             ('a', IntegerGen(min_val=1, max_val=20))], length=30)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=5)), 
                              ('b', IntegerGen(min_val=1, max_val=20))], length=15)
        
        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Simple equality join with bridge expressions in SELECT, not join condition
        return spark.sql("""
            SELECT /*+ BROADCAST(right_table) */ 
                   left_table.id, 
                   left_table.a + 10 as left_sum,
                   right_table.b + 20 as right_sum
            FROM left_table 
            JOIN right_table ON left_table.id = right_table.id
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    # Should work: bridge expressions in select, simple equality in join
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_sort_merge_left_outer_join_works():
    """Left outer join with bridge expressions in condition runs on GPU via the CPU bridge"""
    def test_func(spark):
        # Use larger range to force sort-merge join instead of broadcast
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=1000)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=500)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=1000)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=200)
        # Give each join input's shuffle a columnar child so they stay on GPU under AQE.
        left = with_columnar_source(left)
        right = with_columnar_source(right)

        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Left outer join: a+10/b+5 are not AST-able (Add disabled), so they run via the
        # CPU bridge in pre-join projections instead of forcing a CPU fallback.
        return spark.sql("""
            SELECT left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table
            LEFT JOIN right_table ON left_table.id = right_table.id
                                   AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    # Outer joins now run on GPU: the non-AST a+10/b+5 are evaluated via the CPU bridge.
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_sort_merge_right_outer_join_works():
    """Right outer join with bridge expressions in condition runs on GPU via the CPU bridge"""
    def test_func(spark):
        # Use larger range to force sort-merge join instead of broadcast
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=1000)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=200)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=1000)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=500)
        # Give each join input's shuffle a columnar child so they stay on GPU under AQE.
        left = with_columnar_source(left)
        right = with_columnar_source(right)

        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Right outer join: a+10/b+5 are not AST-able (Add disabled), so they run via the
        # CPU bridge in pre-join projections instead of forcing a CPU fallback.
        return spark.sql("""
            SELECT left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table
            RIGHT JOIN right_table ON left_table.id = right_table.id
                                    AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    # Outer joins now run on GPU: the non-AST a+10/b+5 are evaluated via the CPU bridge.
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_sort_merge_full_outer_join_works():
    """Full outer join with bridge expressions in condition runs on GPU via the CPU bridge"""
    def test_func(spark):
        # Use larger range to force sort-merge join instead of broadcast
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=1000)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=300)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=1000)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=300)
        # Give each join input's shuffle a columnar child so they stay on GPU under AQE.
        left = with_columnar_source(left)
        right = with_columnar_source(right)

        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Full outer join: a+10/b+5 are not AST-able (Add disabled), so they run via the
        # CPU bridge in pre-join projections instead of forcing a CPU fallback.
        return spark.sql("""
            SELECT left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table
            FULL OUTER JOIN right_table ON left_table.id = right_table.id
                                         AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    # Outer joins now run on GPU: the non-AST a+10/b+5 are evaluated via the CPU bridge.
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_inner_join_with_bridge_expressions_works():
    """Inner join with bridge expressions in condition should work on GPU (no fallback needed)"""
    def test_func(spark):
        # Use smaller range to allow broadcast join
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=10)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=50)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=10)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=20)
        
        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Inner join with disabled Add expressions in condition
        # Should work on GPU: Inner joins can use post-filtering for non-AST conditions
        return spark.sql("""
            SELECT /*+ BROADCAST(right_table) */
                   left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table
            INNER JOIN right_table ON left_table.id = right_table.id
                                    AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    # Should work on GPU: Inner joins don't require AST for conditions
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_hash_join_left_outer_works():
    """Non-broadcast left outer join with a non-AST-able (bridged) condition runs on GPU: the
    bridged sub-expressions are extracted into pre-join projections and the remaining comparison
    is evaluated as the AST join condition."""
    def test_func(spark):
        # Use medium range - not small enough for broadcast
        left = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=100)), 
                             ('a', IntegerGen(min_val=1, max_val=100))], length=200)
        right = gen_df(spark, [('id', IntegerGen(min_val=1, max_val=100)), 
                              ('b', IntegerGen(min_val=1, max_val=100))], length=150)
        
        left.createOrReplaceTempView("left_table")
        right.createOrReplaceTempView("right_table")
        
        # Disable adaptive query execution for predictable behavior
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        
        # Left outer join with disabled Add expressions in condition
        return spark.sql("""
            SELECT left_table.id, left_table.a, right_table.id as right_id, right_table.b
            FROM left_table
            LEFT JOIN right_table ON left_table.id = right_table.id
                                   AND left_table.a + 10 > right_table.b + 5
        """)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_broadcast_nested_loop_join_condition_works():
    """BNLJ extracts bridged non-AST sub-expressions before AST condition tagging."""
    def test_func(spark):
        left = gen_df(spark, [('a', IntegerGen(min_val=1, max_val=100))], length=50)
        right = gen_df(spark, [('b', IntegerGen(min_val=1, max_val=100))], length=20)

        return left.join(f.broadcast(right), (left.a + 10) > (right.b + 5), 'Left')

    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        test_func, exist_classes='GpuBroadcastNestedLoopJoin', conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_hash_partitioning_works():
    """Bridge expressions in partitioning should work with partition bridge optimization"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', string_gen)], length=2000)
        # Give the repartition's shuffle a columnar child so it stays on GPU under AQE.
        df = with_columnar_source(df)
        # Partition by expression containing bridge - with partition bridge optimization,
        # hash partitioning should work entirely on GPU
        return df.repartition(10, df.a + df.b).groupBy("c").count()
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_sort_key():
    """Bridge expressions in sort keys should work entirely on GPU with partition bridge optimization"""
    def test_func(spark):
        # Use non-overlapping ranges to avoid ambiguous sort results
        # a: 1-50, b: 100-150, so a+b ranges from 101-200 (no overlaps possible)
        # No nulls to avoid null + anything = null ambiguity
        df = gen_df(spark, [
            ('a', IntegerGen(min_val=1, max_val=50, nullable=False)), 
            ('b', IntegerGen(min_val=100, max_val=150, nullable=False))
        ], length=1000)
        # Give the global-sort shuffle a columnar child so it stays on GPU under AQE.
        df = with_columnar_source(df)
        # Sort by bridge expression - with partition bridge optimization,
        # both partitioning and sort should work on GPU
        # Secondary sort by 'a' eliminates any remaining ambiguity
        return df.orderBy(df.a + df.b, df.a)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
def test_cpu_bridge_sort_with_limit():
    """Bridge expressions in sort keys with LIMIT should work entirely on GPU"""
    def test_func(spark):
        # Use non-overlapping ranges to avoid ambiguous sort results
        # a: 1-100, b: 1000-1100, so a+b ranges from 1001-1200 (no overlaps)
        # No nulls to avoid null + anything = null ambiguity
        df = gen_df(spark, [
            ('a', IntegerGen(min_val=1, max_val=100, nullable=False)), 
            ('b', IntegerGen(min_val=1000, max_val=1100, nullable=False))
        ], length=500)
        
        # Sort by bridge expression with limit - uses top-K algorithm, no shuffle needed
        # Secondary sort by 'a' eliminates any remaining ambiguity
        return df.orderBy(df.a + df.b, df.a).limit(50)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@allow_non_gpu('Add')
@ignore_order(local=True)
def test_cpu_bridge_group_by_key_works():
    """Bridge expressions as grouping keys should work via project rewrite"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', int_gen)], length=2000)
        # Group by bridge expression should work - Spark rewrites to project first
        return df.groupBy(df.a + df.b).agg(f.sum("c").alias("total"))
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

@allow_non_gpu('Add')
def test_cpu_bridge_window_partition_key_works():
    """Bridge expressions in window partition keys should work via project rewrite"""
    def test_func(spark):
        # Use unique values for ordering to ensure deterministic ROW_NUMBER() results
        df = gen_df(spark, [
            ('a', IntegerGen(min_val=1, max_val=50, nullable=False)), 
            ('b', IntegerGen(min_val=100, max_val=150, nullable=False)), 
            ('c', UniqueLongGen(nullable=False))
        ], length=1000)
        # Window partition by bridge expression should work - Spark rewrites to project first
        return df.selectExpr(
            "a", "b", "c",
            "row_number() over (partition by a + b order by c) as row_num",
            "sum(c) over (partition by a + b order by c) as running_sum"
        )
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@allow_non_gpu('Add')
def test_cpu_bridge_window_order_key_works():
    """Bridge expressions in window order keys should work via project rewrite"""
    def test_func(spark):
        # Use non-overlapping ranges and unique ordering to ensure deterministic results
        df = gen_df(spark, [
            ('a', IntegerGen(min_val=1, max_val=50, nullable=False)), 
            ('b', IntegerGen(min_val=1000, max_val=1050, nullable=False)), 
            ('c', StringGen(pattern='[a-d]', nullable=False)),  # Small set for partitioning
            ('order_key', UniqueLongGen(nullable=False))  # Unique ordering column
        ], length=1000)
        # Window order by bridge expression should work - Spark rewrites to project first
        # Use unique order_key as secondary sort to ensure deterministic ROW_NUMBER() and LAG()
        return df.selectExpr(
            "a", "b", "c",
            "row_number() over (partition by c order by a + b, order_key) as row_num",
            "lag(a, 1) over (partition by c order by a + b, order_key) as prev_a"
        )
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


# ==============================================================================
# PREDICATE PUSHDOWN TESTS - Bridge expressions in filter predicates
# These test scenarios where bridge expressions in filters affect scan operations
# ==============================================================================

# Note that this is only for the Filter after the Scan. The Scan itself does not
# use the bridge expressions.
@allow_non_gpu('Add')
def test_cpu_bridge_predicate_pushdown_parquet_works(spark_tmp_path):
    """Check we didn't break predicate push down when bridge expressions are enabled"""
    # Generate data once and write to parquet
    data_path = spark_tmp_path + '/BRIDGE_PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', string_gen)], length=2000)
                     .coalesce(1).write.mode("overwrite").parquet(data_path))
    
    def test_func(spark):
        # Read back with filter containing bridge expression
        # Predicate pushdown should work because those expressions are
        # treated differently.
        return spark.read.parquet(data_path).filter((col("a") + col("b")) > 100)
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@allow_non_gpu('Add', 'And', 'IsNotNull', 'GreaterThan', 'LessThan', 
  'CaseWhen', 'GreaterThan')
def test_cpu_bridge_complex_predicate_pushdown_works(spark_tmp_path):
    """Check we didn't break predicate push down when bridge expressions are enabled"""
    # Generate data once and write to parquet
    data_path = spark_tmp_path + '/BRIDGE_COMPLEX_PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', int_gen), ('d', string_gen)], length=1500)
                     .coalesce(1).write.mode("overwrite").parquet(data_path))
    
    def test_func(spark):
        return spark.read.parquet(data_path).filter(
            ((col("a") + col("b")) > 50) & 
            ((col("b") + col("c")) < 200) &
            (col("d").isNotNull())
        )
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


# Test that simple filters without bridge expressions still work on GPU
def test_cpu_bridge_simple_predicate_pushdown_works(spark_tmp_path):
    """Simple predicates without bridge expressions should still use GPU scan"""
    # Generate data once and write to parquet
    data_path = spark_tmp_path + '/BRIDGE_SIMPLE_PARQUET_DATA'
    with_cpu_session(
        lambda spark: gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', string_gen)], length=1000)
                     .coalesce(1).write.mode("overwrite").parquet(data_path))
    
    def test_func(spark):
        # Simple filter that can be pushed down - should work on GPU
        return spark.read.parquet(data_path).filter((col("a") > 50) & (col("b") < 100))
    
    conf = create_cpu_bridge_fallback_conf(['Add'])  # Add disabled but not used in filter
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


# Test mixed scenario: bridge expression in select but simple predicate
@allow_non_gpu('Add')
def test_cpu_bridge_mixed_select_and_predicate():
    """Bridge in select with simple predicate should work correctly"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', int_gen)], length=1000)
        # Bridge expression in select, simple predicate - should work
        return df.filter(col("c") > 50).selectExpr("a", "b", "c", "a + b as sum")
    
    conf = create_cpu_bridge_fallback_conf(['Add'])
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


# Test CPU bridge with aggregations containing lambda functions (ArrayForAll 
# with collect_list/collect_set) This test covers the scenario where lambda 
# functions are used within aggregate expressions that fall back to the CPU bridge. 
# It verifies that lambda functions remain on the CPU side and arenot incorrectly
# converted to GPU inputs, which would cause resolution errors.
@ignore_order(local=True)
@allow_non_gpu('ArrayForAll', 'NamedLambdaVariable', 'GreaterThan', 'Length', 'LambdaFunction')
@pytest.mark.parametrize('data_gen', [[
    ('k1', RepeatSeqGen(LongGen(), length=100)),
    ('k2', RepeatSeqGen(LongGen(), length=20)),
    ('v', StringGen(nullable=False))]], ids=idfn)
@pytest.mark.parametrize('use_obj_hash_agg', ['false', 'true'], ids=idfn)
def test_cpu_bridge_aggregate_with_lambda_functions(spark_tmp_table_factory,
                                                     data_gen,
                                                     use_obj_hash_agg):
    # --- Create bucketed table to trigger AQE optimization ---
    bucketed_table = spark_tmp_table_factory.get()
    def write_bucket_table(spark):
        df = gen_df(spark, data_gen, length=4096)
        return df.write.bucketBy(8, "k1").format('parquet').mode("overwrite") \
            .saveAsTable(bucketed_table)
    with_cpu_session(write_bucket_table)

    # --- Run test case with CPU bridge enabled ---
    conf = {
        'spark.sql.adaptive.enabled': True,
        'spark.sql.execution.useObjectHashAggregateExec': use_obj_hash_agg,
        # Enable CPU bridge and force ArrayForAll to use it, just in case.
        'spark.rapids.sql.expression.cpuBridge.enabled': True,
        'spark.rapids.sql.expression.ArrayForAll': False
    }
    
    # Query uses forall with lambda functions - ArrayForAll will use CPU bridge
    # The lambda functions (x -> length(x) > 0) must remain on CPU side
    query = """
        select a,
            forall(collect_list(b), x -> length(x) > 0) as forall_list,
            forall(collect_set(b), x -> length(x) > 1) as forall_set
        from (
            select k1, max(k2) as a, max(v) as b
            from table
            group by k1
        ) t
        group by a"""
    exist_clz = ['ArrayForAll']

    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark: spark.table(bucketed_table),
        table_name='table',
        exist_classes=','.join(exist_clz),
        sql=query,
        conf=conf)
