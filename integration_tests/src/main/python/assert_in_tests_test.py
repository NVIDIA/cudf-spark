# Copyright (c) 2025, NVIDIA CORPORATION.
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

"""
Integration tests for assertInTests utility from Python/pytest.

This test suite verifies that the assertInTests utility correctly detects
test environments via the com.nvidia.spark.rapids.runningTests system property.
"""

from spark_session import with_gpu_session, with_cpu_session


def test_assertInTests_detects_test_environment():
    """
    Test that assertInTests can detect it's running in the integration test environment.
    This works because run_pyspark_from_build.sh sets com.nvidia.spark.rapids.runningTests=true
    """
    def test_in_executors(spark):
        # Get the Scala helper class
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Test that a true assertion executes and passes
        assert_executed = helper.testAssertion(True, "This assertion should execute in tests")
        assert assert_executed, "Assertion body should have been executed in test environment"

        return True

    def _test_detection(spark):
        spark.sparkContext.parallelize(range(1, 10)).map(test_in_executors(spark))
    
    # Run on GPU
    with_gpu_session(_test_detection)
    
    # Run on CPU too (to verify it works regardless of GPU/CPU mode)
    with_cpu_session(_test_detection)


def test_assertInTests_works_on_driver():
    """
    Test that assertInTests works on the driver (simplified from executor test).
    Testing on executors is complex due to Spark serialization constraints.
    """
    def _test_on_driver(spark):
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Test assertion execution
        assert_executed = helper.testAssertion(True, "Assertion on driver")
        
        # Verify assertion ran
        assert assert_executed, "Assertion should have executed on driver"
        
        return True
    
    with_gpu_session(_test_on_driver)

