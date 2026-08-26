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

from unittest.mock import Mock

import pytest

from asserts import _prep_func_for_compare
from marks import validate_execs_in_gpu_plan
import spark_session
from spark_session import with_gpu_session


# Tests in this file validate the behavior of plan validation in the python test.


_adaptive_conf = {"spark.sql.adaptive.enabled": "true"}
_validate_execs_conf = "spark.rapids.sql.test.validateExecsInGpuPlan"


def _install_fake_validation_callback(monkeypatch, callback):
    spark = Mock()
    rapids = spark.sparkContext._jvm.org.apache.spark.sql.rapids
    rapids.ExecutionPlanCaptureCallback = callback
    monkeypatch.setattr(
        spark_session, "with_spark_session", lambda func, conf: func(spark))


def test_gpu_session_does_not_catch_keyboard_interrupt(monkeypatch):
    callback = Mock()
    _install_fake_validation_callback(monkeypatch, callback)

    def interrupt(_):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        with_gpu_session(interrupt)

    callback.getValidationErrorWithTimeout.assert_not_called()


def test_gpu_session_does_not_suppress_keyboard_interrupt_from_validation(monkeypatch):
    callback = Mock()
    callback.getValidationErrorWithTimeout.side_effect = KeyboardInterrupt
    _install_fake_validation_callback(monkeypatch, callback)

    def fail(_):
        raise RuntimeError("primary test error")

    with pytest.raises(KeyboardInterrupt):
        with_gpu_session(fail)


@validate_execs_in_gpu_plan("MissingFromFinalPlan")
def test_aqe_final_plan_validation_failure_reaches_python():
    with pytest.raises(AssertionError, match="MissingFromFinalPlan"):
        with_gpu_session(
            lambda spark: spark.range(10).repartition(2).count(),
            conf=_adaptive_conf)


@validate_execs_in_gpu_plan("MissingAfterPrimaryError")
def test_aqe_query_error_remains_primary_over_plan_validation():
    class ExpectedError(Exception):
        pass

    def run_and_fail(spark):
        spark.range(10).repartition(2).count()
        raise ExpectedError("primary query error")

    with pytest.raises(ExpectedError, match="primary query error"):
        with_gpu_session(run_and_fail, conf=_adaptive_conf)


def test_aqe_first_action_validation_error_reaches_python():
    def run_actions(spark):
        spark.conf.set(_validate_execs_conf, "MissingFromFirstAction")
        spark.range(10).repartition(2).count()
        spark.conf.set(_validate_execs_conf, "MissingFromSecondAction")
        spark.range(20).repartition(2).count()

    with pytest.raises(AssertionError) as error:
        with_gpu_session(run_actions, conf=_adaptive_conf)

    assert "MissingFromFirstAction" in str(error.value)
    assert "MissingFromSecondAction" not in str(error.value)


def test_aqe_validation_state_is_cleared_between_gpu_sessions():
    def run_with_validation_error(spark):
        spark.conf.set(_validate_execs_conf, "MissingFromPreviousSession")
        return spark.range(10).repartition(2).count()

    with pytest.raises(AssertionError, match="MissingFromPreviousSession"):
        with_gpu_session(run_with_validation_error, conf=_adaptive_conf)

    assert with_gpu_session(
        lambda spark: spark.range(1).count(), conf=_adaptive_conf) == 1


@validate_execs_in_gpu_plan("MissingFromIteratorFinalPlan")
def test_to_local_iterator_validates_after_normal_exhaustion():
    bring_back, _ = _prep_func_for_compare(lambda spark: spark.range(1), "ITERATOR")
    iterator = with_gpu_session(bring_back, conf=_adaptive_conf)

    assert next(iterator).id == 0
    with pytest.raises(AssertionError, match="MissingFromIteratorFinalPlan"):
        next(iterator)


@validate_execs_in_gpu_plan("MissingAfterIteratorError")
def test_to_local_iterator_error_remains_primary():
    def make_failing_df(spark):
        return spark.range(2, numPartitions=2).selectExpr(
            "IF(id = 0, id, raise_error('iterator primary error')) AS id")

    bring_back, _ = _prep_func_for_compare(make_failing_df, "ITERATOR")
    iterator = with_gpu_session(bring_back, conf=_adaptive_conf)

    assert next(iterator).id == 0
    with pytest.raises(Exception, match="iterator primary error") as error:
        next(iterator)
    assert not isinstance(error.value, AssertionError)
