/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark":["401","402","411"],"carryForward":"none"}
{"spark":["405"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.delta

import java.lang.reflect.InvocationTargetException

import com.nvidia.spark.rapids.RapidsConf
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.execution.datasources.FileFormat

class DeltaProbeImplSuite extends AnyFunSuite {
  test("an unusable Delta runtime shim disables only Delta acceleration") {
    assert(DeltaProbeImpl.providerOrNoDelta(None) eq NoDeltaProvider)
    Seq[Throwable](
      new NoClassDefFoundError("org/apache/spark/internal/LoggingShims"),
      new ClassNotFoundException("org.apache.spark.sql.delta.rapids.delta33x.Delta33xRuntimeShim"),
      new InvocationTargetException(new NoClassDefFoundError("org/apache/spark/internal/Logging"))
    ).foreach { error =>
      assert(DeltaProbeImpl.providerOrNoDelta(throw error) eq NoDeltaProvider)
    }
  }

  test("an unreadable Delta version does not undo the fallback") {
    val noVersion = new NoClassDefFoundError("io/delta/package$")
    assert(DeltaProbeImpl.providerOrNoDelta(throw noVersion, throw noVersion) eq NoDeltaProvider)
  }

  test("a usable Delta runtime shim provider is returned unchanged") {
    val provider = mock[DeltaProvider]
    assert(DeltaProbeImpl.providerOrNoDelta(Some(provider)) eq provider)
  }

  test("planning-hook linkage failures after shim construction fall back to NoDeltaProvider") {
    val brokenScans = mock[DeltaProvider]
    when(brokenScans.getScans).thenThrow(
      new NoClassDefFoundError("io/delta/spark/internal/v2/read/SparkScan"))
    assert(DeltaProbeImpl.providerOrNoDelta(Some(brokenScans)) eq NoDeltaProvider)

    val brokenExecRules = mock[DeltaProvider]
    when(brokenExecRules.getExecRules).thenThrow(
      new NoClassDefFoundError("org/apache/spark/sql/delta/commands/DeleteCommand"))
    assert(DeltaProbeImpl.providerOrNoDelta(Some(brokenExecRules)) eq NoDeltaProvider)

    val brokenFormatCheck = mock[DeltaProvider]
    when(brokenFormatCheck.isSupportedFormat(classOf[FileFormat])).thenThrow(
      new NoClassDefFoundError("org/apache/spark/sql/delta/DeltaParquetFileFormat"))
    assert(DeltaProbeImpl.providerOrNoDelta(Some(brokenFormatCheck)) eq NoDeltaProvider)

    val brokenDvCheck = mock[DeltaProvider]
    when(brokenDvCheck.isPushDVPredicateDownEnabled(any(classOf[RapidsConf]))).thenThrow(
      new NoClassDefFoundError("org/apache/spark/sql/delta/sources/DeltaSQLConf"))
    assert(DeltaProbeImpl.providerOrNoDelta(Some(brokenDvCheck)) eq NoDeltaProvider)

    val fatalHook = mock[DeltaProvider]
    when(fatalHook.getStrategyRules).thenThrow(new OutOfMemoryError("test"))
    intercept[OutOfMemoryError] {
      DeltaProbeImpl.providerOrNoDelta(Some(fatalHook))
    }
  }

  test("fatal errors are not swallowed") {
    intercept[OutOfMemoryError] {
      DeltaProbeImpl.providerOrNoDelta(throw new OutOfMemoryError("test"))
    }
    // A fatal error from the reflective shim constructor arrives wrapped.
    val wrapped = intercept[InvocationTargetException] {
      DeltaProbeImpl.providerOrNoDelta(
        throw new InvocationTargetException(new OutOfMemoryError("test")))
    }
    assert(wrapped.getCause.isInstanceOf[OutOfMemoryError])
  }
}
