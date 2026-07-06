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

package com.nvidia.spark.rapids.fileio

import com.nvidia.spark.rapids.{HttpBackendType, PerfIO, PerfIOConf}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

class RapidsInputFilesSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val resolvedS3Conf = {
    val field = PerfIO.getClass.getDeclaredField("s3ApiConf")
    field.setAccessible(true)
    field
  }
  private var previousS3Conf: AnyRef = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    previousS3Conf = resolvedS3Conf.get(PerfIO)
    setResolvedS3State(enabled = false)
  }

  override protected def afterEach(): Unit = {
    resolvedS3Conf.set(PerfIO, previousS3Conf)
    super.afterEach()
  }

  test("S3 PerfIO gate uses the resolved opportunistic backend state") {
    // The resolved state is intentionally installed without a SparkEnv or an explicit
    // spark.rapids.perfio.s3.enabled value. Reading the raw optional config would return false.
    setResolvedS3State(enabled = true)

    assert(RapidsInputFiles.isS3PerfEnabled)
  }

  test("S3 PerfIO gate reports a resolved S3A fallback as disabled") {
    setResolvedS3State(enabled = false)

    assert(!RapidsInputFiles.isS3PerfEnabled)
  }

  private def setResolvedS3State(enabled: Boolean): Unit = {
    val conf = mock[PerfIOConf]
    when(conf.s3PerfEnabled).thenReturn(enabled)
    if (enabled) {
      when(conf.httpBackendType).thenReturn(HttpBackendType.CRT)
    }
    // Install only PerfIO's already-resolved configuration. Calling initS3Client here would
    // additionally initialize an HTTP backend, whose optional AWS SDK is deliberately absent
    // from this unit-test module's classpath.
    resolvedS3Conf.set(PerfIO, conf)
  }
}
