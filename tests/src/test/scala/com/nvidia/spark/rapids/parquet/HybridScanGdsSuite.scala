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

package com.nvidia.spark.rapids.parquet

import ai.rapids.cudf.{ByteRange, CudfException}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.jni.NativeFileSource
import org.scalatest.{BeforeAndAfterAll, Canceled, Outcome}

/**
 * [[HybridScanNativeFileSuite]]'s tests with KvikIO in AUTO mode, where it reads through cuFile
 * if it can and through POSIX if not, plus a check of which it does.
 *  - The data checks pass either way.
 *  - "a local file is opened for cuFile" is cancelled when cuFile is not used. Set
 *    -Dgds.required=true to make it fail instead, on machines that should have GDS.
 *  - libcudf fixes KvikIO's mode at the first cuDF IO in a JVM, so the tests run only in the JVM
 *    tests/pom.xml starts for this suite, and are cancelled in any other.
 */
class HybridScanGdsSuite extends HybridScanNativeFileSuite with BeforeAndAfterAll {
  private val ownJvm = java.lang.Boolean.getBoolean("rapids.test.hybridScanGdsJvm")
  private val gdsRequired = java.lang.Boolean.getBoolean("gds.required")

  // Large enough for device reads above KvikIO's GDS threshold, unless KVIKIO_GDS_THRESHOLD
  // raises it.
  private val LARGE_FILE_SIZE = 9 << 20

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (ownJvm) {
      NativeFileSource.setUpKvikio(NativeFileSource.CompatMode.AUTO, 4)
    }
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (ownJvm) {
      super.withFixture(test)
    } else {
      Canceled("runs only in the JVM tests/pom.xml starts for HybridScanGdsSuite")
    }
  }

  // Does not detect cuFile falling back to its own compatibility mode; see gds_stats for that.
  test("a local file is opened for cuFile") {
    withLocalFile(LARGE_FILE_SIZE) { path =>
      val metrics = newMetrics()
      val large = Array(new ByteRange((1L << 20) + 3, 5L << 20), new ByteRange(0L, 4096L))
      withResource(nativeSource(path, metrics, gdsEnabled = true)) { source =>
        // The first query opens the file, and counts a fallback if KvikIO did not open it for
        // cuFile.
        val direct = source.isDeviceReadDirect(large)
        val openedForCuFile = metrics(GpuMetric.HYBRID_SCAN_GDS_FALLBACK_SPLITS).value == 0
        info(s"KvikIO opened $path for cuFile: $openedForCuFile; 5 MiB read direct: $direct")
        val message = s"KvikIO is not reading $path through cuFile"
        val stagedMessage = s"KvikIO would stage the 5 MiB read of $path through host memory; " +
          "is KVIKIO_GDS_THRESHOLD above 5 MiB?"
        if (gdsRequired) {
          assert(openedForCuFile, message)
          assert(direct, stagedMessage)
        } else {
          assume(openedForCuFile, message)
          assume(direct, stagedMessage)
        }
        // cuFile can open a file on a machine that cannot read it through GDS.
        try {
          assertDeviceFetchHoldsRanges(source, large)
        } catch {
          case e: CudfException if !gdsRequired => cancel(s"cuFile cannot read $path here", e)
        }
      }
    }
  }
}
