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

package com.nvidia.spark.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.commons.io.IOUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

class RapidsSerializerManagerSuite extends AnyFunSuite {
  for {
    compressionEnabled <- Seq(false, true)
    encryptionEnabled <- Seq(false, true)
    shuffle <- Seq(false, true)
  } {
    test(s"stream wrapping with compression=$compressionEnabled " +
        s"encryption=$encryptionEnabled shuffle=$shuffle") {
      // Opposite flags detect accidentally using shuffle compression for spill streams.
      val conf = new SparkConf(false)
        .set("spark.io.compression.codec", "zstd")
        .set("spark.shuffle.spill.compress", compressionEnabled.toString)
        .set("spark.shuffle.compress", (!compressionEnabled).toString)
        .set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled.toString)
      val manager = new RapidsSerializerManager(conf)
      val blockId: BlockId = if (shuffle) {
        ShuffleBlockId(1, 2, 3)
      } else {
        BlockId(s"temp_local_${java.util.UUID.randomUUID()}")
      }
      val data = Array.tabulate[Byte](8192)(i => (i % 17).toByte)
      val reference = new ByteArrayOutputStream
      val referenceStream = if (compressionEnabled && !shuffle) {
        TrampolineUtil.createCodec(conf).compressedOutputStream(reference)
      } else {
        reference
      }
      withResource(referenceStream)(_.write(data))

      val output = new ByteArrayOutputStream
      withResource(manager.wrapStream(blockId, output)) { wrapped =>
        assert((wrapped eq output) == (shuffle || (!compressionEnabled && !encryptionEnabled)))
        if (compressionEnabled && !shuffle) {
          assert(wrapped.getClass == referenceStream.getClass)
        }
        wrapped.write(data)
      }
      // Compare the wire bytes independently of the matching reader, which could share a bug.
      assert(output.toByteArray.sameElements(reference.toByteArray) ==
        (shuffle || !encryptionEnabled))
      val input = new ByteArrayInputStream(output.toByteArray)
      withResource(manager.wrapStream(blockId, input)) { wrapped =>
        assert((wrapped eq input) == (shuffle || (!compressionEnabled && !encryptionEnabled)))
        assert(IOUtils.toByteArray(wrapped).sameElements(data))
      }
    }
  }
}
