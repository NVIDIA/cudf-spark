/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import java.io.File
import java.security.SecureRandom

import org.apache.hadoop.conf.Configuration
import org.apache.orc.{EncryptionAlgorithm, InMemoryKeystore}
import org.apache.orc.impl.CryptoUtils

class OrcEncryptionSuite extends SparkQueryCompareTestSuite {

  // Create an InMemoryKeystore provider and addKey `pii` to it.
  // CryptoUtils caches it so it can be used later by the test
  val hadoopConf = new Configuration()
  hadoopConf.set("orc.key.provider", "memory")
  val random = new SecureRandom()
  val keystore: InMemoryKeystore =
    CryptoUtils.getKeyProvider(hadoopConf, random).asInstanceOf[InMemoryKeystore]
  val algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES_CTR_128
  val piiKey = new Array[Byte](algorithm.keyLength)
  val topSecretKey = new Array[Byte](algorithm.keyLength)
  random.nextBytes(piiKey)
  random.nextBytes(topSecretKey)
  keystore.addKey("pii", algorithm, piiKey).addKey("top_secret", algorithm, topSecretKey)

  testGpuWriteFallback(
    "Write encrypted ORC fallback",
    "DataWritingCommandExec",
    intsDf,
    execsAllowedNonGpu = Seq("ShuffleExchangeExec", "DataWritingCommandExec", "WriteFilesExec")) {
    frame =>
      // ORC encryption is only allowed in 3.2+
      val isValidTestForSparkVersion = ShimLoader.getShimVersion match {
        case SparkShimVersion(major, minor, _) => major == 3 && minor != 1
        case DatabricksShimVersion(major, minor, _, _) => major == 3 && minor != 1
        case ClouderaShimVersion(major, minor, _, _) => major == 3 && minor != 1
        case _ => true
      }
      assume(isValidTestForSparkVersion)

      val tempFile = File.createTempFile("orc-encryption-test", "")
      frame.write.options(Map("orc.key.provider" -> "memory",
        "orc.encrypt" -> "pii:ints,more_ints",
        "orc.mask" -> "sha256:ints,more_ints")).mode("overwrite").orc(tempFile.getAbsolutePath)
  }
}
