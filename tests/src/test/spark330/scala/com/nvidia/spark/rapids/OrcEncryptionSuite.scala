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
{"spark":["330","350","400","420"],"carryForward":"all"}
{"spark":["350db143"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import java.io.File
import java.security.SecureRandom

import org.apache.hadoop.conf.Configuration
import org.apache.orc.{
  EncryptionAlgorithm,
  EncryptionKey,
  EncryptionVariant,
  InMemoryKeystore,
  Reader,
  TypeDescription
}
import org.apache.orc.impl.CryptoUtils
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

class OrcEncryptionSuite extends SparkQueryCompareTestSuite with MockitoSugar {

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

  private def exceptionMessages(error: Throwable): String = {
    val messages = new StringBuilder
    var current = error
    while (current != null) {
      messages.append(current.toString)
      messages.append('\n')
      current = current.getCause
    }
    messages.toString()
  }

  private def mockEncryptedReader(
      encryptedRoot: TypeDescription,
      keyAvailable: Boolean): Reader = {
    val variant = mock[EncryptionVariant]
    when(variant.getRoot).thenReturn(encryptedRoot)
    val key = mock[EncryptionKey]
    when(key.getEncryptionRoots).thenReturn(Array(variant))
    when(key.isAvailable).thenReturn(keyAvailable)
    val reader = mock[Reader]
    when(reader.getColumnEncryptionKeys).thenReturn(Array(key))
    reader
  }

  testGpuWriteFallback(
    "Write encrypted ORC fallback",
    "DataWritingCommandExec",
    intsDf,
    execsAllowedNonGpu = Seq("ShuffleExchangeExec", "DataWritingCommandExec", "WriteFilesExec")) {
    frame =>
      // ORC encryption is only allowed in 3.2+
      val isValidTestForSparkVersion = ShimLoader.getShimVersion match {
        case SparkShimVersion(major, minor, _) => major > 3 || (major == 3 && minor >= 2)
        case DatabricksShimVersion(major, minor, _, _) =>
          major > 3 || (major == 3 && minor >= 2)
        case ClouderaShimVersion(major, minor, _, _) =>
          major > 3 || (major == 3 && minor >= 2)
        case _ => true
      }
      assume(isValidTestForSparkVersion)

      val tempFile = File.createTempFile("orc-encryption-test", "")
      frame.write.options(Map("orc.key.provider" -> "memory",
        "orc.encrypt" -> "pii:ints,more_ints",
        "orc.mask" -> "sha256:ints,more_ints")).mode("overwrite").orc(tempFile.getAbsolutePath)
  }

  test("Encrypted ORC read guard allows requested columns without available keys") {
    val fileSchema = TypeDescription.createStruct()
      .addField("encrypted", TypeDescription.createInt())
    val encryptedRoot = fileSchema.getChildren.get(0)
    val included = new Array[Boolean](fileSchema.getMaximumId + 1)
    included(encryptedRoot.getId) = true

    GpuOrcScan.failIfRequestedColumnsAreEncrypted(
      mockEncryptedReader(encryptedRoot, keyAvailable = false), included)
  }

  test("Encrypted ORC read guard allows count-only scans with root encryption") {
    val fileSchema = TypeDescription.createStruct()
      .addField("encrypted", TypeDescription.createInt())
    val included = new Array[Boolean](fileSchema.getMaximumId + 1)
    included(fileSchema.getId) = true

    GpuOrcScan.failIfRequestedColumnsAreEncrypted(
      mockEncryptedReader(fileSchema, keyAvailable = true), included)
  }

  test("Encrypted ORC reads return masked values when keys are unavailable") {
    withTempPath { path =>
      withCpuSparkSession { spark =>
        intsDf(spark).write.options(Map(
          "orc.key.provider" -> "memory",
          "orc.encrypt" -> "pii:ints",
          "orc.mask" -> "nullify:ints"))
          .mode("overwrite")
          .orc(path.getAbsolutePath)
      }

      val conf = new SparkConf()
        .set(RapidsConf.ORC_READER_TYPE.key, RapidsReaderType.MULTITHREADED.toString)
      val read = (spark: SparkSession) =>
        spark.read.orc(path.getAbsolutePath).select("ints")
      val (fromCpu, fromGpu) = runOnCpuAndGpu(read, df => df, conf, repart = 0)

      assert(fromCpu.length == 8)
      assert(fromCpu.forall(_.isNullAt(0)))
      assert(fromGpu.toSeq == fromCpu.toSeq)
    }
  }

  Seq(
    (RapidsReaderType.PERFILE, false),
    (RapidsReaderType.PERFILE, true),
    (RapidsReaderType.COALESCING, false),
    (RapidsReaderType.MULTITHREADED, false),
    (RapidsReaderType.MULTITHREADED, true)).foreach {
      case (readerType, ignoreCorruptFiles) =>
        test(s"Encrypted ORC reads fail only when a requested column uses an available key - " +
          s"$readerType, ignoreCorruptFiles=$ignoreCorruptFiles") {
          withTempPath { path =>
            withCpuSparkSession { spark =>
              intsDf(spark).write.options(Map(
                "orc.key.provider" -> "memory",
                "orc.encrypt" -> "pii:ints",
                "orc.mask" -> "nullify:ints"))
                .mode("overwrite")
                .orc(path.getAbsolutePath)
            }

            val conf = new SparkConf()
              .set(RapidsConf.ORC_READER_TYPE.key, readerType.toString)
              .set("spark.sql.files.ignoreCorruptFiles", ignoreCorruptFiles.toString)
              .set("orc.key.provider", "memory")
            withGpuSparkSession({ spark =>
              assert(spark.read.orc(path.getAbsolutePath).count() == 8)

              val unencryptedValues = spark.read.orc(path.getAbsolutePath)
                .select("more_ints")
                .collect()
                .map(_.getInt(0))
                .toSet
              assert(unencryptedValues == Set(1, 2, 3, 4, 5, 6, 7))

              val error = intercept[SparkException] {
                spark.read.orc(path.getAbsolutePath).select("ints").collect()
              }
              assert(exceptionMessages(error).contains(
                "The GPU does not support decrypting encrypted ORC columns"))
              assert(exceptionMessages(error).contains(RapidsConf.ENABLE_ORC_READ.key))
            }, conf)
          }
        }
  }
}
