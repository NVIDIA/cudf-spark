/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.test

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Objects, UUID}

import scala.collection.mutable
import scala.util.Random

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

case class CsvWriterWrapper(filePath: String, conf: Configuration) extends AutoCloseable {

  // This is implemented as a method to make it easier to subclass
  // ColumnarOutputWriter in the tests, and override this behavior.
  private def getOutputStream: FSDataOutputStream = {
    val hadoopPath = new Path(filePath)
    val fs = hadoopPath.getFileSystem(conf)
    fs.create(hadoopPath, false)
  }
  private var fileStream: FSDataOutputStream = getOutputStream

  override def close(): Unit = {
    if (fileStream != null) {
      // csv writer will close under file writer
      fileStream.close()
      fileStream = null
    }
  }

  def escape(str: String): String = {
    if (str == null) {
      ""
    } else {
      "\"" + str.replace("\n", "**LF**")
        .replace("\r", "**CR**")
        .replace("\"", "**QT**")
        .replace(",", "**COMMA**")+ "\""
    }
  }

  def writeRow(isLegacy: Boolean, row: Array[String]): Unit = {
    val fullSeq = Seq(isLegacy.toString) ++ row
    fileStream.write(fullSeq.map(escape).mkString("", ",", "\n").getBytes("UTF8"))
  }
}

/**
 * Used to mask customer data to avoid customer data leakage.
 */
object GetJsonObjectMask {

  /**
   * Used by mask data
   */
  private def getRetainChars: Set[Char] = {
    val s = mutable.Set[Char]()
    for (i <- 0 to 32) {
      s += i.toChar
    }
    val others = Array[Char](
      '{', '}', '[', ']', ',', ':', '"', '\'',
      '\\', '/', 'b', 'f', 'n', 'r', 't', 'u',
      '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'e', 'E',
      'u', 'A', 'a', 'B', 'b', 'C', 'c', 'D', 'd', 'E', 'e', 'F', 'f',
      't', 'r', 'u', 'e',
      'f', 'a', 'l', 's', 'e',
      'n', 'u', 'l', 'l',
      '$', '[', ']', '.', '*', '\'', '?'
    )
    s ++= others
    s.toSet
  }

  private val RETAIN_CHARS = getRetainChars

  private def getCharsForKey: Set[Char] = {
    val buf = new mutable.ArrayBuffer[Char]
    for (c <- 'A' to 'Z') {
      buf.append(c)
    }
    for (c <- 'a' to 'z') {
      buf.append(c)
    }
    buf.toSet
  }

  private val oneToOneMappingChars: Set[Char] = getCharsForKey -- RETAIN_CHARS

  /**
   * Mask data. RAPIDS Accelerator should not dump the original Customer data.
   * This dump tool only care about the functionality of get-json-object, the masked data should
   * reproduce issues if original data/path can reproduce issues. The mask is to find a way to
   * mask data and reproduce issues by using masked data.
   *
   * Special/retain chars, the following chars will not be masked:
   *     ASCII chars [0, 31] including space char
   *     { } [ ] , : " ' :  JSON structure chars, should not mask
   *     \  :  escape char, should not mask
   *     / b f n r t u : can follow \, should not mask
   *     - :  used by number, should not mask
   *     0-9  : used by number, it's special char, mask method refers to the following
   *     e E  : used by number, e.g.: 1.0E-3, should not mask
   *     u A-F a-f : used by JSON string by unicode, e.g.: \u1e2F
   *     true :  should not mask
   *     false :  should not mask
   *     null :  should not mask
   *     $ [ ] . * '  : used by path, should not mask
   *     ?  : json path supports although Spark does not support, also add this because has no side
   *     effect
   * Above special/retain chars should not be masked, or the JSON will be invalid.
   *
   * Mask logic:
   *     - Assume path only contains a-z, A-Z, '_' and [0-9]
   *     - For digits [1-9] create a random one to one mapping and replace, note do not touch '0'
   *       Because 00 number is invalid.
   *     - For above special/retain chars do not change
   *     - For char set [a-z, A-Z] minus special/retain chars like [eE1-9], create a random one to
   *       one mapping to mask data. e.g.: a -> b, b -> c, ..., z -> a
   *     - For other chars, e.g.: Chinese chars, map to a const char 's'
   *
   * @return masked data
   */
  def mask(
      seed: Long,
      pathStr: String,
      jsonStr: String,
      cpuResult: String,
      gpuResult: String): Array[String] = {
    val random = new Random(seed)
    // generate one to one map
    // Note: path/json/result should use the same mask way
    val randomInt = random.nextInt()
    val charMap = getMap(randomInt)
    val digitMap = getDigitMap(randomInt)
    Array(
      doMask(pathStr, RETAIN_CHARS, charMap, digitMap),
      doMask(jsonStr, RETAIN_CHARS, charMap, digitMap),
      doMask(cpuResult, RETAIN_CHARS, charMap, digitMap),
      doMask(gpuResult, RETAIN_CHARS, charMap, digitMap)
    )
  }

  private def getMap(seed: Int): Map[Char, Char] = {
    val random = new Random(seed)
    val charsFrom = oneToOneMappingChars.toList
    val charsTo = random.shuffle(oneToOneMappingChars.toList)
    val map = mutable.Map[Char, Char]()
    for( i <- charsFrom.indices) {
      map(charsFrom(i)) = charsTo(i)
    }
    map.toMap
  }

  private def getDigitMap(seed: Int): Map[Char, Char] = {
    val random = new Random(seed)
    val digits = '1' to '9'
    val from = digits.toList
    val to = random.shuffle(digits.toList)
    val map = mutable.Map[Char, Char]()
    for (i <- from.indices) {
      map(from(i)) = to(i)
    }
    map.toMap
  }


  /**
   * Mask chars
   * @param originStr origin json/path/result string
   * @param retainChars retain chars, should not be masked
   * @param oneToOneMap char to char map for masking
   * @return masked string
   */
  private def doMask(
      originStr: String,
      retainChars: Set[Char],
      oneToOneMap: Map[Char, Char],
      digitMap: Map[Char, Char]): String = {
    if (originStr != null) {
      val buf = new StringBuilder(originStr.length)
      var idx = 0
      while (idx < originStr.length) {
        val originChar = originStr(idx)
        idx += 1
        if (originChar >= '1' && originChar <= '9') {
          // digits need to one to one map
          val toDigit = digitMap(originChar)
          buf.append(toDigit)
        } else {
          // not in [1-9]
          if (oneToOneMappingChars.contains(originChar)) {
            // chars need one to one map
            val toChar = oneToOneMap(originChar)
            buf.append(toChar)
          } else {
            if (!retainChars.contains(originChar)) {
              // if it's not a retain char, replace to a const char 's'
              buf.append('s')
            } else {
              // retain char, do not change
              buf.append(originChar)
            }
          }
        }
      }
      buf.toString
    } else {
      null
    }
  }
}

object CpuGetJsonObject {
  /**
   * verify results from Cpu and Gpu, save diffs if have
   * @param dataCv original JSON data
   * @param path the path to extract JSON data
   * @param fromGpuCv result from GPU
   * @param fromCpuHCV result from CPU
   * @param savePathForVerify save path if have diffs
   * @param saveRowsForVerify max diff rows to save, Note: only take effective for current data
   */
  def verify(
      isLegacy: Boolean,
      seed: Long,
      dataCv: ColumnVector,
      path: UTF8String,
      fromGpuCv: ColumnVector,
      fromCpuHCV: HostColumnVector,
      savePathForVerify: String,
      saveRowsForVerify: Int,
      conf: Configuration): Unit = {
    withResource(dataCv.copyToHost()) { dataHCV =>
      withResource(fromGpuCv.copyToHost()) { fromGpuHCV =>
        val tcId = TaskContext.get.taskAttemptId()
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
        val uuid = UUID.randomUUID()
        val savePath = s"$savePathForVerify/${date}_${tcId}_${uuid}.csv"
        withResource(CsvWriterWrapper(savePath, conf)) { csvWriter =>
          val pathStr = if (path == null) "null" else path.toString
          var currRow = 0
          var diffRowsNum = 0
          while (currRow < dataCv.getRowCount.toInt &&
              diffRowsNum < saveRowsForVerify
          ) {
            val str = dataHCV.getJavaString(currRow)
            val cpuStr = if (fromCpuHCV.isNull(currRow)) null else fromCpuHCV.getJavaString(currRow)
            val gpuStr = if (fromGpuHCV.isNull(currRow)) null else fromGpuHCV.getJavaString(currRow)
            if (!Objects.equals(cpuStr, gpuStr)) { // if have diff
              diffRowsNum += 1
              // mask customer data
              val masked = GetJsonObjectMask.mask(seed, pathStr, str, cpuStr, gpuStr)
              csvWriter.writeRow(isLegacy, masked)
            }
            currRow += 1
          }
        }
      }
    }
  }

  /**
   * Run get-json-object on CPU
   * @param dataCv original JSON data
   * @param path path scalar
   * @return CPU result of get-json-object
   */
  def getJsonObjectOnCpu(dataCv: GpuColumnVector, path: UTF8String): HostColumnVector = {
    withResource(dataCv.copyToHost()) { dataHCV =>
      withResource(HostColumnVector.builder(DType.STRING, dataHCV.getRowCount.toInt)) {
        resultBuilder =>
          val pathLiteral = Literal.create(path, StringType)
          for (i <- 0 until dataHCV.getRowCount.toInt) {
            val json = dataHCV.getUTF8String(i)
            // In order to use `GetJsonObject` directly,
            // here use a literal json and a literal path
            val jsonLiteral = Literal.create(json, StringType)
            val cpuGetJsonObject = GetJsonObject(jsonLiteral, pathLiteral)
            // input null is safe because both json and path are literal
            val utf8String = cpuGetJsonObject.eval(null)
            if (utf8String == null) {
              resultBuilder.appendNull()
            } else {
              resultBuilder.append(utf8String.toString)
            }
          }
          resultBuilder.build()
      }
    }
  }
}
