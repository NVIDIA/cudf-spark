/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{ExceptionWithRowIndex, ParseURI}
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuParseUrl {
  val HOST = "HOST"
  val PATH = "PATH"
  val QUERY = "QUERY"
  val REF = "REF"
  val PROTOCOL = "PROTOCOL"
  val FILE = "FILE"
  val AUTHORITY = "AUTHORITY"
  val USERINFO = "USERINFO"

  def isSupportedPart(part: String): Boolean = {
    part match {
      case REF | FILE | AUTHORITY | USERINFO =>
        false
      case _ => // PROTOCOL, HOST, QUERY, PATH and invalid parts are supported
        true
    }
  }
}

case class GpuParseUrl(children: Seq[Expression], failOnError: Boolean)
  extends GpuExpression with ShimExpression with ExpectsInputTypes {

  override def nullable: Boolean = true
  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType
  override def prettyName: String = "parse_url"

  import GpuParseUrl._

  def doColumnar(url: GpuColumnVector, partToExtract: GpuScalar): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    part match {
      case PROTOCOL =>
        ParseURI.parseURIProtocol(url.getBase, failOnError)
      case HOST =>
        ParseURI.parseURIHost(url.getBase, failOnError)
      case QUERY =>
        ParseURI.parseURIQuery(url.getBase, failOnError)
      case PATH =>
        ParseURI.parseURIPath(url.getBase, failOnError)
      case REF | FILE | AUTHORITY | USERINFO =>
        throw new UnsupportedOperationException(s"$this is not supported partToExtract=$part. " +
            s"Only PROTOCOL, HOST, QUERY and PATH are supported")
      case _ =>
        // For invalid parts, we still need to validate the URL first in ANSI mode
        // If the URL is invalid, the parsing should fail and throw an error
        // If the URL is valid, then we return NULL for the invalid part
        if (failOnError) {
          // Try to parse with PROTOCOL to validate the URL, but ignore the result
          // This will throw an exception if the URL is invalid
          withResource(ParseURI.parseURIProtocol(url.getBase, failOnError)) { _ =>
            // URL is valid, return NULL for invalid part
            GpuColumnVector.columnVectorFromNull(url.getRowCount.toInt, StringType)
          }
        } else {
          GpuColumnVector.columnVectorFromNull(url.getRowCount.toInt, StringType)
        }
    }
  }

  def doColumnar(col: GpuColumnVector, partToExtract: GpuScalar, key: GpuScalar): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    if (part != QUERY || key == null || !key.isValid) {
      // return a null columnvector
      return GpuColumnVector.columnVectorFromNull(col.getRowCount.toInt, StringType)
    }
    val keyStr = key.getValue.asInstanceOf[UTF8String].toString
    ParseURI.parseURIQueryWithLiteral(col.getBase, keyStr, failOnError)
  }

  def doColumnar(col: GpuColumnVector, partToExtract: GpuScalar, 
      key: GpuColumnVector): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    if (part != QUERY) {
      // return a null columnvector
      return GpuColumnVector.columnVectorFromNull(col.getRowCount.toInt, StringType)
    }
    ParseURI.parseURIQueryWithColumn(col.getBase, key.getBase, failOnError)
  }

  private def parseUrlException(rowException: ExceptionWithRowIndex,
                                  part: String): Nothing = {
    val errorRowIndex = rowException.getRowIndex
    throw new SparkException(
      s"[INVALID_URL] The url is invalid: $part at row $errorRowIndex")
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (children.size == 2) {
      val Seq(url, partToExtract) = children
      withResourceIfAllowed(url.columnarEval(batch)) { urls =>
        withResourceIfAllowed(partToExtract.columnarEvalAny(batch)) { parts =>
          parts match {
            case partScalar: GpuScalar =>
              try {
                GpuColumnVector.from(doColumnar(urls, partScalar), dataType)
              } catch {
                case rowException: ExceptionWithRowIndex if failOnError =>
                  val urlValue = ColumnViewUtils.getElementStringFromColumnView(
                    urls.getBase, rowException.getRowIndex)
                  parseUrlException(rowException, s"'$urlValue'")
              }
            case _ =>
              throw new UnsupportedOperationException(
                s"Cannot columnar evaluate expression: $this")
          }
        }
      }
    } else {
      // 3-arg, i.e. QUERY with key
      assert(children.size == 3)
      val Seq(url, partToExtract, key) = children
      withResourceIfAllowed(url.columnarEval(batch)) { urls =>
        withResourceIfAllowed(partToExtract.columnarEvalAny(batch)) { parts =>
          withResourceIfAllowed(key.columnarEvalAny(batch)) { keys =>
            (urls, parts, keys) match {
              case (urlCv: GpuColumnVector, partScalar: GpuScalar, keyScalar: GpuScalar) =>
                try {
                  GpuColumnVector.from(doColumnar(urlCv, partScalar, keyScalar), dataType)
                } catch {
                  case rowException: ExceptionWithRowIndex if failOnError =>
                    val urlValue = ColumnViewUtils.getElementStringFromColumnView(
                      urlCv.getBase, rowException.getRowIndex)
                    parseUrlException(rowException, s"'$urlValue'")
                }
              case (urlCv: GpuColumnVector, partScalar: GpuScalar, keyCv: GpuColumnVector) =>
                try {
                  GpuColumnVector.from(doColumnar(urlCv, partScalar, keyCv), dataType)
                } catch {
                  case rowException: ExceptionWithRowIndex if failOnError =>
                    val urlValue = ColumnViewUtils.getElementStringFromColumnView(
                      urlCv.getBase, rowException.getRowIndex)
                    parseUrlException(rowException, s"'$urlValue'")
                }
              case _ =>
                throw new 
                    UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
            }
          }
        }
      }
    }
  }
}
