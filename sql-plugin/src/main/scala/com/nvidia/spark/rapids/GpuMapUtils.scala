/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import java.util.Optional

import ai.rapids.cudf.{ColumnVector, ColumnView, DType}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableColumn

import org.apache.spark.sql.catalyst.expressions.{Expression, MapFromArrays}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuMapFromArrays
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types.DataType

/**
 * Provide a set of APIs to manipulate map columns in common ways. CUDF does not officially support
 * maps so we store it as a list of key/value structs.
 */
object GpuMapUtils {
  val KEY_INDEX: Int = 0
  val VALUE_INDEX: Int = 1
  private[this] def pullChildOutAsListView(input: ColumnView, index: Int): ColumnView = {
    withResource(input.getChildColumnView(0)) { structView =>
      withResource(structView.getChildColumnView(index)) { keyView =>
        GpuListUtils.replaceListDataColumnAsView(input, keyView)
      }
    }
  }

  def getMapValueOrThrow(
      map: ColumnVector,
      indices: ColumnVector,
      dtype: DataType,
      origin: Origin): ColumnVector = {
    withResource(map.getMapKeyExistence(indices)) { keyExists =>
      withResource(keyExists.all()) { exist =>
        if (exist.isValid && exist.getBoolean) {
          map.getMapValue(indices)
        } else {
          val firstFalseKey = getFirstFalseKey(indices, keyExists)
          throw RapidsErrorUtils.mapKeyNotExistError(firstFalseKey, dtype, origin)
        }
      }
    }
  }

  private def getFirstFalseKey(indices: ColumnVector, keyExists: ColumnVector): String = {
    withResource(new ai.rapids.cudf.Table(Array(indices, keyExists):_*)) { table =>
      withResource(keyExists.not()) { keyNotExist =>
        withResource(table.filter(keyNotExist)) { tableWithBadKeys =>
          val badKeys = tableWithBadKeys.getColumn(0)
          withResource(badKeys.getScalarElement(0)) { firstBadKey =>
            val key = GpuScalar.extract(firstBadKey)
            if (key != null) {
              key.toString
            } else {
              "null"
            }
          }
        }
      }
    }
  }

  /**
   * Get the keys from a map column as a list.
   * @param input the input map column.
   * @return a list of the keys as a column view.
   */
  def getKeysAsListView(input: ColumnView): ColumnView =
    pullChildOutAsListView(input, KEY_INDEX)

  /**
   * Get the values from a map column as a list.
   * @param input the input map column.
   * @return a list of the values as a column view.
   */
  def getValuesAsListView(input: ColumnView): ColumnView =
    pullChildOutAsListView(input, VALUE_INDEX)

  private[this] def replaceStructChild(
      structView: ColumnView,
      toReplace: ColumnView,
      index: Int): ColumnView = {

    withResource(structView.getValid) { validity =>
      val childViews = new Array[ColumnView](structView.getNumChildren)
      try {
        childViews.indices.foreach { idx =>
          if (idx == index) {
            childViews(idx) = toReplace
          } else {
            childViews(idx) = structView.getChildColumnView(idx)
          }
        }
        new ColumnView(DType.STRUCT, structView.getRowCount,
          Optional.empty[java.lang.Long](), validity, null,
          childViews)
      } finally {
        childViews.indices.foreach { idx =>
          // We don't want to try and close the view that was passed in.
          if (idx != index) {
            childViews(idx).safeClose()
          }
        }
      }
    }
  }

  private[this] def replaceExplodedKeyOrValueAsView(
      mapCol: ColumnView,
      toReplace: ColumnView,
      index: Int): ColumnView = {
    withResource(mapCol.getChildColumnView(0)) { keyValueView =>
      withResource(replaceStructChild(keyValueView, toReplace, index)) { newKeyValueView =>
        GpuListUtils.replaceListDataColumnAsView(mapCol, newKeyValueView)
      }
    }
  }

  /**
   * Replace the values in a map. The values are the underlying exploded values (not in a list)
   * @param mapCol the original map column
   * @param newValue the new values column
   * @return and updated map column view
   */
  def replaceExplodedValueAsView(
      mapCol: ColumnView,
      newValue: ColumnView): ColumnView =
    replaceExplodedKeyOrValueAsView(mapCol, newValue, VALUE_INDEX)

  /**
   * Replace the keys in a map. The values are the underlying exploded keys (not in a list)
   * @param mapCol the original map column
   * @param newKey the new keys column
   * @return and updated map column view
   */
  def replaceExplodedKeyAsView(
      mapCol: ColumnView,
      newKey: ColumnView): ColumnView =
    replaceExplodedKeyOrValueAsView(mapCol, newKey, KEY_INDEX)

  def assertNoNullKeys(mapView: ColumnView): Unit = {
    withResource(mapView.getChildColumnView(0)) { keyValueList =>
      withResource(keyValueList.getChildColumnView(KEY_INDEX)) { keyView =>
        if (keyView.getNullCount > 0) {
          throw new RuntimeException("Cannot use null as map key.")
        }
      }
    }
  }

  // Copied from Spark org.apache.spark.sql.errors.QueryExecutionErrors
  def duplicateMapKeyFoundError: Throwable = {
    new RuntimeException(s"Duplicate map key was found, please check the input " +
        "data. If you want to remove the duplicated keys, you can set " +
        s"${SQLConf.MAP_KEY_DEDUP_POLICY.key} to ${SQLConf.MapKeyDedupPolicy.LAST_WIN} so that " +
        "the key inserted at last takes precedence.")
  }

}

case class GpuMapFromArraysMeta(expr: MapFromArrays,
                                override val conf: RapidsConf,
                                override val parent: Option[RapidsMeta[_, _, _]],
                                rule: DataFromReplacementRule)
  extends BinaryExprMeta[MapFromArrays](expr, conf, parent, rule) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuMapFromArrays(lhs, rhs)
}
