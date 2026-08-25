/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.catalog.BucketTransform
import org.apache.spark.sql.delta.skipping.clustering.temp.{ClusterBySpec, ClusterByTransform => TempClusterByTransform}

object DeltaTrampoline {
  def getV1Table(v1Table: CatalogTable): V1Table = {
    V1Table(v1Table)
  }

  // Copy of DeltaCatalog.convertTransforms which uses private objects
  def convertTransforms(
      partitions: Seq[Transform]): (Seq[String], Option[BucketSpec], Option[ClusterBySpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]
    var clusterBySpec = Option.empty[ClusterBySpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, bucketCols, sortCols) =>
        bucketSpec = Some(BucketSpec(
          numBuckets, bucketCols.map(_.fieldNames.head), sortCols.map(_.fieldNames.head)))
      case TempClusterByTransform(columnNames) =>
        if (clusterBySpec.nonEmpty) {
          // Parser guarantees that it only passes down one TempClusterByTransform.
          throw SparkException.internalError("Cannot have multiple cluster by transforms.")
        }
        clusterBySpec = Some(ClusterBySpec(columnNames))

      case _ =>
        throw DeltaErrors.operationNotSupportedException(s"Partitioning by expressions")
    }
    // Parser guarantees that partition and cluster by can't both exist.
    assert(!(identityCols.toSeq.nonEmpty && clusterBySpec.nonEmpty))
    // Parser guarantees that bucketing and cluster by can't both exist.
    assert(!(bucketSpec.nonEmpty && clusterBySpec.nonEmpty))

    (identityCols.toSeq, bucketSpec, clusterBySpec)
  }
}