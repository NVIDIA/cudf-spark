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

package org.apache.spark.sql.delta.catalog

import java.util

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

/** Delta 4.3 accessors for catalog state that is intentionally package-private upstream. */
object DeltaCatalogRestApiShim {

  /**
   * Returns whether the initialized CPU catalog has an active Delta REST client. Inspecting the
   * client preserves Delta's own Unity Catalog detection, default-enable rule, and initialization
   * behavior instead of duplicating those rules in the RAPIDS plugin.
   */
  def isRestApiEnabled(catalog: DeltaCatalog): Boolean = catalog.deltaCatalogClient.nonEmpty

  /** Mirrors Delta's side-effect-free routing gate for CREATE and CTAS. */
  def shouldRouteCreate(
      catalog: DeltaCatalog,
      ident: Identifier,
      properties: util.Map[String, String],
      spark: SparkSession): Boolean = {
    isRestApiEnabled(catalog) &&
      !properties.containsKey(TableCatalog.PROP_LOCATION) &&
      !properties.containsKey(TableCatalog.PROP_EXTERNAL) &&
      !isPathIdentifier(ident, spark)
  }

  /**
   * Keeps REPLACE and RTAS on CPU when Delta will either route through REST or reject properties
   * that are invalid on its REST path before creating a staged table.
   */
  def shouldRouteOrValidateReplace(
      catalog: DeltaCatalog,
      ident: Identifier,
      properties: util.Map[String, String],
      spark: SparkSession): Boolean = {
    isRestApiEnabled(catalog) &&
      (properties.containsKey(TableCatalog.PROP_LOCATION) ||
        properties.containsKey(TableCatalog.PROP_EXTERNAL) ||
        !isPathIdentifier(ident, spark))
  }

  /** Matches Delta's SupportsPathIdentifier predicate without invoking catalog I/O. */
  private def isPathIdentifier(ident: Identifier, spark: SparkSession): Boolean = {
    try {
      spark.sessionState.conf.runSQLonFile &&
        ident.namespace().length == 1 &&
        DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head) &&
        new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }
}
