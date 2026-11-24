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

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.delta.Snapshot

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
      properties: util.Map[String, String]): Boolean = {
    isRestApiEnabled(catalog) &&
      !properties.containsKey(TableCatalog.PROP_LOCATION) &&
      !properties.containsKey(TableCatalog.PROP_EXTERNAL) &&
      !DeltaCatalogPathIdentifierAccess.isPathIdentifier(catalog, ident)
  }

  /**
   * Mirrors Delta's routing gate for REPLACE, RTAS and CREATE OR REPLACE. Delta rejects a
   * location or external property on its REST path before staging, so those requests are not
   * routed and stay with the CPU catalog, which reports Delta's own error.
   */
  def shouldRouteReplace(
      catalog: DeltaCatalog,
      ident: Identifier,
      properties: util.Map[String, String]): Boolean = {
    isRestApiEnabled(catalog) &&
      !properties.containsKey(TableCatalog.PROP_LOCATION) &&
      !properties.containsKey(TableCatalog.PROP_EXTERNAL) &&
      !DeltaCatalogPathIdentifierAccess.isPathIdentifier(catalog, ident)
  }

  /** Prefix Delta puts on `delta.*` properties that a catalog-managed REPLACE carries forward. */
  def carryForwardPrefix: String = AbstractDeltaCatalogClient.CARRY_FORWARD_PREFIX

  /**
   * Mirrors Delta's create callback: publishes a committed catalog-managed table through the
   * Delta REST client together with the committed snapshot. Returns false, without side effects,
   * when Delta would not route the table through the client.
   */
  def createManagedTable(
      catalog: DeltaCatalog,
      ident: Identifier,
      table: CatalogTable,
      snapshot: Snapshot): Boolean = catalog.deltaCatalogClient match {
    case Some(client) if table.tableType == CatalogTableType.MANAGED =>
      client.createTable(
        ident,
        table,
        snapshot.metadata,
        snapshot.domainMetadata,
        snapshot.protocol,
        snapshot.timestamp)
      true
    case _ => false
  }
}
