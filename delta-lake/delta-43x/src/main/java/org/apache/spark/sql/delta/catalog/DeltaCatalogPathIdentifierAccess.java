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

package org.apache.spark.sql.delta.catalog;

import org.apache.spark.sql.connector.catalog.Identifier;

/** Java bridge for Delta's Scala-protected path identifier predicate. */
public final class DeltaCatalogPathIdentifierAccess {
  private DeltaCatalogPathIdentifierAccess() {}

  public static boolean isPathIdentifier(DeltaCatalog catalog, Identifier ident) {
    return catalog.isPathIdentifier(ident);
  }
}
