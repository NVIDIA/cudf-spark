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

package io.unitycatalog.rapids;

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Minimal Unity Catalog-shaped delegate used to test Delta catalog initialization. */
public final class TestUnityCatalog implements CatalogPlugin {
  private String name;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }
}
