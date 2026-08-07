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

package org.apache.iceberg;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.apache.iceberg.catalog.Catalog;

/** Package-local access to Iceberg catalog properties used when building tables. */
public final class GpuCatalogPropertiesAccess {
  private GpuCatalogPropertiesAccess() {
  }

  /**
   * Returns the initialized catalog properties, including table defaults and overrides.
   *
   * <p>Most Iceberg catalogs inherit a protected {@code properties()} implementation from
   * {@link BaseMetastoreCatalog}. Catalogs such as RESTCatalog expose the same method publicly,
   * so use that public contract as a fallback without depending on a particular catalog type.
   */
  public static Map<String, String> properties(Catalog catalog) {
    if (catalog instanceof CachingCatalog) {
      try {
        Field delegate = CachingCatalog.class.getDeclaredField("catalog");
        delegate.setAccessible(true);
        return properties((Catalog) delegate.get(catalog));
      } catch (NoSuchFieldException | IllegalAccessException e) {
        return Collections.emptyMap();
      }
    }

    if (catalog instanceof BaseMetastoreCatalog) {
      return ((BaseMetastoreCatalog) catalog).properties();
    }

    try {
      Method properties = catalog.getClass().getMethod("properties");
      @SuppressWarnings("unchecked")
      Map<String, String> result = (Map<String, String>) properties.invoke(catalog);
      return result;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return Collections.emptyMap();
    }
  }
}
