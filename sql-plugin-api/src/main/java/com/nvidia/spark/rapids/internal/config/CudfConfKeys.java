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

package com.nvidia.spark.rapids.internal.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Internal utilities for resolving the canonical and legacy cuDF plugin configuration namespaces.
 *
 * <p>{@code spark.cudf.*} is canonical. {@code spark.rapids.*} remains a compatibility alias,
 * and the canonical value wins when both forms of the same key are present.</p>
 */
public final class CudfConfKeys {
  public static final String CANONICAL_PREFIX = "spark.cudf.";
  public static final String LEGACY_PREFIX = "spark.rapids.";

  private CudfConfKeys() {}

  public static boolean isCanonical(String key) {
    return key.startsWith(CANONICAL_PREFIX);
  }

  public static boolean isLegacy(String key) {
    return key.startsWith(LEGACY_PREFIX);
  }

  /** Return the canonical name for a plugin configuration key. */
  public static String canonicalKey(String key) {
    Objects.requireNonNull(key, "key");
    if (isLegacy(key)) {
      return CANONICAL_PREFIX + key.substring(LEGACY_PREFIX.length());
    }
    return key;
  }

  /** Return the legacy alias for a canonical or legacy plugin configuration key. */
  public static String legacyKey(String key) {
    String canonical = canonicalKey(key);
    if (isCanonical(canonical)) {
      return LEGACY_PREFIX + canonical.substring(CANONICAL_PREFIX.length());
    }
    return canonical;
  }

  /**
   * Resolve a key from a map. The canonical value takes precedence over its legacy alias.
   * Returns {@code null} when neither name is present.
   */
  public static String get(Map<String, String> conf, String key) {
    String canonical = canonicalKey(key);
    if (conf.containsKey(canonical)) {
      return conf.get(canonical);
    }
    return conf.get(legacyKey(canonical));
  }

  /** Return whether either name of a plugin configuration is present in a map. */
  public static boolean contains(Map<String, String> conf, String key) {
    String canonical = canonicalKey(key);
    return conf.containsKey(canonical) || conf.containsKey(legacyKey(canonical));
  }

  /**
   * Insert a value under its canonical name, retaining an existing canonical value when the
   * incoming key is the legacy alias. This makes the result independent of iteration order.
   */
  public static void putCanonical(
      Map<String, String> conf, String sourceKey, String value) {
    String canonical = canonicalKey(sourceKey);
    if (isCanonical(sourceKey) || !conf.containsKey(canonical)) {
      conf.put(canonical, value);
    }
  }

  /**
   * Copy a configuration map and materialize both aliases for every plugin configuration.
   * Non-plugin settings are copied unchanged.
   */
  public static Map<String, String> withAliases(Map<String, String> conf) {
    Map<String, String> canonicalValues = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      if (isCanonical(entry.getKey()) || isLegacy(entry.getKey())) {
        putCanonical(canonicalValues, entry.getKey(), entry.getValue());
      }
    }

    Map<String, String> result = new LinkedHashMap<>(conf);
    for (Map.Entry<String, String> entry : canonicalValues.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
      result.put(legacyKey(entry.getKey()), entry.getValue());
    }
    return result;
  }
}
