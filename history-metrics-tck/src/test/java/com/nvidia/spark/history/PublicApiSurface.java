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
package com.nvidia.spark.history;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Compiled-reflection assertions for exact public contract surfaces. */
final class PublicApiSurface {
  private PublicApiSurface() {
  }

  static void assertMethods(Class<?> type, String... expected) {
    Set<String> actual = new HashSet<String>();
    for (Method method : type.getDeclaredMethods()) {
      if (Modifier.isPublic(method.getModifiers()) && !method.isSynthetic()) {
        actual.add(signature(method.getName(), method.getParameterTypes()) +
            ":" + method.getReturnType().getName());
      }
    }
    assertEquals(new HashSet<String>(Arrays.asList(expected)), actual);
  }

  static void assertConstructors(Class<?> type, String... expected) {
    Set<String> actual = new HashSet<String>();
    for (Constructor<?> constructor : type.getConstructors()) {
      if (!constructor.isSynthetic()) {
        actual.add(signature(type.getSimpleName(), constructor.getParameterTypes()));
      }
    }
    assertEquals(new HashSet<String>(Arrays.asList(expected)), actual);
  }

  private static String signature(String name, Class<?>[] parameters) {
    StringBuilder value = new StringBuilder(name).append('(');
    for (int index = 0; index < parameters.length; index++) {
      if (index != 0) {
        value.append(',');
      }
      value.append(parameters[index].getName());
    }
    return value.append(')').toString();
  }
}
