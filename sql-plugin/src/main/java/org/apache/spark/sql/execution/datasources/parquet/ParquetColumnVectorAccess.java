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

package org.apache.spark.sql.execution.datasources.parquet;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/** Access Spark's package-private vector even when the plugin has a separate classloader. */
public final class ParquetColumnVectorAccess {
  private static final Class<?> VECTOR_CLASS = loadVectorClass();
  private static final Method GET_COLUMN = method("getColumn");
  private static final Method GET_CHILDREN = method("getChildren");
  private static final Method GET_LEAVES = method("getLeaves");
  private static final Method GET_READER = method("getColumnReader");
  private static final Method SET_READER = method("setColumnReader", VectorizedColumnReader.class);
  private static final Method GET_VALUE = method("getValueVector");
  private static final Method GET_REPETITION = method("getRepetitionLevelVector");
  private static final Method GET_DEFINITION = method("getDefinitionLevelVector");
  private static final Method ASSEMBLE = method("assemble");
  private static final Method RESET = method("reset");

  private ParquetColumnVectorAccess() {
  }

  private static Class<?> loadVectorClass() {
    try {
      return Class.forName("org.apache.spark.sql.execution.datasources.parquet.ParquetColumnVector",
          false, ParquetColumn.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Spark ParquetColumnVector is missing", e);
    }
  }

  private static Method method(String name, Class<?>... types) {
    try {
      Method result = VECTOR_CLASS.getDeclaredMethod(name, types);
      result.setAccessible(true);
      return result;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Spark ParquetColumnVector." + name + " is missing", e);
    }
  }

  private static Object invoke(Method method, Object target, Object... args) {
    try {
      return method.invoke(target, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
      throw new IllegalStateException("Spark ParquetColumnVector." + method.getName() + " failed",
          cause);
    }
  }

  public static Object newVector(Class<?>[] types, Object... args) {
    try {
      Constructor<?> constructor = VECTOR_CLASS.getDeclaredConstructor(types);
      constructor.setAccessible(true);
      return constructor.newInstance(args);
    } catch (ReflectiveOperationException e) {
      Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
      throw new IllegalStateException("Spark ParquetColumnVector construction failed", cause);
    }
  }

  public static ParquetColumn getColumn(Object vector) {
    return (ParquetColumn) invoke(GET_COLUMN, vector);
  }

  @SuppressWarnings("unchecked")
  public static List<Object> getChildren(Object vector) {
    return (List<Object>) invoke(GET_CHILDREN, vector);
  }

  @SuppressWarnings("unchecked")
  public static List<Object> getLeaves(Object vector) {
    return (List<Object>) invoke(GET_LEAVES, vector);
  }

  public static VectorizedColumnReader getColumnReader(Object vector) {
    return (VectorizedColumnReader) invoke(GET_READER, vector);
  }

  public static void setColumnReader(Object vector, VectorizedColumnReader reader) {
    invoke(SET_READER, vector, reader);
  }

  public static WritableColumnVector getValueVector(Object vector) {
    return (WritableColumnVector) invoke(GET_VALUE, vector);
  }

  public static WritableColumnVector getRepetitionLevelVector(Object vector) {
    return (WritableColumnVector) invoke(GET_REPETITION, vector);
  }

  public static WritableColumnVector getDefinitionLevelVector(Object vector) {
    return (WritableColumnVector) invoke(GET_DEFINITION, vector);
  }

  public static void assemble(Object vector) {
    invoke(ASSEMBLE, vector);
  }

  public static void reset(Object vector) {
    invoke(RESET, vector);
  }
}
