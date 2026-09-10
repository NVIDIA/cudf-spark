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

package org.apache.iceberg.spark;

import java.lang.reflect.Field;

import com.nvidia.spark.rapids.iceberg.spark.GpuSparkReadOptions$;
import com.nvidia.spark.rapids.iceberg.spark.GpuSparkSQLProperties$;

/** Package-local access to Iceberg Spark read configuration internals. */
public final class GpuSparkReadConfAccess {
  private GpuSparkReadConfAccess() {
  }

  public static boolean handleTimestampWithoutZone(SparkReadConf readConf) {
    SparkConfParser confParser = readField(readConf, "confParser", SparkConfParser.class);
    return confParser.booleanConf()
        .option(GpuSparkReadOptions$.MODULE$.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE())
        .sessionConf(GpuSparkSQLProperties$.MODULE$.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE())
        .defaultValue(GpuSparkSQLProperties$.MODULE$.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT())
        .parse();
  }

  private static <T> T readField(Object target, String fieldName, Class<T> fieldType) {
    try {
      Field field = findField(target.getClass(), fieldName);
      field.setAccessible(true);
      return fieldType.cast(field.get(target));
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to read " + fieldName + " from " + target.getClass().getName(), e);
    }
  }

  private static Field findField(Class<?> targetClass, String fieldName) {
    Class<?> current = targetClass;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new IllegalStateException("No field " + fieldName + " in " + targetClass.getName());
  }
}
