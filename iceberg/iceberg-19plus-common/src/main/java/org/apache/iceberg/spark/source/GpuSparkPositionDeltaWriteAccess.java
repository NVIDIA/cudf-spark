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

package org.apache.iceberg.spark.source;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;

/** Access to position-delta batch-write internals shared by Iceberg 1.9 and later. */
public final class GpuSparkPositionDeltaWriteAccess {
  private static final ClassValue<Method> BROADCAST_REWRITABLE_DELETES_METHOD =
      new ClassValue<Method>() {
        @Override
        protected Method computeValue(Class<?> type) {
          Method method = findMethod(type, "broadcastRewritableDeletes");
          method.setAccessible(true);
          return method;
        }
      };

  private GpuSparkPositionDeltaWriteAccess() {
  }

  /**
   * Returns the delete files that Iceberg's CPU batch write selected for replacement.
   *
   * <p>Iceberg keeps {@code broadcastRewritableDeletes()} private on its position-delta batch
   * writer, but the GPU deletion-vector writer must use the same selection when merging an
   * existing deletion vector. Package placement cannot access a private member, so this helper
   * uses reflection and caches the resolved method per runtime class and class loader.
   *
   * @return the broadcast delete-file map, or {@code null} when there are no existing deletes
   *     to rewrite, such as the first deletion-vector write for a data file
   */
  @SuppressWarnings("unchecked")
  public static Broadcast<Map<String, DeleteFileSet>> broadcastRewritableDeletes(
      DeltaBatchWrite write) {
    try {
      Method method = BROADCAST_REWRITABLE_DELETES_METHOD.get(write.getClass());
      return (Broadcast<Map<String, DeleteFileSet>>) method.invoke(write);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Unable to broadcast rewritable deletes from " + write.getClass().getName(), e);
    }
  }

  private static Method findMethod(Class<?> targetClass, String methodName) {
    Class<?> current = targetClass;
    while (current != null) {
      try {
        return current.getDeclaredMethod(methodName);
      } catch (NoSuchMethodException e) {
        current = current.getSuperclass();
      }
    }
    throw new IllegalStateException("No method " + methodName + " in " + targetClass.getName());
  }
}
