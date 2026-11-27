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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;

/**
 * Package-local access to Iceberg Spark scan classes.
 *
 * <p>Iceberg keeps SparkScan, SparkBatchQueryScan, SparkCopyOnWriteScan, SparkBatch, and
 * SparkInputPartition package-private. Keep direct references to those classes in this helper so
 * callers can use Spark's public scan interfaces.
 */
public final class GpuSparkScanAccess {
  private GpuSparkScanAccess() {
  }

  public static boolean supports(Scan scan) {
    return scan instanceof SparkBatchQueryScan || scan instanceof SparkCopyOnWriteScan;
  }

  public static boolean isBatchQueryScan(Scan scan) {
    return scan instanceof SparkBatchQueryScan;
  }

  public static boolean isCopyOnWriteScan(Scan scan) {
    return scan instanceof SparkCopyOnWriteScan;
  }

  public static boolean isMetadataScan(Scan scan) {
    return sparkScan(scan).table() instanceof BaseMetadataTable;
  }

  public static SparkReadConf readConf(Scan scan) {
    return readField(sparkScan(scan), SparkReadConf.class, "readConf");
  }

  public static Table table(Scan scan) {
    return sparkScan(scan).table();
  }

  public static String branch(Scan scan) {
    // Iceberg 1.10.x and earlier exposed a protected SparkScan.branch() method;
    // 1.11.x removed it but the concrete scan classes still carry a private
    // `branch` field (included in description()). Read the field directly so this
    // works across all supported Iceberg versions. Returns null when no branch is
    // set (or, defensively, when the field is absent).
    Object target = sparkScan(scan);
    Field f = findField(target.getClass(), "branch");
    if (f == null) {
      return null;
    }
    try {
      f.setAccessible(true);
      Object v = f.get(target);
      return v == null ? null : v.toString();
    } catch (IllegalAccessException | RuntimeException e) {
      // RuntimeException also covers InaccessibleObjectException (Java 9+ module
      // encapsulation) and SecurityException, so any access failure degrades to
      // null per the contract above rather than escaping from this display-only path.
      return null;
    }
  }

  public static boolean caseSensitive(Scan scan) {
    return sparkScan(scan).caseSensitive();
  }

  public static Schema expectedSchema(Scan scan) {
    // Iceberg 1.10.x: SparkScan.expectedSchema(); Iceberg 1.11.x renamed it to
    // SparkScan.projection().
    return invokeMethod(sparkScan(scan), Schema.class, "expectedSchema", "projection");
  }

  public static Statistics estimateStatistics(Scan scan) {
    return sparkScan(scan).estimateStatistics();
  }

  @SuppressWarnings("unchecked")
  public static List<Expression> filterExpressions(Scan scan) {
    // Iceberg 1.10.x: SparkScan.filterExpressions(); Iceberg 1.11.x renamed it to
    // SparkScan.filters().
    List<Expression> r = (List<Expression>)
        invokeMethod(sparkScan(scan), List.class, "filterExpressions", "filters");
    return r != null ? r : Collections.emptyList();
  }

  public static Types.StructType groupingKeyType(Scan scan) {
    return sparkScan(scan).groupingKeyType();
  }

  public static List<? extends ScanTaskGroup<?>> taskGroups(Scan scan) {
    return sparkScan(scan).taskGroups();
  }

  public static Batch toBatch(Scan scan) {
    return sparkScan(scan).toBatch();
  }

  /** The scan's planned tasks before grouping; empty for scans that do not plan tasks. */
  public static List<? extends ScanTask> tasks(Scan scan) {
    return scan instanceof SparkPartitioningAwareScan
        ? ((SparkPartitioningAwareScan<?>) scan).tasks()
        : Collections.emptyList();
  }

  /** Data bytes of {@code tasks}, excluding delete files (counted per piece by Iceberg). */
  public static long dataBytes(Iterable<? extends ScanTask> tasks) {
    long bytes = 0L;
    for (ScanTask task : tasks) {
      bytes += task.isFileScanTask() ? task.asFileScanTask().length() : task.sizeBytes();
    }
    return bytes;
  }

  /** Whether any of {@code tasks} is read with delete files. */
  public static boolean hasDeletes(Iterable<? extends ScanTask> tasks) {
    for (ScanTask task : tasks) {
      if (task.isFileScanTask() && !task.asFileScanTask().deletes().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /** The split size Iceberg groups this scan by, after its adaptive adjustment. */
  public static long splitSize(Scan scan) {
    return sparkScan(scan).adjustSplitSize(tasks(scan), readConf(scan).splitSize());
  }

  /** Whether the split size is set as a read option. */
  public static boolean hasExplicitSplitSize(Scan scan) {
    return readConf(scan).splitSizeOption() != null;
  }

  /** Iceberg's input partitions for the scan's tasks regrouped by {@code splitBytes}. */
  public static InputPartition[] planInputPartitions(Scan scan, Batch batch, long splitBytes) {
    SparkReadConf conf = readConf(scan);
    List<ScanTaskGroup<ScanTask>> groups = TableScanUtil.planTaskGroups(
        new ArrayList<ScanTask>(tasks(scan)),
        splitBytes,
        conf.splitLookback(),
        conf.splitOpenFileCost());
    return withTaskGroups(batch, groups).planInputPartitions();
  }

  /**
   * A copy of {@code batch} over {@code groups}. The constructor differs across Iceberg versions,
   * so each argument is taken from the batch's field of the same type.
   */
  private static Batch withTaskGroups(Batch batch, List<? extends ScanTaskGroup<?>> groups) {
    Class<?> batchClass = batch.getClass();
    Constructor<?>[] constructors = batchClass.getDeclaredConstructors();
    if (constructors.length != 1) {
      throw new IllegalStateException("Expected one constructor on " + batchClass.getName());
    }
    Class<?>[] types = constructors[0].getParameterTypes();
    Object[] args = new Object[types.length];
    for (int i = 0; i < types.length; i++) {
      if (types[i] == List.class) {
        args[i] = groups;
      } else {
        Field field = fieldOfType(batchClass, types[i]);
        try {
          field.setAccessible(true);
          args[i] = field.get(batch);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException("Unable to read " + field.getName(), e);
        }
      }
    }
    try {
      constructors[0].setAccessible(true);
      return (Batch) constructors[0].newInstance(args);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to rebuild " + batchClass.getName(), e);
    }
  }

  private static Field fieldOfType(Class<?> target, Class<?> type) {
    Field found = null;
    for (Field f : target.getDeclaredFields()) {
      if (f.getType() == type) {
        if (found != null) {
          throw new IllegalStateException(
              "More than one field of type " + type.getName() + " on " + target.getName());
        }
        found = f;
      }
    }
    if (found == null) {
      throw new IllegalStateException(
          "No field of type " + type.getName() + " on " + target.getName());
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  public static List<Expression> runtimeFilterExpressions(Scan scan) {
    // Iceberg 1.6.x / 1.9.x / 1.10.x: field "runtimeFilterExpressions" on
    // SparkBatchQueryScan. Iceberg 1.11.x: field "runtimeFilters" on the new parent
    // class SparkRuntimeFilterableScan.
    return (List<Expression>) readField(scan, List.class,
        "runtimeFilterExpressions", "runtimeFilters");
  }

  public static Schema expectedSchema(Batch batch) {
    // Iceberg 1.10.x: field "expectedSchema" on SparkBatch.
    // Iceberg 1.11.x: renamed to "projection".
    return readField(batch, Schema.class, "expectedSchema", "projection");
  }

  public static Table table(InputPartition partition) {
    return sparkInputPartition(partition).table();
  }

  public static boolean isCaseSensitive(InputPartition partition) {
    return sparkInputPartition(partition).isCaseSensitive();
  }

  public static ScanTaskGroup<ScanTask> taskGroup(InputPartition partition) {
    return sparkInputPartition(partition).taskGroup();
  }

  private static SparkScan sparkScan(Scan scan) {
    return (SparkScan) scan;
  }

  private static SparkInputPartition sparkInputPartition(InputPartition partition) {
    return (SparkInputPartition) partition;
  }

  /**
   * Read the first existing field from {@code fieldNames} on {@code target} (or its
   * superclasses). Used when a field was renamed across Iceberg versions — list the
   * candidate names in priority order. Throws if none of the names exist.
   */
  private static <T> T readField(Object target, Class<T> fieldType, String... fieldNames) {
    Field field = findField(target.getClass(), fieldNames);
    if (field == null) {
      throw new IllegalStateException(
          "None of fields " + String.join(",", fieldNames)
              + " exist on " + target.getClass().getName());
    }
    try {
      field.setAccessible(true);
      return fieldType.cast(field.get(target));
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to read " + field.getName() + " from " + target.getClass().getName(), e);
    }
  }

  private static Field findField(Class<?> targetClass, String... fieldNames) {
    Class<?> current = targetClass;
    while (current != null) {
      for (String fieldName : fieldNames) {
        try {
          return current.getDeclaredField(fieldName);
        } catch (NoSuchFieldException ignore) {
          // try the next name (or the superclass)
        }
      }
      current = current.getSuperclass();
    }
    return null;
  }

  /**
   * Invoke the first existing zero-arg method from {@code methodNames} on
   * {@code target} (or its superclasses). Used when a protected method was renamed or
   * removed across Iceberg versions — list the candidate names in priority order.
   * Returns {@code null} if none of the names exist (caller decides the fallback).
   */
  private static <T> T invokeMethod(Object target, Class<T> returnType, String... methodNames) {
    Class<?> current = target.getClass();
    while (current != null) {
      for (String methodName : methodNames) {
        try {
          Method m = current.getDeclaredMethod(methodName);
          m.setAccessible(true);
          Object result = m.invoke(target);
          return result == null ? null : returnType.cast(result);
        } catch (NoSuchMethodException ignore) {
          // try the next name (or the superclass)
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException(
              "Unable to invoke " + methodName + " on " + target.getClass().getName(), e);
        }
      }
      current = current.getSuperclass();
    }
    return null;
  }
}
