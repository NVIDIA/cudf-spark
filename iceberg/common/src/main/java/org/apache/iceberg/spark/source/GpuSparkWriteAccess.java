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

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * Package-local access to Iceberg Spark write classes.
 *
 * <p>Iceberg keeps SparkWrite and SparkPositionDeltaWrite package-private. Resolve those
 * classes from a conventional-root helper so runtime package access is checked in the same
 * class loader as Iceberg itself.
 */
public final class GpuSparkWriteAccess {
  private GpuSparkWriteAccess() {
  }

  public static boolean supports(Class<? extends Write> cpuClass) {
    return SparkWrite.class.isAssignableFrom(cpuClass)
        || SparkPositionDeltaWrite.class.isAssignableFrom(cpuClass);
  }

  public static String sparkWriteClassName() {
    return SparkWrite.class.getName();
  }

  public static Table table(Write write) {
    return readField(sparkWrite(write), "table", Table.class);
  }

  public static FileFormat format(Write write) {
    return readField(sparkWrite(write), "format", FileFormat.class);
  }

  public static JavaSparkContext sparkContext(Write write) {
    return readField(sparkWrite(write), "sparkContext", JavaSparkContext.class);
  }

  public static String queryId(Write write) {
    return readField(sparkWrite(write), "queryId", String.class);
  }

  public static int outputSpecId(Write write) {
    return readField(sparkWrite(write), "outputSpecId", Integer.class);
  }

  public static long targetFileSize(Write write) {
    return readField(sparkWrite(write), "targetFileSize", Long.class);
  }

  public static Schema writeSchema(Write write) {
    return readField(sparkWrite(write), "writeSchema", Schema.class);
  }

  public static StructType dsSchema(Write write) {
    return readField(sparkWrite(write), "dsSchema", StructType.class);
  }

  public static boolean useFanoutWriter(Write write) {
    return readField(sparkWrite(write), "useFanoutWriter", Boolean.class);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> writeProperties(Write write) {
    return readField(sparkWrite(write), "writeProperties", Map.class);
  }

  public static Table table(DeltaWrite write) {
    return readField(positionDeltaWrite(write), "table", Table.class);
  }

  public static JavaSparkContext sparkContext(DeltaWrite write) {
    return readField(positionDeltaWrite(write), "sparkContext", JavaSparkContext.class);
  }

  public static Command command(DeltaWrite write) {
    return readField(positionDeltaWrite(write), "command", Command.class);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> writeProperties(DeltaWrite write) {
    return readField(positionDeltaWrite(write), "writeProperties", Map.class);
  }

  public static Object context(DeltaWrite write) {
    return readField(positionDeltaWrite(write), "context", Object.class);
  }

  public static Schema contextDataSchema(Object context) {
    return readField(context, "dataSchema", Schema.class);
  }

  public static StructType contextDataSparkType(Object context) {
    return readField(context, "dataSparkType", StructType.class);
  }

  public static FileFormat contextDataFileFormat(Object context) {
    return readField(context, "dataFileFormat", FileFormat.class);
  }

  public static long contextTargetDataFileSize(Object context) {
    return readField(context, "targetDataFileSize", Long.class);
  }

  public static StructType contextDeleteSparkType(Object context) {
    return readField(context, "deleteSparkType", StructType.class);
  }

  public static StructType contextMetadataSparkType(Object context) {
    return readField(context, "metadataSparkType", StructType.class);
  }

  public static FileFormat contextDeleteFileFormat(Object context) {
    return readField(context, "deleteFileFormat", FileFormat.class);
  }

  public static long contextTargetDeleteFileSize(Object context) {
    return readField(context, "targetDeleteFileSize", Long.class);
  }

  public static DeleteGranularity contextDeleteGranularity(Object context) {
    return readField(context, "deleteGranularity", DeleteGranularity.class);
  }

  public static String contextQueryId(Object context) {
    return readField(context, "queryId", String.class);
  }

  public static boolean contextUseFanoutWriter(Object context) {
    return readField(context, "useFanoutWriter", Boolean.class);
  }

  public static boolean contextInputOrdered(Object context) {
    return readField(context, "inputOrdered", Boolean.class);
  }

  public static WriterCommitMessage taskCommit(DataFile[] files) {
    SparkWrite.TaskCommit commit = new SparkWrite.TaskCommit(files);
    commit.reportOutputMetrics();
    return commit;
  }

  public static WriterCommitMessage deltaTaskCommit(WriteResult result) {
    return new SparkPositionDeltaWrite.DeltaTaskCommit(result);
  }

  public static WriterCommitMessage deltaTaskCommit(DeleteWriteResult result) {
    return new SparkPositionDeltaWrite.DeltaTaskCommit(result);
  }

  private static SparkWrite sparkWrite(Write write) {
    return (SparkWrite) write;
  }

  private static SparkPositionDeltaWrite positionDeltaWrite(DeltaWrite write) {
    return (SparkPositionDeltaWrite) write;
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
