/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.data;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * This class is inspired by {@link org.apache.iceberg.data.FileHelpers}. This class is only
 * used in tests.
 *
 * We need to copy its code since the original class is in `iceberg-data`'s test module., which
 * is not included in `iceberg-spark-runtime`. If we add `iceberg-data`'s test module as a
 * dependency, we will need to introduce all other dependencies like `iceberg-parquet`.
 */
public class GpuFileHelpers {
  private static final String GENERIC_FILE_WRITER_FACTORY_CLASS =
      "org.apache.iceberg.data.GenericFileWriterFactory";
  private static final Method BUILDER_FOR;
  private static final Method EQUALITY_DELETE_ROW_SCHEMA;
  private static final Method EQUALITY_FIELD_IDS;
  private static final Method BUILD;

  static {
    try {
      Class<?> factoryClass = Class.forName(
          GENERIC_FILE_WRITER_FACTORY_CLASS, true, Table.class.getClassLoader());
      BUILDER_FOR = findMethod(factoryClass, "builderFor", Table.class);
      Class<?> builderClass = BUILDER_FOR.getReturnType();
      EQUALITY_DELETE_ROW_SCHEMA =
          findMethod(builderClass, "equalityDeleteRowSchema", Schema.class);
      EQUALITY_FIELD_IDS = findMethod(builderClass, "equalityFieldIds", int[].class);
      BUILD = findMethod(builderClass, "build");

      BUILDER_FOR.setAccessible(true);
      EQUALITY_DELETE_ROW_SCHEMA.setAccessible(true);
      EQUALITY_FIELD_IDS.setAccessible(true);
      BUILD.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static DeleteFile writeDeleteFile(
      Table table,
      OutputFile out,
      StructLike partition,
      List<Record> deletes,
      Schema deleteRowSchema)
      throws IOException {
    int[] equalityFieldIds =
        deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();
    FileWriterFactory<Record> factory =
        newWriterFactory(table, deleteRowSchema, equalityFieldIds);

    EqualityDeleteWriter<Record> writer =
        factory.newEqualityDeleteWriter(encrypt(out), table.spec(), partition);
    try (Closeable toClose = writer) {
      writer.write(deletes);
    }

    return writer.toDeleteFile();
  }

  private static EncryptedOutputFile encrypt(OutputFile out) {
    return EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY);
  }

  @SuppressWarnings("unchecked")
  private static FileWriterFactory<Record> newWriterFactory(
      Table table, Schema deleteRowSchema, int[] equalityFieldIds) throws IOException {
    try {
      // Iceberg 1.10 declares Builder package-private while 1.11 makes it public. Avoid static
      // linkage so this common class has identical bytecode in both version-specific modules.
      Object builder = BUILDER_FOR.invoke(null, table);
      builder = EQUALITY_DELETE_ROW_SCHEMA.invoke(builder, deleteRowSchema);
      builder = EQUALITY_FIELD_IDS.invoke(builder, (Object) equalityFieldIds);
      return (FileWriterFactory<Record>) BUILD.invoke(builder);
    } catch (ReflectiveOperationException e) {
      throw new IOException("Failed to create Iceberg file writer factory", e);
    }
  }

  private static Method findMethod(Class<?> type, String name, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Class<?> current = type;
    while (current != null) {
      try {
        return current.getDeclaredMethod(name, parameterTypes);
      } catch (NoSuchMethodException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchMethodException(type.getName() + "." + name);
  }
}
