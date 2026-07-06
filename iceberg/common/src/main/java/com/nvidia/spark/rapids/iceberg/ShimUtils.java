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

package com.nvidia.spark.rapids.iceberg;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.ShimLoader;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.spark.source.GpuSparkScan;
import org.apache.spark.sql.connector.read.Scan;

import java.io.IOException;
import java.util.Map;

/**
 * Dispatches to the correct version-specific Iceberg shim utilities based on the
 * Spark version and the iceberg-spark-runtime jar detected at runtime.
 *
 * <p>Version detection is centralized in {@link IcebergProvider#shimPackage()}.
 */
public class ShimUtils {
    private static final IcebergShimUtils IMPL = loadImpl();

    private static IcebergShimUtils loadImpl() {
        String implClass = IcebergProvider.shimPackage() + ".ShimUtilsImpl";
        try {
            return (IcebergShimUtils) ShimLoader.getShimClassLoader()
                    .loadClass(implClass).getConstructor().newInstance();
        } catch (Exception | LinkageError e) {
            throw new RuntimeException("Failed to load Iceberg ShimUtils: " + implClass, e);
        }
    }

    public static String locationOf(ContentFile<?> f) {
        return IMPL.locationOf(f);
    }

    public static Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema,
                                                    Table table) {
        return IMPL.constantsMap(task, readSchema, table);
    }

    public static Map<String, Map<String, String>> storageCredentialOverlays(FileIO fileIO) {
        return IMPL.storageCredentialOverlays(fileIO);
    }

    public static ParquetFileReader openParquetReader(
            IcebergInputFile inputFile,
            Path filePath,
            ParquetReadOptions options,
            scala.collection.immutable.Map<String, GpuMetric> metrics) throws IOException {
        return IMPL.openParquetReader(inputFile, filePath, options, metrics);
    }

    public static GpuSparkScan newCopyOnWriteScan(
            Scan cpuScan,
            RapidsConf rapidsConf,
            boolean queryUsesInputFile) {
        return IMPL.newCopyOnWriteScan(cpuScan, rapidsConf, queryUsesInputFile);
    }
}
