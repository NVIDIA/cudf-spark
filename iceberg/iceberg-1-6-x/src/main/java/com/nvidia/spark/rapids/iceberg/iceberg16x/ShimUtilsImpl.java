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

package com.nvidia.spark.rapids.iceberg.iceberg16x;

import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.iceberg.IcebergShimUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.source.GpuBaseReader;
import org.apache.iceberg.spark.source.GpuSparkCopyOnWriteV1Scan;
import org.apache.iceberg.spark.source.GpuSparkScan;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.sql.connector.read.Scan;

import java.util.Collections;
import java.util.Map;

/** Iceberg 1.6.x shim: uses {@code ContentFile.path()} and {@code GpuBaseReader::convertConstant}. */
public class ShimUtilsImpl implements IcebergShimUtils {
    @Override
    public String locationOf(ContentFile<?> f) {
        return f.path().toString();
    }

    @Override
    public Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema, Table table) {
        if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
            Types.StructType partitionType = Partitioning.partitionType(table);
            return PartitionUtil.constantsMap(task,
                    partitionType,
                    GpuBaseReader::convertConstant);
        } else {
            return PartitionUtil.constantsMap(task, GpuBaseReader::convertConstant);
        }
    }

    /** Iceberg 1.6.x predates {@code SupportsStorageCredentials} — no per-prefix overlays. */
    @Override
    public Map<String, Map<String, String>> storageCredentialOverlays(FileIO fileIO) {
        return Collections.emptyMap();
    }

    // openParquetReader: inherits the no-cache default from IcebergShimUtils. The shaded
    // ParquetFileReader in 1.6.x has no public API to inject pre-parsed footer metadata,
    // so file-cache routing is not possible here.

    @Override
    public GpuSparkScan newCopyOnWriteScan(
            Scan cpuScan,
            RapidsConf rapidsConf,
            boolean queryUsesInputFile) {
        return GpuSparkCopyOnWriteV1Scan.create(cpuScan, rapidsConf, queryUsesInputFile);
    }
}
