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

package com.nvidia.spark.rapids.iceberg.iceberg110x;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;
import com.nvidia.spark.rapids.iceberg.IcebergDeletionVector;
import com.nvidia.spark.rapids.iceberg.IcebergShimUtils;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.source.GpuSparkCopyOnWriteV1Scan;
import org.apache.iceberg.spark.source.GpuSparkScan;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Scan;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** Iceberg 1.10.x shim: uses {@code SparkUtil::internalToSpark} and a cache-aware footer path. */
public class ShimUtilsImpl implements IcebergShimUtils {
    @Override
    public int formatVersion(Table table) {
        return TableUtil.formatVersion(table);
    }

    @Override
    public String locationOf(ContentFile<?> f) {
        return f.location();
    }

    @Override
    public boolean isDeletionVector(DeleteFile deleteFile) {
        return deleteFile.format() == FileFormat.PUFFIN;
    }

    @Override
    public boolean isDeletionVectorFormat(FileFormat fileFormat) {
        return fileFormat == FileFormat.PUFFIN;
    }

    @Override
    public PartitioningWriter<PositionDelete<InternalRow>, DeleteWriteResult>
            newDeletionVectorWriter(
                    Table table, OutputFileFactory fileFactory, Object rewritableDeletes) {
        return new PartitioningDVWriter<>(
                fileFactory, previousDeleteLoader(table, rewritableDeletes));
    }

    @Override
    public WriteResult positionDeltaWriteResult(
            DataWriteResult dataResult, DeleteWriteResult deleteResult) {
        return WriteResult.builder()
                .addDataFiles(dataResult.dataFiles())
                .addDeleteFiles(deleteResult.deleteFiles())
                .addReferencedDataFiles(deleteResult.referencedDataFiles())
                .addRewrittenDeleteFiles(deleteResult.rewrittenDeleteFiles())
                .build();
    }

    @SuppressWarnings("unchecked")
    private Function<CharSequence, PositionDeleteIndex> previousDeleteLoader(
            Table table, Object rewritableDeletes) {
        if (rewritableDeletes == null) {
            return path -> null;
        }

        Map<String, DeleteFileSet> deleteFiles =
                (Map<String, DeleteFileSet>) rewritableDeletes;
        BaseDeleteLoader deleteLoader = new BaseDeleteLoader(
                deleteFile -> EncryptingFileIO.combine(table.io(), table.encryption())
                        .newInputFile(deleteFile));
        return path -> {
            DeleteFileSet files = deleteFiles.get(path.toString());
            return files != null ? deleteLoader.loadPositionDeletes(files, path) : null;
        };
    }

    @Override
    public IcebergDeletionVector readDeletionVector(
            DeleteFile deleteFile, RapidsInputFile inputFile, boolean validateCrc)
            throws IOException {
        return IcebergDeletionVector.read(
                inputFile, deleteFile.contentOffset(), deleteFile.contentSizeInBytes(),
                deleteFile.recordCount(), validateCrc);
    }

    @Override
    public Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema, Table table) {
        if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
            Types.StructType partitionType = Partitioning.partitionType(table);
            return PartitionUtil.constantsMap(task,
                    partitionType,
                    SparkUtil::internalToSpark);
        } else {
            return PartitionUtil.constantsMap(task, SparkUtil::internalToSpark);
        }
    }

    @Override
    public Map<String, Map<String, String>> storageCredentialOverlays(FileIO fileIO) {
        if (!(fileIO instanceof SupportsStorageCredentials)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> result = new HashMap<>();
        for (StorageCredential sc : ((SupportsStorageCredentials) fileIO).credentials()) {
            result.put(sc.prefix(), sc.config());
        }
        return result;
    }

    @Override
    public ParquetFileReader openParquetReader(
            IcebergInputFile inputFile,
            Path filePath,
            ParquetReadOptions options,
            scala.collection.immutable.Map<String, GpuMetric> metrics) throws IOException {
        return GpuParquetIOShim.openReader(inputFile, filePath, options, metrics);
    }

    @Override
    public GpuSparkScan newCopyOnWriteScan(
            Scan cpuScan,
            RapidsConf rapidsConf,
            boolean queryUsesInputFile) {
        return GpuSparkCopyOnWriteV1Scan.create(cpuScan, rapidsConf, queryUsesInputFile);
    }
}
