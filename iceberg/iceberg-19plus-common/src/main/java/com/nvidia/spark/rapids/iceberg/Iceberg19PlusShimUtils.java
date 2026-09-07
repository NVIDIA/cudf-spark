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

package com.nvidia.spark.rapids.iceberg;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.spark.source.GpuSparkPositionDeltaWriteAccess;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Shared deletion-vector shim implementation for Iceberg 1.9 and later. */
public abstract class Iceberg19PlusShimUtils implements IcebergShimUtils {
    @Override
    public boolean isDeletionVector(DeleteFile deleteFile) {
        return deleteFile.format() == FileFormat.PUFFIN;
    }

    @Override
    public boolean isPuffinFormat(FileFormat fileFormat) {
        return fileFormat == FileFormat.PUFFIN;
    }

    @Override
    public RewritableDeletes broadcastRewritableDeletes(DeltaBatchWrite write) {
        Broadcast<Map<String, DeleteFileSet>> rewritableDeletes =
                GpuSparkPositionDeltaWriteAccess.broadcastRewritableDeletes(write);
        return rewritableDeletes != null ? new RewritableDeletesImpl(rewritableDeletes) : null;
    }

    @Override
    public PartitioningWriter<PositionDelete<InternalRow>, DeleteWriteResult>
            newDeletionVectorWriter(
                    Table table, OutputFileFactory fileFactory,
                    RewritableDeletes rewritableDeletes) {
        Map<String, DeleteFileSet> deleteFiles = rewritableDeletes == null
                ? null
                : ((RewritableDeletesImpl) rewritableDeletes).value();
        return new PartitioningDVWriter<>(
                fileFactory, previousDeleteLoader(table, deleteFiles));
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

    @Override
    public void setPositionDelete(
            PositionDelete<InternalRow> delete, CharSequence path, long position) {
        delete.set(path, position);
    }

    private static Function<CharSequence, PositionDeleteIndex> previousDeleteLoader(
            Table table, Map<String, DeleteFileSet> rewritableDeletes) {
        if (rewritableDeletes == null) {
            return path -> null;
        }

        BaseDeleteLoader deleteLoader = new BaseDeleteLoader(
                deleteFile -> EncryptingFileIO.combine(table.io(), table.encryption())
                        .newInputFile(deleteFile));
        return path -> {
            Set<DeleteFile> files = rewritableDeletes.get(path.toString());
            return files != null ? deleteLoader.loadPositionDeletes(files, path) : null;
        };
    }

    private static final class RewritableDeletesImpl implements RewritableDeletes {
        private final Broadcast<Map<String, DeleteFileSet>> delegate;

        private RewritableDeletesImpl(Broadcast<Map<String, DeleteFileSet>> delegate) {
            this.delegate = delegate;
        }

        private Map<String, DeleteFileSet> value() {
            return delegate.value();
        }
    }
}
