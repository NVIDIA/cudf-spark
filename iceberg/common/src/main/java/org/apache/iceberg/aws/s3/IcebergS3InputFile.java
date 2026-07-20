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

package org.apache.iceberg.aws.s3;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.IcebergS3RangeCopier;
import com.nvidia.spark.rapids.IcebergS3RangeCopier.IcebergS3Client;
import com.nvidia.spark.rapids.fileio.RapidsInputFiles;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.GpuTaskMetrics$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.OptionalLong;

/**
 * S3-backed {@link RapidsInputFile} that delegates byte-range reads to
 * {@link IcebergS3RangeCopier}. The supplied {@link FileIO} is only used for
 * its property map and any per-prefix storage-credential overlays.
 *
 * <p>The package-private S3 file access is isolated in {@link IcebergS3InputFileAccess}.
 */
public final class IcebergS3InputFile implements RapidsInputFile {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergS3InputFile.class);

  private final IcebergInputFile delegate;
  private final URI s3Uri;
  private final IcebergS3Client icebergS3Client;

  private IcebergS3InputFile(
      IcebergInputFile delegate, URI s3Uri, IcebergS3Client icebergS3Client) {
    this.delegate = delegate;
    this.s3Uri = s3Uri;
    this.icebergS3Client = icebergS3Client;
  }

  public static RapidsInputFile maybeCreate(InputFile inputFile, FileIO fileIO) {
    // When the gating conf is off (or the file is not an S3 file), return the
    // default IcebergInputFile so the standard Iceberg SeekableInputStream path is used.
    IcebergInputFile delegate = new IcebergInputFile(inputFile);
    if (!RapidsInputFiles.isS3PerfEnabled()) {
      return delegate;
    }
    URI s3Uri = IcebergS3InputFileAccess.s3Uri(inputFile);
    if (s3Uri == null) {
      return delegate;
    }
    // Iceberg < 1.7 does not have SupportsStorageCredentials; ShimUtils returns
    // the per-prefix credential overlays (or an empty map on 1.6).
    IcebergS3Client icebergS3Client = IcebergS3RangeCopier.resolveClient(
        s3Uri.toString(),
        fileIO.properties(),
        ShimUtils.storageCredentialOverlays(fileIO));
    if (icebergS3Client == null) {
      if (TaskContext.get() != null) {
        GpuTaskMetrics$.MODULE$.get().recordPerfioS3IcebergFallback();
      }
      LOG.debug("IcebergS3RangeCopier path disabled for {}", s3Uri);
      return delegate;
    }
    LOG.debug("IcebergS3RangeCopier path active for {}", s3Uri);
    return new IcebergS3InputFile(delegate, s3Uri, icebergS3Client);
  }

  @Override
  public String path() {
    return delegate.path();
  }

  @Override
  public long getLength() throws IOException {
    return delegate.getLength();
  }

  @Override
  public OptionalLong getLastModificationTime() throws IOException {
    return delegate.getLastModificationTime();
  }

  @Override
  public SeekableInputStream open() throws IOException {
    return delegate.open();
  }

  /**
   * Returns the underlying Iceberg {@link InputFile}, matching
   * {@link IcebergInputFile#getDelegate()} for use by iceberg-internal
   * code paths that need direct access to the iceberg API.
   */
  public InputFile getDelegate() {
    return delegate.getDelegate();
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    IcebergS3RangeCopier.copyToHMB(icebergS3Client, output, s3Uri, copyRanges);
  }

  /**
   * Issue a single suffix-range {@code GetObject} ({@code Range: bytes=-N}) for
   * the last {@code length} bytes. Avoids the {@code getLength()} round-trip the
   * default {@link RapidsInputFile#readTail} would make.
   */
  @Override
  public void readTail(long length, HostMemoryBuffer output) throws IOException {
    if (length == 0) {
      return;
    }
    if (length < 0) {
      throw new IllegalArgumentException("length must be non-negative");
    }
    IcebergS3RangeCopier.copyTailToHMB(icebergS3Client, output, s3Uri, length, /*dstOffset*/ 0L);
  }
}
