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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.GpuTaskMetrics$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * S3-backed {@link RapidsInputFile} that delegates byte-range reads to
 * {@link IcebergS3RangeCopier}. The supplied {@link FileIO} is only used for
 * its property map and any per-prefix storage-credential overlays.
 *
 * <p>The package-private S3 file access is isolated in {@link IcebergS3InputFileAccess}.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergS3InputFile.class);

  private final String s3Bucket;
  private final String s3Key;
  private final IcebergS3Client icebergS3Client;

  private IcebergS3InputFile(
      InputFile delegate,
      String s3Bucket,
      String s3Key,
      IcebergS3Client icebergS3Client) {
    super(delegate);
    this.s3Bucket = s3Bucket;
    this.s3Key = s3Key;
    this.icebergS3Client = icebergS3Client;
  }

  public static IcebergInputFile maybeCreate(InputFile inputFile, FileIO fileIO) {
    // When the gating conf is off (or the file is not an S3 file), return the
    // default IcebergInputFile so the standard Iceberg SeekableInputStream path is used.
    IcebergInputFile delegate = new IcebergInputFile(inputFile);
    if (!RapidsInputFiles.isS3PerfEnabled()) {
      return delegate;
    }
    String[] s3BucketAndKey = IcebergS3InputFileAccess.s3BucketAndKey(inputFile);
    if (s3BucketAndKey == null) {
      return delegate;
    }
    String s3Bucket = s3BucketAndKey[0];
    String s3Key = s3BucketAndKey[1];
    // Iceberg < 1.7 does not have SupportsStorageCredentials; ShimUtils returns
    // the per-prefix credential overlays (or an empty map on 1.6).
    IcebergS3Client icebergS3Client = IcebergS3RangeCopier.resolveClient(
        inputFile.location(),
        fileIO.properties(),
        ShimUtils.storageCredentialOverlays(fileIO));
    if (icebergS3Client == null) {
      if (TaskContext.get() != null) {
        GpuTaskMetrics$.MODULE$.get().recordPerfioS3IcebergFallback();
      }
      LOG.debug("IcebergS3RangeCopier path disabled for {}", inputFile.location());
      return delegate;
    }
    LOG.debug("IcebergS3RangeCopier path active for {}", inputFile.location());
    return new IcebergS3InputFile(inputFile, s3Bucket, s3Key, icebergS3Client);
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    IcebergS3RangeCopier.copyToHMB(
        icebergS3Client, output, s3Bucket, s3Key, copyRanges);
  }

  /**
   * Submit vectored S3 reads without occupying an Iceberg reader worker while the responses are
   * in flight. The future completes only after every response has finished writing to
   * {@code output}.
   */
  public CompletableFuture<Long> readVectoredAsync(
      HostMemoryBuffer output,
      List<CopyRange> copyRanges) {
    return IcebergS3RangeCopier.copyToHMBAsync(
        icebergS3Client, output, s3Bucket, s3Key, copyRanges);
  }

  /**
   * Submit vectored S3 reads and notify the caller after each request has populated its output
   * range. The aggregate future remains alive until all submitted requests and callbacks are
   * terminal, including on failure.
   */
  public CompletableFuture<Long> readVectoredAsync(
      HostMemoryBuffer output,
      List<CopyRange> copyRanges,
      Consumer<CopyRange> requestSucceeded) {
    return IcebergS3RangeCopier.copyToHMBAsync(
        icebergS3Client, output, s3Bucket, s3Key, copyRanges, requestSucceeded);
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
    IcebergS3RangeCopier.copyTailToHMB(
        icebergS3Client, output, s3Bucket, s3Key, length, /*dstOffset*/ 0L);
    LOG.debug(
        "PerfIO S3 Iceberg readTail suffix-range GET completed: uri=s3://{}/{}, "
            + "range=bytes=-{}",
        s3Bucket,
        s3Key,
        length);
  }

  /**
   * Submit a suffix-range S3 read without blocking a reader worker.
   *
   * @return a future containing the number of bytes S3 returned, which may be less than
   *     {@code length} when the object itself is shorter.
   */
  public CompletableFuture<Long> readTailAsync(
      long length,
      HostMemoryBuffer output,
      long outputOffset) {
    if (length < 0) {
      throw new IllegalArgumentException("length must be non-negative");
    }
    return IcebergS3RangeCopier.copyTailToHMBAsync(
        icebergS3Client, output, s3Bucket, s3Key, length, outputOffset);
  }
}
