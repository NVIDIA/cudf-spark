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

package com.nvidia.spark.rapids.tests.delta;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Test-only S3 filesystem backed by local disk.
 *
 * <p>The OSS Unity Catalog connector fetches temporary storage credentials from the catalog
 * server and publishes them in the Hadoop configuration attached to a catalog table. Every
 * filesystem access validates those credentials before mapping the S3 path to local storage, so
 * a path-only DeltaLog fails while a catalog-aware DeltaLog succeeds.
 *
 * <p>The credentials arrive as plain configuration values because
 * {@code delta_lake_catalog_managed_test.py} sets {@code renewCredential.enabled} to false. With
 * renewal enabled Unity Catalog would instead publish a credential-provider class and a separate
 * set of {@code fs.s3a.init.*} keys, which would require the AWS SDK on the classpath.
 */
public class CredentialTestFileSystem extends RawLocalFileSystem {
  private static final String SCHEME = "s3:";
  private static final String EXPECTED_BUCKET = "test-bucket0";
  private static final String EXPECTED_ACCESS_KEY = "accessKey0";
  private static final String EXPECTED_SECRET_KEY = "secretKey0";
  private static final String EXPECTED_SESSION_TOKEN = "sessionToken0";

  // Same keys as org.apache.hadoop.fs.s3a.Constants. Keep the test helper independent of
  // hadoop-aws and the AWS SDK so it can live in the regular integration-test jar.
  private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String S3A_SESSION_TOKEN = "fs.s3a.session.token";
  private static final AtomicReference<String> FAIL_NEXT_CREATE_SUFFIX =
      new AtomicReference<>();

  /** Inject a one-shot failure for the next create whose path ends with {@code suffix}. */
  public static void failNextCreateEndingWith(String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      throw new IllegalArgumentException("The failure suffix must not be empty");
    }
    FAIL_NEXT_CREATE_SUFFIX.set(suffix);
  }

  /** Clear a pending failure so it cannot leak into another integration test. */
  public static void clearInjectedFailure() {
    FAIL_NEXT_CREATE_SUFFIX.set(null);
  }

  @Override
  protected void checkPath(Path path) {
    // Accept the synthetic s3 scheme even though RawLocalFileSystem normally accepts file paths.
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    Path localPath = toLocalPath(path);
    String failSuffix = FAIL_NEXT_CREATE_SUFFIX.get();
    if (failSuffix != null && path.toString().endsWith(failSuffix) &&
        FAIL_NEXT_CREATE_SUFFIX.compareAndSet(failSuffix, null)) {
      throw new IOException("Injected create failure for " + path);
    }
    return super.create(
        localPath, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if (!path.toString().startsWith(SCHEME)) {
      return super.getFileStatus(path);
    }
    Path localPath = toLocalPath(path);
    try {
      return restoreS3Path(path, super.getFileStatus(localPath));
    } catch (FileNotFoundException e) {
      // Delta's LogStore checks that _delta_log exists before listing it. S3 has no real
      // directories, so expose the not-yet-materialized log prefix as an empty directory.
      if ("_delta_log".equals(path.getName())) {
        return new FileStatus(0, true, 1, getDefaultBlockSize(path), 0, path);
      }
      throw e;
    }
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return super.open(toLocalPath(path));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FileStatus[] statuses;
    try {
      statuses = super.listStatus(toLocalPath(path));
    } catch (FileNotFoundException e) {
      // Object stores return an empty listing for a missing prefix.
      return new FileStatus[0];
    }
    FileStatus[] restored = new FileStatus[statuses.length];
    for (int index = 0; index < statuses.length; index++) {
      restored[index] = restoreS3Path(path, statuses[index]);
    }
    return restored;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    return super.mkdirs(toLocalPath(path), permission);
  }

  @Override
  public boolean rename(Path source, Path destination) throws IOException {
    return super.rename(toLocalPath(source), toLocalPath(destination));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    return super.delete(toLocalPath(path), recursive);
  }

  private Path toLocalPath(Path path) {
    checkCredentials(path);
    return new Path(path.toString().replaceAll(SCHEME + "//.*?/", "file:///"));
  }

  private FileStatus restoreS3Path(Path originalPath, FileStatus status) {
    String s3Prefix = SCHEME + "//" + originalPath.toUri().getHost();
    String restoredPath = status.getPath().toString().replace("file:", s3Prefix);
    return new FileStatus(
        status.getLen(),
        status.isDirectory(),
        status.getReplication(),
        status.getBlockSize(),
        status.getModificationTime(),
        new Path(restoredPath));
  }

  private void checkCredentials(Path path) {
    assertEquals(EXPECTED_BUCKET, path.toUri().getHost(), "S3 bucket");
    Configuration conf = getConf();
    assertEquals(EXPECTED_ACCESS_KEY, conf.get(S3A_ACCESS_KEY), "access key");
    assertEquals(EXPECTED_SECRET_KEY, conf.get(S3A_SECRET_KEY), "secret key");
    assertEquals(EXPECTED_SESSION_TOKEN, conf.get(S3A_SESSION_TOKEN), "session token");
  }

  private void assertEquals(String expected, Object actual, String fieldName) {
    if (!expected.equals(actual)) {
      throw new AssertionError(
          "Unexpected " + fieldName + ": expected " + expected + ", found " + actual);
    }
  }
}
