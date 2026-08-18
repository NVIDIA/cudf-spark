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

import org.apache.iceberg.io.InputFile;

/**
 * Root-loadable bridge for package-private Iceberg S3 APIs.
 *
 * <p>The JVM defines a runtime package by both its package name and defining classloader.
 * When Iceberg runtime jars are supplied through {@code extraClassPath}, Iceberg classes such as
 * {@link BaseS3File} can be loaded by the app classloader while RAPIDS shim classes are loaded by
 * Spark's {@code MutableURLClassLoader}. Direct access from a shim class would therefore fail with
 * {@link IllegalAccessError}, despite both classes having the same Java package name.
 *
 * <p>This class must remain root-loadable via {@code unshimmed-common-from-single-shim.txt}, and
 * all access to {@link BaseS3File} and {@link S3URI} must remain isolated here. Moving that access
 * into {@link IcebergS3InputFile} will reintroduce the classloader access failure.
 */
public final class IcebergS3InputFileAccess {
  private IcebergS3InputFileAccess() {
  }

  /**
   * Returns the raw S3 bucket and key, in that order, or {@code null} for a non-S3 input file.
   *
   * <p>The values are intentionally returned without constructing a URI so reserved characters in
   * the object key are not percent-encoded or double-escaped before the S3 request is issued.
   */
  public static String[] s3BucketAndKey(InputFile inputFile) {
    if (!(inputFile instanceof BaseS3File)) {
      return null;
    }
    S3URI uri = ((BaseS3File) inputFile).uri();
    return new String[] {uri.bucket(), uri.key()};
  }
}
