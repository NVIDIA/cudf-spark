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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.NativeDepUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Command-line utilities for a RAPIDS Accelerator for Apache Spark distribution JAR. */
public final class SparkRapidsJarTool {
  private static final String RAPIDS_BUILD_INFO = "rapids4spark-version-info.properties";
  private static final String PRIVATE_BUILD_INFO =
      "cudf-spark-private-version-info.properties";
  private static final String JNI_BUILD_INFO = "cudf-spark-jni-version-info.properties";
  private static final String LEGACY_JNI_BUILD_INFO = "spark-rapids-jni-version-info.properties";
  private static final String CUDF_BUILD_INFO = "cudf-java-version-info.properties";
  private static final String SHIM_SERVICES =
      "META-INF/services/com.nvidia.spark.rapids.SparkShimServiceProvider";
  private static final String SHIM_PACKAGE_MARKER = ".shims.";
  private static final String CHUNK_MANIFEST_SUFFIX = ".chunks.properties";
  private static final String NATIVE_DEP_UTIL = "ai.rapids.cudf.NativeDepUtil";
  private static final String NATIVE_DEP_UTIL_CLASS =
      "ai/rapids/cudf/NativeDepUtil.class";
  private static final Pattern SPARK_SHIM = Pattern.compile("spark(\\d)(\\d)(\\d)(.*)");
  private static final int COPY_BUFFER_SIZE = 128 * 1024;

  private SparkRapidsJarTool() {
  }

  /** Run the distribution JAR utility. */
  public static void main(String[] args) {
    int exitCode;
    try {
      exitCode = run(args, System.out, System.err);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      exitCode = 1;
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(String[] args, PrintStream out, PrintStream err) throws IOException {
    if (args.length == 0 || (args.length == 1 && "info".equals(args[0]))) {
      printBuildInfo(out);
      return 0;
    }
    if (args.length == 1 && ("help".equals(args[0]) || "--help".equals(args[0]) ||
        "-h".equals(args[0]))) {
      printUsage(out, nativeExtractorAvailable());
      return 0;
    }
    if (args.length == 1 && "checksum".equals(args[0])) {
      try (JarFile jar = new JarFile(getJarPath())) {
        out.println(payloadSha256(jar));
      }
      return 0;
    }
    if (args.length == 3 && "extract".equals(args[0])) {
      extractNativeDependency(args);
      return 0;
    }

    err.println("Invalid arguments");
    printUsage(err, nativeExtractorAvailable());
    return 2;
  }

  private static void extractNativeDependency(String[] args) throws IOException {
    NativeDepUtil.main(args);
  }

  private static void printBuildInfo(PrintStream out) throws IOException {
    File jarPath = getJarPath();
    try (JarFile jar = new JarFile(jarPath)) {
      out.println("RAPIDS Accelerator for Apache Spark distribution");
      out.println("JAR: " + jarPath.getAbsolutePath());
      out.println("Compute the payload SHA-256 with:");
      out.println("  java -jar <dist.jar> checksum");
      out.println();

      printProperties(jar, RAPIDS_BUILD_INFO, "RAPIDS Accelerator for Apache Spark", out, true);
      printProperties(jar, CUDF_BUILD_INFO, "cuDF Java", out, true);
      if (!printProperties(jar, JNI_BUILD_INFO, "cuDF Spark JNI", out, false)) {
        printProperties(jar, LEGACY_JNI_BUILD_INFO, "cuDF Spark JNI", out, true);
      }

      Set<String> shims = findSparkShims(jar);
      out.println("Packaged Spark versions:");
      if (shims.isEmpty()) {
        out.println("  (none found)");
      } else {
        for (String shim : shims) {
          out.println("  " + formatSparkShim(shim));
        }
      }
      out.println();

      printProperties(jar, PRIVATE_BUILD_INFO, "cuDF Spark Private", out, true);

      printNativeLibraries(jar, out);
    }
  }

  /**
   * Compute a reproducible checksum of the distribution payload.
   *
   * <p>ZIP ordering, compression, timestamps, signatures, manifests, Maven metadata, and
   * build-info properties are intentionally ignored. This allows artifacts published under old
   * and new product coordinates to compare equal when their actual packaged payload is equal.</p>
   */
  static String payloadSha256(JarFile jar) throws IOException {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("SHA-256 is not available", e);
    }

    Set<String> names = new TreeSet<>();
    Enumeration<JarEntry> entries = jar.entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      if (!entry.isDirectory() && includeInPayloadChecksum(entry.getName()) &&
          !names.add(entry.getName())) {
        throw new IOException("Duplicate payload entry: " + entry.getName());
      }
    }

    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    for (String name : names) {
      byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
      updateInt(digest, nameBytes.length);
      digest.update(nameBytes);

      JarEntry entry = jar.getJarEntry(name);
      updateLong(digest, entry.getSize());
      try (InputStream in = jar.getInputStream(entry)) {
        int count;
        while ((count = in.read(buffer)) != -1) {
          digest.update(buffer, 0, count);
        }
      }
    }
    return toHex(digest.digest());
  }

  private static boolean includeInPayloadChecksum(String name) {
    String lowerName = name.toLowerCase(Locale.ROOT);
    String upperName = name.toUpperCase(Locale.ROOT);
    int slash = name.lastIndexOf('/');
    String fileName = slash < 0 ? lowerName : lowerName.substring(slash + 1);
    return !lowerName.startsWith("meta-inf/maven/") &&
        !lowerName.contains("/meta-inf/maven/") &&
        !upperName.equals("META-INF/MANIFEST.MF") &&
        !upperName.endsWith("/META-INF/MANIFEST.MF") &&
        !isSignatureFile(upperName) &&
        !fileName.endsWith("version-info.properties");
  }

  private static boolean isSignatureFile(String upperName) {
    int slash = upperName.lastIndexOf('/');
    String directory = slash < 0 ? "" : upperName.substring(0, slash + 1);
    String fileName = slash < 0 ? upperName : upperName.substring(slash + 1);
    return directory.endsWith("META-INF/") &&
        (fileName.endsWith(".SF") || fileName.endsWith(".RSA") ||
            fileName.endsWith(".DSA") || fileName.endsWith(".EC"));
  }

  private static void updateInt(MessageDigest digest, int value) {
    digest.update((byte) (value >>> 24));
    digest.update((byte) (value >>> 16));
    digest.update((byte) (value >>> 8));
    digest.update((byte) value);
  }

  private static void updateLong(MessageDigest digest, long value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      digest.update((byte) (value >>> shift));
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder result = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      result.append(String.format(Locale.ROOT, "%02x", value & 0xff));
    }
    return result.toString();
  }

  private static File getJarPath() throws IOException {
    CodeSource codeSource = SparkRapidsJarTool.class.getProtectionDomain().getCodeSource();
    URL location = codeSource == null ? null : codeSource.getLocation();
    if (location == null || !"file".equals(location.getProtocol())) {
      throw new IOException("Could not locate the distribution JAR");
    }
    try {
      File path = Paths.get(location.toURI()).toFile();
      if (!path.isFile()) {
        throw new IOException("This command must be run from a packaged distribution JAR");
      }
      return path;
    } catch (URISyntaxException e) {
      throw new IOException("Invalid distribution JAR location: " + location, e);
    }
  }

  private static boolean printProperties(JarFile jar, String resource, String heading,
      PrintStream out, boolean required) throws IOException {
    JarEntry entry = jar.getJarEntry(resource);
    if (entry == null) {
      if (required) {
        throw new IOException("Required build information not found: " + resource);
      }
      return false;
    }

    Properties properties = new Properties();
    try (InputStream in = jar.getInputStream(entry)) {
      properties.load(in);
    }
    List<String> names = new ArrayList<>(properties.stringPropertyNames());
    Collections.sort(names);
    out.println(heading + ":");
    for (String name : names) {
      out.println("  " + name + ": " + properties.getProperty(name));
    }
    out.println();
    return true;
  }

  private static Set<String> findSparkShims(JarFile jar) throws IOException {
    Set<String> shims = new TreeSet<>();
    JarEntry services = jar.getJarEntry(SHIM_SERVICES);
    if (services != null) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(
          jar.getInputStream(services), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          int marker = line.indexOf(SHIM_PACKAGE_MARKER);
          if (marker >= 0) {
            int start = marker + SHIM_PACKAGE_MARKER.length();
            int end = line.indexOf('.', start);
            if (end > start) {
              shims.add(line.substring(start, end));
            }
          }
        }
      }
    }
    return shims;
  }

  private static String formatSparkShim(String shim) {
    Matcher matcher = SPARK_SHIM.matcher(shim);
    if (!matcher.matches()) {
      return shim;
    }
    String version = matcher.group(1) + "." + matcher.group(2) + "." + matcher.group(3);
    String suffix = matcher.group(4);
    if (suffix.startsWith("db") && suffix.length() > 2) {
      version += "-databricks-" + suffix.substring(2);
    } else if (!suffix.isEmpty()) {
      version += "-" + suffix;
    }
    return version + " (" + shim + ")";
  }

  private static boolean nativeExtractorAvailable() throws IOException {
    try (JarFile jar = new JarFile(getJarPath())) {
      return nativeExtractorAvailable(jar);
    }
  }

  private static boolean nativeExtractorAvailable(JarFile jar) {
    return jar.getJarEntry(NATIVE_DEP_UTIL_CLASS) != null;
  }

  private static void printNativeLibraries(JarFile jar, PrintStream out) {
    String os = System.getProperty("os.name");
    String arch = System.getProperty("os.arch");
    String resourcePrefix = arch + "/" + os + "/";
    String mappedExample = System.mapLibraryName("example");
    int exampleIndex = mappedExample.indexOf("example");
    String libraryPrefix = mappedExample.substring(0, exampleIndex);
    String librarySuffix = mappedExample.substring(exampleIndex + "example".length());
    Set<String> libraries = new TreeSet<>();

    Enumeration<JarEntry> entries = jar.entries();
    while (entries.hasMoreElements()) {
      String name = entries.nextElement().getName();
      if (!name.startsWith(resourcePrefix)) {
        continue;
      }
      String fileName = name.substring(resourcePrefix.length());
      if (fileName.endsWith(CHUNK_MANIFEST_SUFFIX)) {
        fileName = fileName.substring(0, fileName.length() - CHUNK_MANIFEST_SUFFIX.length());
      }
      if (!fileName.contains("/") && fileName.startsWith(libraryPrefix) &&
          fileName.endsWith(librarySuffix)) {
        libraries.add(fileName.substring(libraryPrefix.length(),
            fileName.length() - librarySuffix.length()));
      }
    }

    boolean extractionAvailable = nativeExtractorAvailable(jar);
    out.println((extractionAvailable ? "Extractable" : "Packaged") +
        " native libraries (" + arch + "/" + os + "):");
    if (libraries.isEmpty()) {
      out.println("  (none found)");
    } else {
      for (String library : libraries) {
        out.println("  " + library);
      }
      if (extractionAvailable) {
        out.println();
        out.println("Extract one with:");
        out.println("  java -jar <dist.jar> extract <library-name> <destination>");
      } else {
        out.println();
        out.println("Native extraction unavailable: missing " + NATIVE_DEP_UTIL);
      }
    }
  }

  private static void printUsage(PrintStream out, boolean extractionAvailable) {
    out.println("Usage:");
    out.println("  java -jar <dist.jar> [info]");
    out.println("  java -jar <dist.jar> checksum");
    if (extractionAvailable) {
      out.println("  java -jar <dist.jar> extract <library-name> <destination>");
    }
    out.println("  java -jar <dist.jar> help");
  }
}
