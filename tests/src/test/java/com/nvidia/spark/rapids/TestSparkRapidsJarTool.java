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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;

public class TestSparkRapidsJarTool {
  @Test
  public void testPayloadChecksumIgnoresPublicationMetadata() throws IOException {
    withTempDir(tempDir -> {
      Path rapidsJar = tempDir.resolve("rapids-4-spark.jar");
      Path renamedJar = tempDir.resolve("cudf-plugin.jar");
      Path changedPayloadJar = tempDir.resolve("changed-payload.jar");

      Map<String, byte[]> rapidsEntries = entries(
          "Rapids-Plugin", "26.10.0-SNAPSHOT", new byte[] {1, 2, 3, 4});
      Map<String, byte[]> renamedEntries = entries(
          "cuDF-Plugin", "26.12.0-SNAPSHOT", new byte[] {1, 2, 3, 4});
      Map<String, byte[]> changedPayloadEntries = entries(
          "cuDF-Plugin", "26.12.0-SNAPSHOT", new byte[] {1, 2, 3, 5});

      writeJar(rapidsJar, rapidsEntries, false, 1_600_000_000_000L);
      writeJar(renamedJar, reverse(renamedEntries), true, 1_700_000_000_000L);
      writeJar(changedPayloadJar, changedPayloadEntries, false, 1_800_000_000_000L);

      assertEquals("Rapids-Plugin", title(rapidsJar));
      assertEquals("cuDF-Plugin", title(renamedJar));
      assertEquals(checksum(rapidsJar), checksum(renamedJar));
      assertNotEquals(checksum(rapidsJar), checksum(changedPayloadJar));
    });
  }

  @Test
  public void testDistributionTitleIsRequired() throws IOException {
    withTempDir(tempDir -> {
      Map<String, byte[]> missingManifest = entries(
          "unused", "26.10.0-SNAPSHOT", new byte[] {1});
      missingManifest.remove("META-INF/MANIFEST.MF");
      Path missingManifestJar = tempDir.resolve("missing-manifest.jar");
      writeJar(missingManifestJar, missingManifest, false, 1_600_000_000_000L);

      Path blankTitleJar = tempDir.resolve("blank-title.jar");
      writeJar(blankTitleJar,
          entries(" ", "26.10.0-SNAPSHOT", new byte[] {1}), false, 1_600_000_000_000L);

      assertThrows(IOException.class, () -> title(missingManifestJar));
      assertThrows(IOException.class, () -> title(blankTitleJar));
    });
  }

  @Test
  public void testPayloadChecksumIncludesNonSignatureEntries() throws IOException {
    withTempDir(tempDir -> {
      Map<String, byte[]> baseEntries = entries(
          "Rapids-Plugin", "26.10.0-SNAPSHOT", new byte[] {1, 2, 3, 4});
      Map<String, byte[]> invalidExtensionEntries = new LinkedHashMap<>(baseEntries);
      invalidExtensionEntries.put("META-INF/SIG-PLUGIN.CONFIG", new byte[] {1});
      Map<String, byte[]> misleadingDirectoryEntries = new LinkedHashMap<>(baseEntries);
      misleadingDirectoryEntries.put("assets/NOTMETA-INF/SIG-PLUGIN", new byte[] {1});

      Path baseJar = tempDir.resolve("base.jar");
      Path invalidExtensionJar = tempDir.resolve("invalid-extension.jar");
      Path misleadingDirectoryJar = tempDir.resolve("misleading-directory.jar");
      writeJar(baseJar, baseEntries, false, 1_600_000_000_000L);
      writeJar(invalidExtensionJar, invalidExtensionEntries, false, 1_600_000_000_000L);
      writeJar(misleadingDirectoryJar, misleadingDirectoryEntries, false, 1_600_000_000_000L);

      assertNotEquals(checksum(baseJar), checksum(invalidExtensionJar));
      assertNotEquals(checksum(baseJar), checksum(misleadingDirectoryJar));
    });
  }

  @Test
  public void testNativeLibraryArchitectureSelection() throws IOException {
    withTempDir(tempDir -> {
      Path jarPath = tempDir.resolve("native-libraries.jar");
      Map<String, byte[]> jarEntries = new LinkedHashMap<>();
      jarEntries.put("amd64/Linux/libcudfjni.so", new byte[] {1});
      jarEntries.put("aarch64/Linux/libcudfjni.so.chunks.properties", new byte[] {2});
      jarEntries.put("aarch64/Linux/libcudfjni.so.chunks/00000", new byte[] {3});
      jarEntries.put("aarch64/Linux/libcudf.so.chunks.properties", new byte[] {4});
      jarEntries.put("amd64/Windows/cudfjni.dll", new byte[] {5});
      writeJar(jarPath, jarEntries, false, 1_600_000_000_000L);

      try (JarFile jar = new JarFile(jarPath.toFile())) {
        Map<String, Set<String>> libraries =
            SparkRapidsJarTool.nativeLibrariesByArchitecture(jar, "Linux");
        assertEquals(new TreeSet<>(Arrays.asList("amd64", "aarch64")), libraries.keySet());
        assertEquals(new TreeSet<>(Arrays.asList("cudfjni")), libraries.get("amd64"));
        assertEquals(new TreeSet<>(Arrays.asList("cudf", "cudfjni")), libraries.get("aarch64"));
        assertEquals("amd64", SparkRapidsJarTool.selectNativeArchitecture(
            libraries, "cudfjni", "amd64"));
        assertEquals("aarch64", SparkRapidsJarTool.selectNativeArchitecture(
            libraries, "cudf", "amd64"));
        assertThrows(IOException.class, () -> SparkRapidsJarTool.selectNativeArchitecture(
            libraries, "cudfjni", "unknown"));
        assertThrows(IOException.class, () -> SparkRapidsJarTool.selectNativeArchitecture(
            libraries, "missing", "amd64"));
      }
    });
  }

  private static void withTempDir(TestBody testBody) throws IOException {
    Path tempDir = Files.createTempDirectory("spark-rapids-jar-tool-");
    try {
      testBody.run(tempDir);
    } finally {
      try (DirectoryStream<Path> files = Files.newDirectoryStream(tempDir)) {
        for (Path file : files) {
          Files.delete(file);
        }
      }
      Files.delete(tempDir);
    }
  }

  @FunctionalInterface
  private interface TestBody {
    void run(Path tempDir) throws IOException;
  }

  private static Map<String, byte[]> entries(
      String productName, String version, byte[] payload) {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put("META-INF/MANIFEST.MF",
        ("Manifest-Version: 1.0\nImplementation-Title: " + productName + "\n\n")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("META-INF/maven/com.nvidia/plugin/pom.properties",
        ("artifactId=" + productName + "\nversion=" + version + "\n")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("spark359/META-INF/maven/com.nvidia/plugin/pom.xml",
        ("<project><name>" + productName + "</name></project>")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("META-INF/PLUGIN.SF", productName.getBytes(StandardCharsets.UTF_8));
    entries.put("META-INF/SIG-PLUGIN", productName.getBytes(StandardCharsets.UTF_8));
    entries.put("spark359/META-INF/SIG-PLUGIN.A1",
        productName.getBytes(StandardCharsets.UTF_8));
    entries.put("rapids-4-spark-version-info.properties",
        ("version=" + version + "\n").getBytes(StandardCharsets.UTF_8));
    entries.put("spark359/cudf-spark-private-version-info.properties",
        ("version=" + version + "\nrevision=metadata-only\n")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("com/nvidia/spark/rapids/Payload.class", payload);
    return entries;
  }

  private static Map<String, byte[]> reverse(Map<String, byte[]> entries) {
    Map<String, byte[]> reversed = new LinkedHashMap<>();
    entries.entrySet().stream()
        .sorted((left, right) -> right.getKey().compareTo(left.getKey()))
        .forEach(entry -> reversed.put(entry.getKey(), entry.getValue()));
    return reversed;
  }

  private static void writeJar(
      Path path, Map<String, byte[]> entries, boolean stored, long timestamp) throws IOException {
    try (OutputStream out = java.nio.file.Files.newOutputStream(path);
         ZipOutputStream zip = new ZipOutputStream(out)) {
      for (Map.Entry<String, byte[]> item : entries.entrySet()) {
        byte[] contents = item.getValue();
        ZipEntry entry = new ZipEntry(item.getKey());
        entry.setTime(timestamp);
        if (stored) {
          CRC32 crc = new CRC32();
          crc.update(contents);
          entry.setMethod(ZipEntry.STORED);
          entry.setSize(contents.length);
          entry.setCompressedSize(contents.length);
          entry.setCrc(crc.getValue());
        }
        zip.putNextEntry(entry);
        zip.write(contents);
        zip.closeEntry();
      }
    }
  }

  private static String checksum(Path path) throws IOException {
    try (JarFile jar = new JarFile(path.toFile())) {
      return SparkRapidsJarTool.payloadSha256(jar);
    }
  }

  private static String title(Path path) throws IOException {
    try (JarFile jar = new JarFile(path.toFile())) {
      return SparkRapidsJarTool.distributionTitle(jar);
    }
  }
}
