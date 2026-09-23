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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkRapidsJarTool {
  @TempDir
  Path tempDir;

  @Test
  public void testPayloadChecksumIgnoresPublicationMetadata() throws IOException {
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

    assertEquals(checksum(rapidsJar), checksum(renamedJar));
    assertNotEquals(checksum(rapidsJar), checksum(changedPayloadJar));
  }

  private static Map<String, byte[]> entries(
      String productName, String version, byte[] payload) {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put("META-INF/MANIFEST.MF",
        ("Manifest-Version: 1.0\nImplementation-Title: " + productName + "\n")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("META-INF/maven/com.nvidia/plugin/pom.properties",
        ("artifactId=" + productName + "\nversion=" + version + "\n")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("spark359/META-INF/maven/com.nvidia/plugin/pom.xml",
        ("<project><name>" + productName + "</name></project>")
            .getBytes(StandardCharsets.UTF_8));
    entries.put("META-INF/PLUGIN.SF", productName.getBytes(StandardCharsets.UTF_8));
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
}
