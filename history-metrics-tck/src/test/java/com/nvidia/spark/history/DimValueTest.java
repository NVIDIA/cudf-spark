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
package com.nvidia.spark.history;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

/** Behavioral coverage for FR-02 typed equality values and canonical component framing. */
class DimValueTest {
  @Test
  void fr02FramesStringLongAndBytesDeterministically() {
    byte[] stringFrame = DimValue.of("é").canonicalBytes();
    byte[] longFrame = DimValue.of(-1L).canonicalBytes();
    byte[] bytesFrame = DimValue.of(new byte[] {10, 20}).canonicalBytes();

    assertEquals(5, stringFrame.length);
    assertArrayEquals(
        new byte[] {0, 2, (byte) 0xC3, (byte) 0xA9},
        Arrays.copyOfRange(stringFrame, 1, stringFrame.length));
    assertEquals(11, longFrame.length);
    assertArrayEquals(
        new byte[] {0, 8, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        Arrays.copyOfRange(longFrame, 1, longFrame.length));
    assertArrayEquals(
        new byte[] {0, 2, 10, 20},
        Arrays.copyOfRange(bytesFrame, 1, bytesFrame.length));
    assertNotEquals(stringFrame[0], longFrame[0]);
    assertNotEquals(stringFrame[0], bytesFrame[0]);
    assertNotEquals(longFrame[0], bytesFrame[0]);

    assertNotEquals(DimValue.of("1"), DimValue.of(new byte[] {'1'}));
    assertEquals(DimValue.Kind.STRING, DimValue.of("value").kind());
    assertEquals(DimValue.Kind.LONG, DimValue.of(1L).kind());
    assertEquals(DimValue.Kind.BYTES, DimValue.of(new byte[0]).kind());
  }

  @Test
  void fr02RejectsDistinctMalformedSurrogatesInsteadOfReplacingThem() {
    String reviewerProbeLeft = "\uD800";
    String reviewerProbeRight = "\uD801";
    assertNotEquals(reviewerProbeLeft, reviewerProbeRight);

    assertThrows(IllegalArgumentException.class, () -> DimValue.of(reviewerProbeLeft));
    assertThrows(IllegalArgumentException.class, () -> DimValue.of(reviewerProbeRight));
    assertThrows(IllegalArgumentException.class, () -> DimValue.of("\uDC00"));
    assertThrows(IllegalArgumentException.class, () -> DimValue.of("before\uD800after"));
  }

  @Test
  void fr02KeepsValidSupplementaryUnicodeStable() {
    String text = "table-\uD83D\uDE80-\u00e9";
    DimValue first = DimValue.of(text);
    DimValue second = DimValue.of(text);

    assertEquals(text, first.stringValue());
    assertEquals(first, second);
    assertArrayEquals(first.canonicalBytes(), second.canonicalBytes());
  }

  @Test
  void fr02AppliesTheCapToTheCompleteFramedComponent() {
    assertEquals(255, DimValue.of(new byte[252]).canonicalBytes().length);
    assertEquals(256, DimValue.of(new byte[253]).canonicalBytes().length);
    assertThrows(IllegalArgumentException.class, () -> DimValue.of(new byte[254]));

    assertEquals(255, DimValue.of(repeatAscii(252)).canonicalBytes().length);
    assertEquals(256, DimValue.of(repeatAscii(253)).canonicalBytes().length);
    assertThrows(IllegalArgumentException.class, () -> DimValue.of(repeatAscii(254)));
  }

  @Test
  void fr02DefensivelyCopiesByteInputsAndOutputs() {
    byte[] input = new byte[] {1, 2, 3};
    DimValue value = DimValue.of(input);
    input[0] = 99;
    assertArrayEquals(new byte[] {1, 2, 3}, value.bytesValue());

    byte[] payload = value.bytesValue();
    payload[1] = 99;
    byte[] frame = value.canonicalBytes();
    frame[3] = 99;
    assertArrayEquals(new byte[] {1, 2, 3}, value.bytesValue());
    assertArrayEquals(new byte[] {3, 0, 3, 1, 2, 3}, value.canonicalBytes());
  }

  @Test
  void fr02UsesUtf8PayloadLengthRatherThanCharacterCount() {
    byte[] payload = "é".getBytes(StandardCharsets.UTF_8);
    assertEquals(payload.length, DimValue.of("é").canonicalBytes()[2]);
    assertThrows(IllegalArgumentException.class, () -> DimValue.of(repeatUnicode(127)));
  }

  private static String repeatAscii(int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, 'a');
    return new String(chars);
  }

  private static String repeatUnicode(int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, 'é');
    return new String(chars);
  }
}
