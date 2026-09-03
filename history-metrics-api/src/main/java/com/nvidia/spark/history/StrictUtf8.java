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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Strict UTF-8 encoding shared by persisted identity components. */
final class StrictUtf8 {
  private StrictUtf8() {
  }

  static byte[] encode(String value, String description) {
    Objects.requireNonNull(value, "value");
    try {
      ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(value));
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      return bytes;
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(description + " must contain valid Unicode", e);
    }
  }

  static void requireEncodedLengthAtMost(
      String value, String description, int maximumBytes) {
    Objects.requireNonNull(value, "value");
    if (maximumBytes < 0 || maximumBytes == Integer.MAX_VALUE) {
      throw new IllegalArgumentException("maximumBytes must permit a bounded validation buffer");
    }
    ByteBuffer encoded = ByteBuffer.allocate(maximumBytes + 1);
    try {
      CoderResult result = StandardCharsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(value), encoded, true);
      if (result.isError()) {
        result.throwException();
      }
      if (result.isOverflow() || encoded.position() > maximumBytes) {
        throw new IllegalArgumentException(
            description + " must encode to at most " + maximumBytes + " UTF-8 bytes");
      }
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(description + " must contain valid Unicode", e);
    }
  }
}
