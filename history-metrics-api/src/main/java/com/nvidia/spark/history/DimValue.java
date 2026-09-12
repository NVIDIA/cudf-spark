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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * A typed, equality-only dimension value.
 *
 * <p>Its canonical component is a one-byte kind tag, an unsigned two-byte big-endian payload
 * length, and the payload. A complete component is limited to 256 bytes.
 */
public abstract class DimValue {
  /** Maximum encoded size of a complete tagged and length-framed value. */
  public static final int MAX_CANONICAL_BYTES = 256;
  private static final int FRAME_BYTES = 3;
  private static final int MAX_PAYLOAD_BYTES = MAX_CANONICAL_BYTES - FRAME_BYTES;

  private final Kind kind;
  private final byte[] payload;

  private DimValue(Kind kind, byte[] payload) {
    this.kind = Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(payload, "payload");
    if (payload.length > MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException(
          "canonical dimension component exceeds " + MAX_CANONICAL_BYTES + " bytes");
    }
    this.payload = payload.clone();
  }

  /** Closed equality-value kinds supported by the portable contract. */
  public enum Kind {
    /** Strict UTF-8 string equality. */
    STRING((byte) 1),
    /** Signed 64-bit integer equality. */
    LONG((byte) 2),
    /** Opaque byte-sequence equality. */
    BYTES((byte) 3);

    private final byte tag;

    Kind(byte tag) {
      this.tag = tag;
    }
  }

  /**
   * Creates a strict UTF-8 string value.
   *
   * @param value non-null string whose framed encoding is at most
   *     {@value #MAX_CANONICAL_BYTES} bytes
   * @return the immutable value
   * @throws NullPointerException if {@code value} is null
   * @throws IllegalArgumentException if the string is malformed or over the encoded bound
   */
  public static DimValue of(String value) {
    Objects.requireNonNull(value, "value");
    return new CanonicalDimValue(Kind.STRING, StrictUtf8.encode(value, "string dimension value"));
  }

  /**
   * Creates a signed 64-bit integer value.
   *
   * @param value integer value
   * @return the immutable value
   */
  public static DimValue of(long value) {
    return new CanonicalDimValue(
        Kind.LONG, ByteBuffer.allocate(Long.BYTES).putLong(value).array());
  }

  /**
   * Creates an opaque byte value by defensive copy.
   *
   * @param value non-null payload whose framed encoding is at most
   *     {@value #MAX_CANONICAL_BYTES} bytes
   * @return the immutable value
   * @throws NullPointerException if {@code value} is null
   * @throws IllegalArgumentException if the payload is over the encoded bound
   */
  public static DimValue of(byte[] value) {
    return new CanonicalDimValue(Kind.BYTES, value);
  }

  /** @return the exact value kind */
  public final Kind kind() {
    return kind;
  }

  /**
   * @return the string payload
   * @throws IllegalStateException if this value is not {@link Kind#STRING}
   */
  public final String stringValue() {
    requireKind(Kind.STRING);
    return new String(payload, StandardCharsets.UTF_8);
  }

  /**
   * @return the signed integer payload
   * @throws IllegalStateException if this value is not {@link Kind#LONG}
   */
  public final long longValue() {
    requireKind(Kind.LONG);
    return ByteBuffer.wrap(payload).getLong();
  }

  /**
   * @return a defensive copy of the opaque byte payload
   * @throws IllegalStateException if this value is not {@link Kind#BYTES}
   */
  public final byte[] bytesValue() {
    requireKind(Kind.BYTES);
    return payload.clone();
  }

  /**
   * Returns the stable kind-tagged, length-framed canonical encoding.
   *
   * @return a newly allocated canonical byte array
   */
  public final byte[] canonicalBytes() {
    byte[] canonical = new byte[FRAME_BYTES + payload.length];
    canonical[0] = kind.tag;
    canonical[1] = (byte) (payload.length >>> 8);
    canonical[2] = (byte) payload.length;
    System.arraycopy(payload, 0, canonical, FRAME_BYTES, payload.length);
    return canonical;
  }

  private void requireKind(Kind required) {
    if (kind != required) {
      throw new IllegalStateException("value kind is " + kind + ", not " + required);
    }
  }

  @Override
  public final boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DimValue)) {
      return false;
    }
    DimValue that = (DimValue) other;
    return kind == that.kind && Arrays.equals(payload, that.payload);
  }

  @Override
  public final int hashCode() {
    return 31 * kind.hashCode() + Arrays.hashCode(payload);
  }

  private static final class CanonicalDimValue extends DimValue {
    private CanonicalDimValue(Kind kind, byte[] payload) {
      super(kind, payload);
    }
  }
}
