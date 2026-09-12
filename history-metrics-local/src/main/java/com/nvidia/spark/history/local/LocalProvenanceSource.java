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
package com.nvidia.spark.history.local;

/** Supplies caller-redacted framework identity to an explicitly owned local provider. */
public interface LocalProvenanceSource {
  /**
   * Supplies identity sampled exactly once for one public record call.
   *
   * <p>The implementation must return caller-redacted text without credentials or authentication
   * tokens. A null value, invalid identity, or source failure causes the local store to count and
   * drop otherwise valid observations from that call.
   *
   * @return current non-null caller-redacted identity
   */
  LocalProvenanceIdentity current();
}
