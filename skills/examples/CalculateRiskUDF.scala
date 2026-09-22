/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package examples

/**
 * Calculate risk score based on credit score.
 * 
 * @param creditScore Credit score
 * @return Risk score
 */
class CalculateRiskUDF extends Function1[Integer, String] with Serializable {
  override def apply(creditScore: Integer): String = {
    Option(creditScore) match {
      case Some(score) if score >= 750 => "LOW"
      case Some(score) if score >= 650 => "MEDIUM"
      case Some(score) if score >= 500 => "HIGH"
      case Some(score) if score < 500 => "VERY_HIGH"
      case None => "UNKNOWN"
    }
  }
}
