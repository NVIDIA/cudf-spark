/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.functions.udf

/**
 * Strip non-digit characters and format as (XXX) XXX-XXXX.
 * See format_phone.sql for equivalent SQL expression.
 */
object FormatPhone {
  val formatPhone = udf((phone: String) => {
    Option(phone).flatMap { p =>
      val digits = p.replaceAll("[^0-9]", "")
      if (digits.length == 10)
        Some(s"($${digits.substring(0, 3)}) $${digits.substring(3, 6)}-$${digits.substring(6)}")
      else
        None
    }.orNull
  })
}
