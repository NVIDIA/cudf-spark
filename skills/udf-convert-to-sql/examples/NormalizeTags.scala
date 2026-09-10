/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * Lowercase, deduplicate, and sort a variable-length tag array.
 * See normalize_tags.sql for equivalent SQL expression.
 */
object NormalizeTags {
  val normalizeTags: UserDefinedFunction = udf((tags: Seq[String]) => {
    Option(tags).flatMap { ts =>
      val cleaned = ts
        .filter(_ != null)
        .map(_.replaceAll("^ +| +$", "").toLowerCase)
        .filter(_.nonEmpty)
        .distinct
        .sorted
      if (cleaned.isEmpty) None else Some(cleaned)
    }.orNull
  })
}
