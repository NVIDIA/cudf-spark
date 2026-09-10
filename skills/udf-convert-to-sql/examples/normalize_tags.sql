-- SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

SELECT
  CASE
    WHEN tags IS NULL THEN NULL
    WHEN SIZE(FILTER(tags, x -> x IS NOT NULL AND TRIM(x) != '')) = 0 THEN NULL
    ELSE SORT_ARRAY(ARRAY_DISTINCT(
      TRANSFORM(
        FILTER(tags, x -> x IS NOT NULL AND TRIM(x) != ''),
        x -> LOWER(TRIM(x))
      )
    ))
  END AS result
FROM __table__
