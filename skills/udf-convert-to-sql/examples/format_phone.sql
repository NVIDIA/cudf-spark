-- SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

SELECT
  CASE
    WHEN phone IS NULL THEN NULL
    WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 THEN NULL
    ELSE CONCAT(
      '(',
      SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', ''), 1, 3),
      ') ',
      SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', ''), 4, 3),
      '-',
      SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', ''), 7, 4)
    )
  END AS result
FROM __table__
