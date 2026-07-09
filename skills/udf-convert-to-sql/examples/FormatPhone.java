/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.api.java.UDF1;

/**
 * Strip non-digit characters and format as (XXX) XXX-XXXX.
 * See format_phone.sql for equivalent SQL expression.
 */
public class FormatPhone implements UDF1<String, String> {
    @Override
    public String call(String phone) throws Exception {
        if (phone == null) {
            return null;
        }
        String digits = phone.replaceAll("[^0-9]", "");
        if (digits.length() != 10) {
            return null;
        }
        return String.format("(%s) %s-%s",
            digits.substring(0, 3),
            digits.substring(3, 6),
            digits.substring(6));
    }
}
