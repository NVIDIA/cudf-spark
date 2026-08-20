/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Lowercase, deduplicate, and sort a variable-length tag array.
 * See normalize_tags.sql for equivalent SQL expression.
 */
public class NormalizeTags extends UDF {
    public List<String> evaluate(List<String> tags) {
        if (tags == null) {
            return null;
        }
        TreeSet<String> result = new TreeSet<>();
        for (String tag : tags) {
            if (tag != null) {
                String stripped = tag.replaceAll("^ +| +$", "").toLowerCase();
                if (!stripped.isEmpty()) {
                    result.add(stripped);
                }
            }
        }
        return result.isEmpty() ? null : new ArrayList<>(result);
    }
}
