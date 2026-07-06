/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.Seq;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Lowercase, deduplicate, and sort a variable-length tag array.
 * See normalize_tags.sql for equivalent SQL expression.
 */
public class NormalizeTags implements UDF1<Seq<String>, List<String>> {
    @Override
    public List<String> call(Seq<String> tags) throws Exception {
        if (tags == null) {
            return null;
        }
        TreeSet<String> result = new TreeSet<>();
        Iterator<String> it = tags.iterator();
        while (it.hasNext()) {
            String tag = it.next();
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
