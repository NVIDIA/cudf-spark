/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package examples;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

@Description(name = "integer_multiply_by_2", value = "_FUNC_(x) - Returns x * 2 for integer values")
public class IntegerMultiplyBy2UDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(IntegerMultiplyBy2UDF.class);
    private PrimitiveObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("Exactly one argument is expected.");
        }

        ObjectInspector oi = arguments[0];
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed.");
        }

        inputOI = (PrimitiveObjectInspector) oi;
        
        // Check if input is numeric
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT &&
            inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG &&
            inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.SHORT &&
            inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BYTE) {
            throw new UDFArgumentTypeException(0, "Argument must be numeric (INT/LONG/SHORT/BYTE), but " + inputOI.getPrimitiveCategory().name() + " was passed.");
        }

        // Return LongWritable type for the result
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments == null || arguments.length != 1) {
            return null;
        }

        Object input = arguments[0].get();
        if (input == null) {
            return null;
        }

        long value = getLongValue(input);
        return new LongWritable(value * 2);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "integer_multiply_by_2(" + (children != null ? String.join(",", children) : "") + ")";
    }

    private long getLongValue(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        } else {
            throw new IllegalArgumentException("Cannot convert " + obj.getClass().getName() + " to long");
        }
    }
}
