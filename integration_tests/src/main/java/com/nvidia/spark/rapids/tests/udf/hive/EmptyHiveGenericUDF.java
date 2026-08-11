/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/** An empty Hive GenericUDF returning the input directly for row-based UDF test only */
public class EmptyHiveGenericUDF extends GenericUDF {
  private transient PrimitiveObjectInspectorConverter.TextConverter converter;
  private final Text textResult = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("One argument is supported, but found: " + arguments.length);
    }
    if (!(arguments[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("Unsupported argument type: " + arguments[0].getTypeName());
    }
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) arguments[0];
    converter = new PrimitiveObjectInspectorConverter.TextConverter(poi);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    Text text = converter.convert(deferredObjects[0].get());
    textResult.set(text == null ? "" : text.toString());
    return textResult;
  }

  @Override
  public String getDisplayString(String[] strings) {
    return getStandardDisplayString("empty", strings);
  }
}
