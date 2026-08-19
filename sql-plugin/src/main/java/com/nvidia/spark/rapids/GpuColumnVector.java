/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.*;
import com.nvidia.spark.rapids.shims.GpuTypeShims;
import org.apache.arrow.memory.ReferenceManager;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import scala.Tuple2;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public class GpuColumnVector extends GpuColumnVectorBase {

  /**
   * Print to standard error the contents of a table. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the table to print out.
   * @param table the table to print out.
   * @deprecated Use ai.rapids.cudf.TableDebug
   */
  @Deprecated
  public static void debug(String name, Table table) {
    TableDebug.get().debug(name, table);
  }

  /**
   * Print to standard error the contents of a table. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the table to print out.
   * @param cb the batch to print out.
   */
  public static void debug(String name, ColumnarBatch cb) {
    if (cb.numCols() <= 0) {
      System.err.println("DEBUG " + name + " NO COLS " + cb.numRows() + " ROWS");
    } else {
      try (Table table = from(cb)) {
        TableDebug.get().debug(name, table);
      }
    }
  }

  /**
   * Print to standard error the contents of a column. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the column to print out.
   * @param col the column to print out.
   * @deprecated see ai.rapids.cudf.TableDebug
   */
  @Deprecated
  public static void debug(String name, ai.rapids.cudf.ColumnView col) {
    TableDebug.get().debug(name, col);
  }

  /**
   * Print to standard error the contents of a column. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the column to print out.
   * @param hostCol the column to print out.
   * @deprecated Use ai.rapids.cudf.TableDebug
   */
  @Deprecated
  public static void debug(String name, HostColumnVectorCore hostCol) {
    TableDebug.get().debug(name, hostCol);
  }

  public static HostColumnVector.DataType convertFrom(DataType spark, boolean nullable) {
    if (spark instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) spark;
      return new HostColumnVector.ListType(nullable,
          convertFrom(arrayType.elementType(), arrayType.containsNull()));
    } else if (spark instanceof MapType) {
      MapType mapType = (MapType) spark;
      return new HostColumnVector.ListType(nullable,
          new HostColumnVector.StructType(false, Arrays.asList(
              convertFrom(mapType.keyType(), false),
              convertFrom(mapType.valueType(), mapType.valueContainsNull())
          )));
    } else if (spark instanceof StructType) {
      StructType stType = (StructType) spark;
      HostColumnVector.DataType[] children = new HostColumnVector.DataType[stType.size()];
      StructField[] fields = stType.fields();
      for (int i = 0; i < children.length; i++) {
        children[i] = convertFrom(fields[i].dataType(), fields[i].nullable());
      }
      return new HostColumnVector.StructType(nullable, children);
    } else if (spark instanceof BinaryType) {
      return new HostColumnVector.ListType(
          nullable, new HostColumnVector.BasicType(false, DType.UINT8));
    } else {
      // Only works for basic types
      return new HostColumnVector.BasicType(nullable, getNonNestedRapidsType(spark));
    }
  }

  public static abstract class GpuColumnarBatchBuilderBase implements AutoCloseable {
    protected StructField[] fields;

    public abstract void close();

    public abstract long copyColumnar(ColumnVector cv, int colNum, int rows);

    protected abstract ai.rapids.cudf.ColumnVector buildAndPutOnDevice(int builderIndex);

    /** Try to build a ColumnarBatch, it can be called multiple times in case of failures */
    public abstract ColumnarBatch tryBuild(int rows);

    public ColumnarBatch build(int rows) {
      return build(rows, this::buildAndPutOnDevice);
    }

    protected ColumnarBatch build(int rows, Function<Integer, ai.rapids.cudf.ColumnVector> col) {
      ColumnVector[] vectors = new ColumnVector[fields.length];
      boolean success = false;
      try {
        for (int i = 0; i < fields.length; i++) {
          vectors[i] = new GpuColumnVector(fields[i].dataType(), col.apply(i));
        }
        ColumnarBatch ret = new ColumnarBatch(vectors, rows);
        success = true;
        return ret;
      } finally {
        if (!success) {
          for (ColumnVector vec: vectors) {
            if (vec != null) {
              vec.close();
            }
          }
        }
      }
    }
  }

  public static final class GpuArrowColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
    private final ai.rapids.cudf.ArrowColumnBuilder[] builders;

    private final ArrowBufReferenceHolder[] referenceHolders;

    /**
     * A collection of builders for building up columnar data from Arrow data.
     * @param schema the schema of the batch.
     */
    public GpuArrowColumnarBatchBuilder(StructType schema) {
      fields = schema.fields();
      int len = fields.length;
      builders = new ai.rapids.cudf.ArrowColumnBuilder[len];
      referenceHolders = new ArrowBufReferenceHolder[len];
      boolean success = false;

      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          builders[i] = new ArrowColumnBuilder(convertFrom(field.dataType(), field.nullable()));
          referenceHolders[i] = new ArrowBufReferenceHolder();
        }
        success = true;
      } finally {
        if (!success) {
          close();
        }
      }
    }

    @Override
    protected ai.rapids.cudf.ColumnVector buildAndPutOnDevice(int builderIndex) {
      ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
      referenceHolders[builderIndex].releaseReferences();
      builders[builderIndex] = null;
      return cv;
    }

    @Override
    public long copyColumnar(ColumnVector cv, int colNum, int rows) {
      Tuple2<List<ReferenceManager>, Long> copyResult =
          HostColumnarToGpu.arrowColumnarCopy(cv, builder(colNum), rows);
      referenceHolders[colNum].addReferences(copyResult._1);
      return copyResult._2;
    }

    public ai.rapids.cudf.ArrowColumnBuilder builder(int i) {
      return builders[i];
    }

    @Override
    public ColumnarBatch tryBuild(int rows) {
      // Arrow data should not be released until close is called.
      return build(rows, i -> builders[i].buildAndPutOnDevice());
    }

    @Override
    public void close() {
      for (ai.rapids.cudf.ArrowColumnBuilder b: builders) {
        if (b != null) {
          b.close();
        }
      }
      for (ArrowBufReferenceHolder holder: referenceHolders) {
        holder.releaseReferences();
      }
    }
  }

  public static final class GpuColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
    private final RapidsHostColumnBuilder[] builders;
    private ai.rapids.cudf.HostColumnVector[] hostColumns;
    private ai.rapids.cudf.HostColumnVector[] wipHostColumns;

    /**
     * A collection of builders for building up columnar data.
     * @param schema the schema of the batch.
     * @param initialRows the initial number of rows to allocate for this batch.
     */
    public GpuColumnarBatchBuilder(StructType schema, int initialRows) {
      fields = schema.fields();
      int len = fields.length;
      builders = new RapidsHostColumnBuilder[len];
      boolean success = false;
      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          builders[i] =
              new RapidsHostColumnBuilder(convertFrom(field.dataType(), field.nullable()), initialRows);
        }
        success = true;
      } finally {
        if (!success) {
          for (RapidsHostColumnBuilder b: builders) {
            if (b != null) {
              b.close();
            }
          }
        }
      }
    }

    @Override
    public long copyColumnar(ColumnVector cv, int colNum, int rows) {
      if (builders.length > 0) {
        return HostColumnarToGpu.columnarCopy(cv, builder(colNum), fields[colNum].dataType(), rows);
      } else {
        return 0;
      }
    }

    public RapidsHostColumnBuilder builder(int i) {
      return builders[i];
    }

    /**
     * Capture the current state of all column builders for rollback on OOM.
     * @return array of snapshots, one per builder
     */
    public RapidsHostColumnBuilder.BuilderSnapshot[] captureState() {
      RapidsHostColumnBuilder.BuilderSnapshot[] snapshots =
          new RapidsHostColumnBuilder.BuilderSnapshot[builders.length];
      for (int i = 0; i < builders.length; i++) {
        if (builders[i] != null) {
          snapshots[i] = builders[i].captureState();
        }
      }
      return snapshots;
    }

    /**
     * Restore all column builders to a previously captured state.
     * @param snapshots the snapshots captured via {@link #captureState()}
     */
    public void restoreState(RapidsHostColumnBuilder.BuilderSnapshot[] snapshots) {
      if (snapshots == null || snapshots.length != builders.length) {
        throw new IllegalArgumentException(
            "Snapshot array must match builders length");
      }
      for (int i = 0; i < builders.length; i++) {
        if (snapshots[i] != null && builders[i] != null) {
          builders[i].restoreState(snapshots[i]);
        }
      }
    }

    @Override
    protected ai.rapids.cudf.ColumnVector buildAndPutOnDevice(int builderIndex) {
      ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
      builders[builderIndex].close();
      builders[builderIndex] = null;
      return cv;
    }

    public HostColumnVector[] buildHostColumns() {
      // buildHostColumns is called from tryBuild, and tryBuild has to be safe to call
      // multiple times, so if a retry exception happens in this code, we need to pick
      // up where we left off last time.
      if (wipHostColumns == null) {
        wipHostColumns = new HostColumnVector[builders.length];
      }
      for (int i = 0; i < builders.length; i++) {
        if (builders[i] != null && wipHostColumns[i] == null) {
          wipHostColumns[i] = builders[i].build();
          builders[i].close();
          builders[i] = null;
        } else if (builders[i] == null && wipHostColumns[i] == null) {
          throw new IllegalStateException("buildHostColumns cannot be called more than once");
        }
      }
      HostColumnVector[] result = wipHostColumns;
      wipHostColumns = null;
      return result;
    }

    /**
     * Build a columnar batch without releasing the holding data on host.
     * It is safe to call this multiple times, and data will be released
     * after a call to `close`.
     */
    @Override
    public ColumnarBatch tryBuild(int rows) {
      if (hostColumns == null) {
        hostColumns = buildHostColumns();
      }
      return build(rows, i -> hostColumns[i].copyToDevice());
    }

    @Override
    public void close() {
      try {
        for (RapidsHostColumnBuilder b: builders) {
          if (b != null) {
            b.close();
          }
        }
      } finally {
        try {
          if (hostColumns != null) {
            for (ai.rapids.cudf.HostColumnVector hcv : hostColumns) {
              if (hcv != null) {
                hcv.close();
              }
            }
            hostColumns = null;
          }
        } finally {
          if (wipHostColumns != null) {
            for (ai.rapids.cudf.HostColumnVector hcv : wipHostColumns) {
              if (hcv != null) {
                hcv.close();
              }
            }
            wipHostColumns = null;
          }
        }
      }
    }
  }

  private static final class ArrowBufReferenceHolder {
    private final List<ReferenceManager> references = new ArrayList<>();

    public void addReferences(List<ReferenceManager> refs) {
      references.addAll(refs);
      refs.forEach(ReferenceManager::retain);
    }

    public void releaseReferences() {
      if (references.isEmpty()) {
        return;
      }
      for (ReferenceManager ref: references) {
        ref.release();
      }
      references.clear();
    }
  }

  private static DType toRapidsOrNull(DataType type) {
    DType ret = toRapidsOrNullCommon(type);
    // Check types that shim supporting
    // e.g.: Spark 3.3.0 begin supporting AnsiIntervalType to/from parquet
    return (ret != null) ? ret : GpuTypeShims.toRapidsOrNull(type);
  }

  private static DType toRapidsOrNullCommon(DataType type) {
    if (type instanceof LongType) {
      return DType.INT64;
    } else if (type instanceof DoubleType) {
      return DType.FLOAT64;
    } else if (type instanceof ByteType) {
      return DType.INT8;
    } else if (type instanceof BooleanType) {
      return DType.BOOL8;
    } else if (type instanceof ShortType) {
      return DType.INT16;
    } else if (type instanceof IntegerType) {
      return DType.INT32;
    } else if (type instanceof FloatType) {
      return DType.FLOAT32;
    } else if (type instanceof DateType) {
      return DType.TIMESTAMP_DAYS;
    } else if (type instanceof TimestampType) {
      return DType.TIMESTAMP_MICROSECONDS;
    } else if (type instanceof StringType) {
      return DType.STRING;
    } else if (type instanceof BinaryType) {
      // FIXME: this should not be here, we should be able remove or throw
      return DType.LIST;
    } else if (type instanceof NullType) {
      // INT8 is used for both in this case
      return DType.INT8;
    } else if (type instanceof DecimalType) {
      // Decimal supportable check has been conducted in the GPU plan overriding stage.
      // So, we don't have to handle decimal-supportable problem at here.
      return DecimalUtil.createCudfDecimal((DecimalType) type);
    } else if (type instanceof GpuUnsignedIntegerType) {
      return DType.UINT32;
    } else if (type instanceof GpuUnsignedLongType) {
      return DType.UINT64;
    }
    return null;
  }

  public static DType getRapidsType(DataType type) {
    if (type instanceof ArrayType) {
      return DType.LIST;
    } else if (type instanceof StructType) {
      return DType.STRUCT;
    } else {
      return getNonNestedRapidsType(type);
    }
  }

  public static boolean isNonNestedSupportedType(DataType type) {
    return toRapidsOrNull(type) != null;
  }

  public static DType getNonNestedRapidsType(DataType type) {
    DType result = toRapidsOrNull(type);
    if (result == null) {
      throw new IllegalArgumentException(type + " is not supported for GPU processing yet.");
    }
    return result;
  }

  /**
   * Create an empty batch from the given format.  This should be used very sparingly because
   * returning an empty batch from an operator is almost always the wrong thing to do.
   */
  public static ColumnarBatch emptyBatch(StructType schema) {
    try (GpuColumnarBatchBuilder builder = new GpuColumnarBatchBuilder(schema, 0)) {
      return builder.build(0);
    }
  }

  /**
   * Create an empty batch from the given format.  This should be used very sparingly because
   * returning an empty batch from an operator is almost always the wrong thing to do.
   */
  public static ColumnarBatch emptyBatch(List<Attribute> format) {
    return emptyBatch(structFromAttributes(format));
  }

  /**
   * Create an empty batch from the give data types
   */
  public static ColumnarBatch emptyBatchFromTypes(DataType[] format) {
    return emptyBatch(structFromTypes(format));
  }


  /**
   * Create empty host column vectors from the given format.  This should only be necessary
   * when serializing an empty broadcast table.
   */
  public static HostColumnVector[] emptyHostColumns(StructType schema) {
    try (GpuColumnarBatchBuilder builder = new GpuColumnarBatchBuilder(schema, 0)) {
      return builder.buildHostColumns();
    }
  }

  /**
   * Create empty host column vectors from the given format.  This should only be necessary
   * when serializing an empty broadcast table.
   */
  public static HostColumnVector[] emptyHostColumns(List<Attribute> format) {
    return emptyHostColumns(structFromAttributes(format));
  }

  /**
   * Create empty host column vectors from the given format.  This should only be necessary
   * when serializing an empty broadcast table.
   */
  public static HostColumnVector[] emptyHostColumns(DataType[] format) {
    return emptyHostColumns(structFromTypes(format));
  }

  private static StructType structFromTypes(DataType[] format) {
    StructField[] fields = new StructField[format.length];
    int i = 0;
    for (DataType t: format) {
      fields[i++] = new StructField(
          String.valueOf(i), // ignored
          t,
          true,
          null);
    }
    return new StructType(fields);
  }

  public static StructType structFromAttributes(List<Attribute> format) {
    StructField[] fields = new StructField[format.size()];
    int i = 0;
    for (Attribute attribute: format) {
      fields[i++] = new StructField(
          attribute.name(),
          attribute.dataType(),
          attribute.nullable(),
          null);
    }
    return new StructType(fields);
  }

  /**
   * Convert a Spark schema into a cudf schema
   * @param input the Spark schema to convert
   * @return the cudf schema
   */
  public static Schema from(StructType input) {
    Schema.Builder builder = Schema.builder();
    input.foreach(f -> builder.column(GpuColumnVector.getNonNestedRapidsType(f.dataType()), f.name()));
    return builder.build();
  }

  /**
   * Converts a list of Spark data types to a cudf schema.
   * <br/>
   *
   * This method correctly handles nested types, but will generate random field names.
   *
   * @param dataTypes the list of data types to convert
   * @return the cudf schema
   */
  public static Schema from(DataType[] dataTypes) {
    Schema.Builder builder = Schema.builder();
    visit(dataTypes, builder, 0);
    return builder.build();
  }

  private static void visit(DataType[] dataTypes, Schema.Builder builder, int level) {
    for (int idx = 0; idx < dataTypes.length; idx ++) {
      DataType dt = dataTypes[idx];
      String name = "_col_" + level + "_" + idx;
      if (dt instanceof MapType) {
        // MapType is list of struct in cudf, so need to handle it specially.
        Schema.Builder listBuilder = builder.addColumn(DType.LIST, name);
        Schema.Builder structBuilder = listBuilder.addColumn(DType.STRUCT, name + "_map");
        MapType mt = (MapType) dt;
        DataType[] structChildren = {mt.keyType(), mt.valueType()};
        visit(structChildren, structBuilder, level + 1);
      } else if (dt instanceof BinaryType) {
        Schema.Builder listBuilder = builder.addColumn(DType.LIST, name);
        listBuilder.addColumn(DType.UINT8, name + "_bytes");
      } else {
        Schema.Builder childBuilder = builder.addColumn(GpuColumnVector.getRapidsType(dt), name);
        if (dt instanceof ArrayType) {
          // Array (aka List)
          DataType[] childType = {((ArrayType) dt).elementType()};
          visit(childType, childBuilder, level + 1);
        } else if (dt instanceof StructType) {
          // Struct
          StructType st = (StructType) dt;
          DataType[] childrenTypes = new DataType[st.length()];
          for (int i = 0; i < childrenTypes.length; i ++) {
            childrenTypes[i] = st.apply(i).dataType();
          }
          visit(childrenTypes, childBuilder, level + 1);
        }
      }
    }
  }


  /**
   * Convert a ColumnarBatch to a table. The table will increment the reference count for all of
   * the columns in the batch, so you will need to close both the batch passed in and the table
   * returned to avoid any memory leaks.
   */
  public static Table from(ColumnarBatch batch) {
    return new Table(extractBases(batch));
  }

  /**
   * Get the data types for a batch.
   */
  public static DataType[] extractTypes(ColumnarBatch batch) {
    DataType[] ret = new DataType[batch.numCols()];
    for (int i = 0; i < batch.numCols(); i++) {
      ret[i] = batch.column(i).dataType();
    }
    return ret;
  }

  /**
   * Get the data types for a struct.
   */
  public static DataType[] extractTypes(StructType st) {
    DataType[] ret = new DataType[st.size()];
    for (int i = 0; i < st.size(); i++) {
      ret[i] = st.apply(i).dataType();
    }
    return ret;
  }

  /**
   * Convert a Table to a ColumnarBatch.  The columns in the table will have their reference counts
   * incremented so you will need to close both the table passed in and the batch returned to
   * not have any leaks.
   * @param colTypes the types of the columns that should be returned.
   */
  public static ColumnarBatch from(Table table, DataType[] colTypes) {
    return from(table, colTypes, 0, table.getNumberOfColumns());
  }

  /**
   * Returns true if the cudf column can be used for the specified Spark type.
   */
  static boolean typeConversionAllowed(ColumnView cv, DataType colType) {
    DType dt = cv.getType();
    if (!dt.isNestedType()) {
      return getNonNestedRapidsType(colType).equals(dt);
    }
    if (colType instanceof MapType) {
      MapType mType = (MapType) colType;
      // list of struct of key/value
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      try (ColumnView structCv = cv.getChildColumnView(0)) {
        if (!(structCv.getType().equals(DType.STRUCT))) {
          return false;
        }
        if (structCv.getNumChildren() != 2) {
          return false;
        }
        try (ColumnView keyCv = structCv.getChildColumnView(0)) {
          if (!typeConversionAllowed(keyCv, mType.keyType())) {
            return false;
          }
        }
        try (ColumnView valCv = structCv.getChildColumnView(1)) {
          return typeConversionAllowed(valCv, mType.valueType());
        }
      }
    } else if (colType instanceof ArrayType) {
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      try (ColumnView tmp = cv.getChildColumnView(0)) {
        return typeConversionAllowed(tmp, ((ArrayType) colType).elementType());
      }
    } else if (colType instanceof StructType) {
      if (!(dt.equals(DType.STRUCT))) {
        return false;
      }
      StructType st = (StructType) colType;
      final int numChildren = cv.getNumChildren();
      if (numChildren != st.size()) {
        return false;
      }
      for (int childIndex = 0; childIndex < numChildren; childIndex++) {
        try (ColumnView tmp = cv.getChildColumnView(childIndex)) {
          StructField entry = ((StructType) colType).apply(childIndex);
          if (!typeConversionAllowed(tmp, entry.dataType())) {
            return false;
          }
        }
      }
      return true;
    } else if (colType instanceof BinaryType) {
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      try (ColumnView tmp = cv.getChildColumnView(0)) {
        DType tmpType = tmp.getType();
        return tmpType.equals(DType.UINT8);
      }
    } else {
      // Unexpected type
      return false;
    }
  }

  static boolean typeConversionAllowed(Table table, DataType[] colTypes, int startCol, int endCol) {
    final int numColumns = endCol - startCol;
    assert numColumns == colTypes.length: "The number of columns and the number of types don't " +
        "match. Expected " + colTypes.length + " but found " + numColumns + ". (" + table +
        " columns " + startCol + " - " + endCol + " vs " +
        Arrays.toString(colTypes) + ")";
    boolean ret = true;
    for (int colIndex = startCol; colIndex < endCol; colIndex++) {
      boolean t = typeConversionAllowed(table.getColumn(colIndex), colTypes[colIndex - startCol]);
      ret = ret && t;
    }
    return ret;
  }

  /**
   * This should only ever be called from an assertion. This is to avoid the performance overhead
   * of doing the complicated check in production.  Sadly this means that we don't get to give a
   * clear message about what part of the check failed, so the assertions that use this should
   * include in the message both types so a user can see what is different about them.
   */
  static boolean typeConversionAllowed(Table table, DataType[] colTypes) {
    final int numColumns = table.getNumberOfColumns();
    assert numColumns == colTypes.length: "The number of columns and the number of types don't " +
        "match " + table + " " + Arrays.toString(colTypes);
    boolean ret = true;
    for (int colIndex = 0; colIndex < numColumns; colIndex++) {
      ret = ret && typeConversionAllowed(table.getColumn(colIndex), colTypes[colIndex]);
    }
    return ret;
  }

  /**
   * Get a ColumnarBatch from a set of columns in the Table. This gets the columns
   * starting at startColIndex and going until but not including untilColIndex. This will
   * increment the reference count for all columns converted so you will need to close
   * both the table that is passed in and the batch returned to be sure that there are no leaks.
   *
   * @param table a table of vectors
   * @param colTypes List of the column data types for the returned batch. It matches startColIndex
   *                 to untilColIndex instead of everything in table.
   * @param startColIndex index of the first vector you want in the final ColumnarBatch
   * @param untilColIndex until index of the columns. (ie doesn't include that column num)
   * @return a ColumnarBatch of the vectors from the table
   */
  public static ColumnarBatch from(Table table, DataType[] colTypes, int startColIndex, int untilColIndex) {
    assert table != null : "Table cannot be null";
    assert typeConversionAllowed(table, colTypes, startColIndex, untilColIndex) :
        "Type conversion is not allowed from " + table + " to " + Arrays.toString(colTypes) +
            " columns " + startColIndex + " to " + untilColIndex;
    int numColumns = untilColIndex - startColIndex;
    ColumnVector[] columns = new ColumnVector[numColumns];
    int finalLoc = 0;
    boolean success = false;
    try {
      for (int i = startColIndex; i < untilColIndex; i++) {
        columns[finalLoc] = from(table.getColumn(i).incRefCount(), colTypes[i - startColIndex]);
        finalLoc++;
      }
      long rows = table.getRowCount();
      if (rows != (int) rows) {
        throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
      }
      ColumnarBatch ret = new ColumnarBatch(columns, (int)rows);
      success = true;
      return ret;
    } finally {
      if (!success) {
        for (ColumnVector cv: columns) {
          if (cv != null) {
            cv.close();
          }
        }
      }
    }
  }

  /**
   * Converts a cudf internal vector to a Spark compatible vector. No reference counts
   * are incremented so you need to either close the returned value or the input value,
   * but not both.
   */
  public static GpuColumnVector from(ai.rapids.cudf.ColumnVector cudfCv, DataType type) {
    assert typeConversionAllowed(cudfCv, type) : "Type conversion is not allowed from " +
        buildColumnTypeString(cudfCv) + " to " + type + " expected " + buildColumnTypeString(type);
    return new GpuColumnVector(type, cudfCv);
  }

  private static String buildColumnTypeString(ai.rapids.cudf.ColumnView view) {
    DType type = view.getType();
    if (type.isNestedType()) {
      StringBuilder sb = new StringBuilder(type.toString());
      sb.append("(");
      for (int i = 0; i < view.getNumChildren(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        try (ColumnView childView = view.getChildColumnView(i)) {
          sb.append(buildColumnTypeString(childView));
        }
      }
      sb.append(")");
      return sb.toString();
    }
    return type.toString();
  }

  private static String buildColumnTypeString(DataType sparkType) {
    DType dtype = toRapidsOrNull(sparkType);
    if (dtype != null) {
      return dtype.toString();
    }
    StringBuilder sb = new StringBuilder();
    if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      sb.append("LIST(");
      sb.append(buildColumnTypeString(arrayType.elementType()));
      sb.append(")");
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      sb.append("LIST(STRUCT(");
      sb.append(buildColumnTypeString(mapType.keyType()));
      sb.append(",");
      sb.append(buildColumnTypeString(mapType.valueType()));
      sb.append("))");
    } else if (sparkType instanceof StructType) {
      StructType structType = (StructType) sparkType;
      sb.append(structType.iterator().map(f -> buildColumnTypeString(f.dataType()))
          .mkString("STRUCT(", ",", ")"));
    } else {
      throw new IllegalArgumentException("Unexpected data type: " + sparkType);
    }
    return sb.toString();
  }

  /**
   * Converts a cudf internal vector to a Spark compatible vector. No reference counts
   * are incremented so you need to either close the returned value or the input value,
   * but not both. This conversion performs an unconditional check that the types are
   * convertible rather than an assertion check.
   * @throws IllegalArgumentException if the type conversion check fails
   */
  public static GpuColumnVector fromChecked(ai.rapids.cudf.ColumnVector cudfCv, DataType type) {
    if (!typeConversionAllowed(cudfCv, type)) {
      throw new IllegalArgumentException("Type conversion error to " + type +
          ": expected cudf type " + buildColumnTypeString(type) +
          " found cudf type " + buildColumnTypeString(cudfCv));
    }
    return new GpuColumnVector(type, cudfCv);
  }

  public static GpuColumnVector from(Scalar scalar, int count, DataType sparkType) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar, count), sparkType);
  }

  /**
   * Creates a GpuColumnVector from a GpuScalar
   *
   * @param scalar the input GpuScalar
   * @param count the row number of the output column
   * @return a GpuColumnVector. It should be closed to avoid memory leak.
   */
  public static GpuColumnVector from(GpuScalar scalar, int count) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar.getBase(), count),
        scalar.dataType());
  }

  /**
   * Creates a cudf ColumnVector where the elements are filled with nulls.
   *
   * @param count the row number of the output column
   * @param sparkType the expected data type of the output column
   * @return a ColumnVector filled with nulls. It should be closed to avoid memory leak.
   */
  public static ai.rapids.cudf.ColumnVector columnVectorFromNull(int count, DataType sparkType) {
    try (Scalar s = GpuScalar.from(null, sparkType)) {
      return ai.rapids.cudf.ColumnVector.fromScalar(s, count);
    }
  }

  /**
   * Creates a GpuColumnVector where the elements are filled with nulls.
   *
   * NOTE: Besides the non-nested types, the array type is supported.
   *
   * @param count the row number of the output column
   * @param sparkType the data type of the output column
   * @return a GpuColumnVector filled with nulls. It should be closed to avoid memory leak.
   */
  public static GpuColumnVector fromNull(int count, DataType sparkType) {
    return GpuColumnVector.from(columnVectorFromNull(count, sparkType), sparkType);
  }

  /**
   * Get the underlying cudf columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static ai.rapids.cudf.ColumnVector[] extractBases(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    ai.rapids.cudf.ColumnVector[] vectors = new ai.rapids.cudf.ColumnVector[numColumns];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((GpuColumnVector)batch.column(i)).getBase();
    }
    return vectors;
  }

  /**
   * Tag a batch that it is known to be the final batch for a partition.
   */
  public static ColumnarBatch tagAsFinalBatch(ColumnarBatch batch) {
    int numCols = batch.numCols();
    for (int col = 0; col < numCols; col++) {
      ((GpuColumnVectorBase)batch.column(col)).setFinalBatch(true);
    }
    return batch;
  }

  /**
   * Check if a batch is tagged as being the final batch in a partition.
   */
  public static boolean isTaggedAsFinalBatch(ColumnarBatch batch) {
    int numCols = batch.numCols();
    if (numCols <= 0) {
      return false;
    }
    for (int col = 0; col < numCols; col++) {
      if (!((GpuColumnVectorBase)batch.column(col)).isKnownFinalBatch()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Increment the reference count for all columns in the input batch.
   */
  public static ColumnarBatch incRefCounts(ColumnarBatch batch) {
    for (ai.rapids.cudf.ColumnVector cv: extractBases(batch)) {
      cv.incRefCount();
    }
    return batch;
  }

  /**
   * Take the columns from all of the batches passed in and put them in a single batch. The order
   * of the columns is preserved.
   * <br/>
   * For example if we had <pre>combineColumns({A, B}, {C, D})</pre> The result would be a single
   * batch with <pre>{A, B, C, D}</pre>
   */
  public static ColumnarBatch combineColumns(ColumnarBatch ... batches) {
    boolean isFirst = true;
    int numRows = 0;
    ArrayList<ColumnVector> columns = new ArrayList<>();
    for (ColumnarBatch cb: batches) {
      if (isFirst) {
        numRows = cb.numRows();
        isFirst = false;
      } else {
        assert cb.numRows() == numRows : "Rows do not match expected " + numRows + " found " +
            cb.numRows();
      }
      int numColumns = cb.numCols();
      for (int i = 0; i < numColumns; i++) {
        columns.add(cb.column(i));
      }
    }
    ColumnarBatch ret = new ColumnarBatch(columns.toArray(new ColumnVector[columns.size()]), numRows);
    return incRefCounts(ret);
  }

  public static ColumnarBatch appendColumns(ColumnarBatch cb, GpuColumnVector ... vectors) {
    final int numRows = cb.numRows();
    final int numCbColumns = cb.numCols();
    ArrayList<ColumnVector> columns = new ArrayList<>(numCbColumns + vectors.length);
    for (int i = 0; i < numCbColumns; i++) {
      columns.add(cb.column(i));
    }
    for (GpuColumnVector cv: vectors) {
      assert cv.getBase().getRowCount() == numRows : "Rows do not match expected " + numRows + " found " +
          cv.getBase().getRowCount();
      columns.add(cv);
    }
    ColumnarBatch ret = new ColumnarBatch(columns.toArray(new ColumnVector[columns.size()]), numRows);
    return incRefCounts(ret);
  }

  /**
   * Remove columns from the batch.  The order of the remaining columns is preserved.
   * dropList[] has an entry for each column in the batch which indicates whether the column should
   * be skipped.  The reference counts are incremented for the retained columns.
   * <br/>
   * For example if we had <pre>dropColumns({A, B, C, D}, {true, false, true, false})</pre>
   * The result would be a batch with <pre>{B, D}</pre>
   */
  public static ColumnarBatch dropColumns(ColumnarBatch cb, boolean[] dropList) {
    int numRows = cb.numRows();
    int numColumns = cb.numCols();
    assert numColumns == dropList.length;
    ArrayList<ColumnVector> columns = new ArrayList<>();
    for (int i = 0; i < numColumns; i++) {
      if (dropList[i] == false) {
        columns.add(cb.column(i));
      }
    }
    ColumnarBatch ret =
        new ColumnarBatch(columns.toArray(new ColumnVector[columns.size()]),numRows);
    return incRefCounts(ret);
  }

  /**
   * Get the underlying Spark compatible columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static GpuColumnVector[] extractColumns(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    GpuColumnVector[] vectors = new GpuColumnVector[numColumns];

    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((GpuColumnVector)batch.column(i));
    }
    return vectors;
  }

  /**
   * Convert the table into columns and return them, outside of a ColumnarBatch.
   * @param colType the types of the columns.
   */
  public static GpuColumnVector[] extractColumns(Table table, DataType[] colType) {
    try (ColumnarBatch batch = from(table, colType)) {
      return extractColumns(batch);
    }
  }

  /**
   * Convert the table into host columns and return them, outside of a ColumnarBatch.
   * @param colType the types of the columns.
   */
  public static RapidsHostColumnVector[] extractHostColumns(Table table, DataType[] colType) {
    try (ColumnarBatch batch = from(table, colType)) {
      GpuColumnVector[] gpuCols = extractColumns(batch);
      RapidsHostColumnVector[] hostCols = new RapidsHostColumnVector[gpuCols.length];
      try {
        for (int i = 0; i < gpuCols.length; i++) {
          hostCols[i] = gpuCols[i].copyToHostAsync(Cuda.DEFAULT_STREAM);
        }
        Cuda.DEFAULT_STREAM.sync();
      } catch (Exception e) {
        // Async D->H copies may still be in flight into the host buffers owned
        // by hostCols; sync before closing to avoid a use-after-free on pinned
        // memory.
        boolean drained = false;
        try {
          Cuda.DEFAULT_STREAM.sync();
          drained = true;
        } catch (Exception syncEx) {
          e.addSuppressed(syncEx);
        }
        // If the recovery sync threw, the CUDA stream is in an error state
        // and there is no guarantee that pending DMA writes into the host
        // buffers have completed. Closing them now risks the DMA engine
        // writing into freed memory. Leak the host buffers instead — the
        // process is already in a broken state (sticky CUDA error typically
        // requires a restart), and a leak is preferable to silent corruption.
        if (drained) {
          for (RapidsHostColumnVector hostCol : hostCols) {
            if (hostCol != null) {
              try {
                hostCol.close();
              } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
              }
            }
          }
        }
        throw e;
      }
      return hostCols;
    }
  }

    /**
     * A wrapper method for Table::filter.
     *
     * @param batch Input columnar batch to filter. This method will not close it, so it's caller's responsibility to
     *              close it.
     * @param mask  A boolean column vector, used to filter rows.
     * @param dataTypes Data types of `batch`. We add this parameter to avoid repeated allocation of array.
     * @return Filtered columnar batch.
     */
    public static ColumnarBatch filter(ColumnarBatch batch, DataType[] dataTypes, ColumnView mask) {
        if (dataTypes.length == 0) {
            try(Scalar s = mask.sum(DType.INT64)) {
                int numRows = Math.toIntExact(s.getLong());
                return new ColumnarBatch(new ColumnVector[0], numRows);
            }
        } else {
            try(Table cudfTable = GpuColumnVector.from(batch)) {
                try(Table filteredTable = cudfTable.filter(mask)) {
                    return GpuColumnVector.from(filteredTable, dataTypes);
                }
            }
        }
    }

  private final ai.rapids.cudf.ColumnVector cudfCv;

  /**
   * Take an INT32 column vector and return a host side int array.  Don't use this for anything
   * too large.  Note that this ignores validity totally.
   */
  public static int[] toIntArray(ai.rapids.cudf.ColumnView vec) {
    assert vec.getType() == DType.INT32;
    int rowCount = (int)vec.getRowCount();
    int[] output = new int[rowCount];
    try (HostColumnVector h = vec.copyToHost()) {
      for (int i = 0; i < rowCount; i++) {
        output[i] = h.getInt(i);
      }
    }
    return output;
  }

  /**
   * Sets up the data type of this column vector.
   */
  GpuColumnVector(DataType type, ai.rapids.cudf.ColumnVector cudfCv) {
    super(type);
    this.cudfCv = cudfCv;
  }

  public final GpuColumnVector incRefCount() {
    // Just pass through the reference counting
    cudfCv.incRefCount();
    return this;
  }

  @Override
  public final void close() {
    // Just pass through the reference counting
    cudfCv.close();
  }

  @Override
  public final boolean hasNull() {
    return cudfCv.hasNulls();
  }

  @Override
  public final int numNulls() {
    return (int) cudfCv.getNullCount();
  }

  public static long getTotalDeviceMemoryUsed(ColumnarBatch batch) {
    long sum = 0;
    if (batch.numCols() == 1 && batch.column(0) instanceof GpuPackedTableColumn) {
      // this is a special case for a packed batch
      return ((GpuPackedTableColumn) batch.column(0)).getTableBuffer().getLength();
    }
    if (batch.numCols() > 0) {
      if (batch.column(0) instanceof WithTableBuffer) {
        WithTableBuffer wtb = (WithTableBuffer) batch.column(0);
        sum += wtb.getTableBuffer().getLength();
      } else {
        HashSet<Long> found = new HashSet<>();
        for (int i = 0; i < batch.numCols(); i++) {
          ai.rapids.cudf.ColumnVector cv = ((GpuColumnVector)batch.column(i)).getBase();
          long id = cv.getNativeView();
          if (found.add(id)) {
            sum += cv.getDeviceMemorySize();
          }
        }
      }
    }
    return sum;
  }

  public static long getTotalDeviceMemoryUsed(GpuColumnVector[] vectors) {
    long sum = 0;
    HashSet<Long> found = new HashSet<>();
    for (GpuColumnVector vector : vectors) {
      ai.rapids.cudf.ColumnVector cv = vector.getBase();
      long id = cv.getNativeView();
      if (found.add(id)) {
        sum += cv.getDeviceMemorySize();
      }
    }
    return sum;
  }

  public static long getTotalDeviceMemoryUsed(Table table) {
    long sum = 0;
    int len = table.getNumberOfColumns();
    // Deduplicate columns that are the same
    HashSet<Long> found = new HashSet<>();
    for (int i = 0; i < len; i++) {
      ai.rapids.cudf.ColumnVector cv = table.getColumn(i);
      long id = cv.getNativeView();
      if (found.add(id)) {
        sum += cv.getDeviceMemorySize();
      }
    }
    return sum;
  }


  public final ai.rapids.cudf.ColumnVector getBase() {
    return cudfCv;
  }

  public final long getRowCount() { return cudfCv.getRowCount(); }

  public final RapidsHostColumnVector copyToHost() {
    return new RapidsHostColumnVector(type, cudfCv.copyToHost());
  }

  public final RapidsHostColumnVector copyToHostAsync(Cuda.Stream stream) {
    return new RapidsHostColumnVector(type, cudfCv.copyToHostAsync(stream));
  }

  public final RapidsNullSafeHostColumnVector copyToNullSafeHost() {
    return new RapidsNullSafeHostColumnVector(copyToHost());
  }

  @Override
  public final String toString() {
    return getBase().toString();
  }
}
