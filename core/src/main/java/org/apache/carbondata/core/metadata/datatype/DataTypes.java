/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.metadata.datatype;

/**
 * Holds all singleton object for all data type used in carbon
 */
public class DataTypes {

  // singleton for each data type
  public static final DataType STRING = StringType.STRING;
  public static final DataType DATE = DateType.DATE;
  public static final DataType TIMESTAMP = TimestampType.TIMESTAMP;
  public static final DataType BOOLEAN = BooleanType.BOOLEAN;
  public static final DataType SHORT = ShortType.SHORT;
  public static final DataType INT = IntType.INT;
  public static final DataType FLOAT = FloatType.FLOAT;
  public static final DataType LONG = LongType.LONG;
  public static final DataType DOUBLE = DoubleType.DOUBLE;
  public static final DataType NULL = NullType.NULL;
  public static final DataType BYTE = ByteType.BYTE;

  // internal use only, for variable length data type
  public static final DataType BYTE_ARRAY = ByteArrayType.BYTE_ARRAY;

  // internal use only, for value compression from integer/long to 3 bytes value
  public static final DataType SHORT_INT = ShortIntType.SHORT_INT;

  // Only for internal use for backward compatability. It is only used for V1 version
  public static final DataType LEGACY_LONG = LegacyLongType.LEGACY_LONG;

  public static final DataType ARRAY = ArrayType.ARRAY;
  public static final DataType STRUCT = StructType.STRUCT;
  public static final DataType MAP = MapType.MAP;

  // these IDs are used within this package only
  static final int STRING_TYPE_ID = 0;
  static final int DATE_TYPE_ID = 1;
  static final int TIMESTAMP_TYPE_ID = 2;
  static final int BOOLEAN_TYPE_ID = 3;
  static final int SHORT_TYPE_ID = 4;
  static final int INT_TYPE_ID = 5;
  static final int FLOAT_TYPE_ID = 6;
  static final int LONG_TYPE_ID = 7;
  static final int DOUBLE_TYPE_ID = 8;
  static final int NULL_TYPE_ID = 9;
  static final int BYTE_TYPE_ID = 10;
  static final int BYTE_ARRAY_TYPE_ID = 11;
  static final int SHORT_INT_TYPE_ID = 12;
  static final int LEGACY_LONG_TYPE_ID = 13;
  static final int DECIMAL_TYPE_ID = 20;
  static final int ARRAY_TYPE_ID = 21;
  static final int STRUCT_TYPE_ID = 22;
  static final int MAP_TYPE_ID = 23;

  /**
   * create a DataType instance from uniqueId of the DataType
   */
  public static DataType valueOf(int id) {
    if (id == STRING.getId()) {
      return STRING;
    } else if (id == DATE.getId()) {
      return DATE;
    } else if (id == TIMESTAMP.getId()) {
      return TIMESTAMP;
    } else if (id == BOOLEAN.getId()) {
      return BOOLEAN;
    } else if (id == BYTE.getId()) {
      return BYTE;
    } else if (id == SHORT.getId()) {
      return SHORT;
    } else if (id == SHORT_INT.getId()) {
      return SHORT_INT;
    } else if (id == INT.getId()) {
      return INT;
    } else if (id == LONG.getId()) {
      return LONG;
    } else if (id == LEGACY_LONG.getId()) {
      return LEGACY_LONG;
    } else if (id == FLOAT.getId()) {
      return FLOAT;
    } else if (id == DOUBLE.getId()) {
      return DOUBLE;
    } else if (id == NULL.getId()) {
      return NULL;
    } else if (id == DECIMAL_TYPE_ID) {
      return createDefaultDecimalType();
    } else if (id == ARRAY.getId()) {
      return ARRAY;
    } else if (id == STRUCT.getId()) {
      return STRUCT;
    } else if (id == MAP.getId()) {
      return MAP;
    } else if (id == BYTE_ARRAY.getId()) {
      return BYTE_ARRAY;
    } else {
      throw new RuntimeException("create DataType with invalid id: " + id);
    }
  }

  /**
   * create a decimal type object with specified precision and scale
   */
  public static DecimalType createDecimalType(int precision, int scale) {
    return new DecimalType(precision, scale);
  }

  /**
   * create a decimal type object with default precision = 10 and scale = 2
   */
  public static DecimalType createDefaultDecimalType() {
    return new DecimalType(10, 2);
  }

  public static boolean isDecimal(DataType dataType) {
    return dataType.getId() == DECIMAL_TYPE_ID;
  }

}
