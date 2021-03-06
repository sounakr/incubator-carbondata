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

package org.apache.carbondata.core.datastore.page.encoding;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This codec will do type casting on page data to make storage minimum.
 */
class AdaptiveIntegerCodec extends AdaptiveCompressionCodec {

  private ColumnPage encodedPage;

  public static ColumnPageCodec newInstance(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats, Compressor compressor) {
    return new AdaptiveIntegerCodec(srcDataType, targetDataType, stats, compressor);
  }

  private AdaptiveIntegerCodec(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats, Compressor compressor) {
    super(srcDataType, targetDataType, stats, compressor);
  }

  @Override
  public String getName() {
    return "AdaptiveIntegerCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) throws MemoryException {
    if (srcDataType.equals(targetDataType)) {
      return input.compress(compressor);
    } else {
      encodedPage = ColumnPage.newPage(targetDataType, input.getPageSize());
      input.encode(codec);
      byte[] result = encodedPage.compress(compressor);
      encodedPage.freeMemory();
      return result;
    }
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    if (srcDataType.equals(targetDataType)) {
      return ColumnPage.decompress(compressor, targetDataType, input, offset, length);
    } else {
      ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length);
      return LazyColumnPage.newPage(page, codec);
    }
  }

  // encoded value = (type cast page value to target data type)
  private PrimitiveCodec codec = new PrimitiveCodec() {
    @Override
    public void encode(int rowId, byte value) {
      switch (targetDataType) {
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, short value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, int value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, long value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case INT:
          encodedPage.putInt(rowId, (int) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, float value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case INT:
          encodedPage.putInt(rowId, (int) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case INT:
          encodedPage.putInt(rowId, (int) value);
          break;
        case LONG:
          encodedPage.putLong(rowId, (long) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public long decodeLong(byte value) {
      return value;
    }

    @Override
    public long decodeLong(short value) {
      return value;
    }

    @Override
    public long decodeLong(int value) {
      return value;
    }

    @Override
    public double decodeDouble(byte value) {
      return value;
    }

    @Override
    public double decodeDouble(short value) {
      return value;
    }

    @Override
    public double decodeDouble(int value) {
      return value;
    }

    @Override
    public double decodeDouble(long value) {
      return value;
    }

    @Override
    public double decodeDouble(float value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }
  };

}
