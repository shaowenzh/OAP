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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.util.TimeZone;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.hadoop.OapParquetDataPageV1;
import org.apache.parquet.hadoop.OapParquetDataPageV2;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;

public class SkippableVectorizedColumnReaderV2 extends SkippableVectorizedColumnReader {
  private static final Logger LOG =
    LoggerFactory.getLogger(SkippableVectorizedColumnReaderV2.class);
  private int currentRowNumber = 0;
  private int currentPageFinalRowNumber = 0;

  public SkippableVectorizedColumnReaderV2(
    ColumnDescriptor descriptor,
    OriginalType originalType,
    PageReader pageReader,
    TimeZone convertTz) throws IOException {
    super(descriptor, originalType, pageReader, convertTz);
  }

  public void skipBatchAndUpdateRowNum(int total, DataType dataType) throws IOException {
    this.skipBatch(total, dataType);
    currentRowNumber += total;
  }

  public long getCurrentRowNumber() {
    return currentRowNumber;
  }

  public boolean hasDictionary() {return this.dictionary != null; }

  public void readBatch(
    long start,
    int total,
    WritableColumnVector column,
    WritableColumnVector dictionaryIds,
    DataType dataType,
    int columnRowId) throws IOException {

    // LOG.info("start: " + start + "currentPageFinalRowNumber" + this.currentPageFinalRowNumber);
    while(start > this.currentPageFinalRowNumber) {
      readPage();
    }

    if (currentRowNumber < start) {
      int rowNeedToSkip = (int)(start - currentRowNumber);
      skipBatch(rowNeedToSkip, dataType);
      currentRowNumber += rowNeedToSkip;
    }
    readBatch(total, column, dictionaryIds, columnRowId);
    currentRowNumber += total;
  }

  public void readBatch(
    int total, WritableColumnVector column,
    WritableColumnVector dictionaryIds,
    int columnRowId) throws IOException {
    int rowId = columnRowId;
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      if (isCurrentPageDictionaryEncoded) {
        // Read and decode dictionary ids.
        defColumn.readIntegers(
          num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);

        // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post process
        // the values to add microseconds precision.
        if (column.hasDictionary() || (rowId == 0 &&
          (descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT32 ||
            (descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64  &&
              originalType != OriginalType.TIMESTAMP_MILLIS) ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.FLOAT ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.DOUBLE ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.BINARY))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
          // non-dictionary encoded values have already been added).
          column.setDictionary(new ParquetDictionaryWrapper(dictionary));
        } else {
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        }
      } else {
        if (column.hasDictionary() && rowId != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          decodeDictionaryIds(0, rowId, column, column.getDictionaryIds());
        }
        column.setDictionary(null);
        switch (descriptor.getType()) {
          case BOOLEAN:
            readBooleanBatch(rowId, num, column);
            break;
          case INT32:
            readIntBatch(rowId, num, column);
            break;
          case INT64:
            readLongBatch(rowId, num, column);
            break;
          case INT96:
            readBinaryBatch(rowId, num, column);
            break;
          case FLOAT:
            readFloatBatch(rowId, num, column);
            break;
          case DOUBLE:
            readDoubleBatch(rowId, num, column);
            break;
          case BINARY:
            readBinaryBatch(rowId, num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            readFixedLenByteArrayBatch(rowId, num, column, descriptor.getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + descriptor.getType());
        }
      }

      valuesRead += num;
      rowId += num;
      total -= num;
    }
  }

  @Override
  protected void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new SkippableVectorizedRleValuesReader(bitWidth);
    // defColumnRef reference defColumn and type is SkippableVectorizedRleValuesReader
    this.defColumnRef = (SkippableVectorizedRleValuesReader)this.defColumn;
    dlReader = this.defColumn;
    try {
      byte[] bytes = page.getBytes().toByteArray();
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      initDataReader(page.getValueEncoding(), bytes, next);

      if (page instanceof OapParquetDataPageV1) {
        this.currentRowNumber = ((OapParquetDataPageV1) page).getPageRowStartNum();
        this.currentPageFinalRowNumber = this.currentRowNumber + this.pageValueCount;
      } else {
        throw new IllegalArgumentException("page should be in OapParquetDataPageV1 format");
      }

    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  /**
   * This method refer to readPageV2 in VectorizedColumnReader,
   * modified the assignment of defColumn and remove assignment to
   * repetitionLevelColumn & definitionLevelColumn because they are useless.
   */
  @Override
  protected void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new SkippableVectorizedRleValuesReader(bitWidth);
    this.defColumn.initFromBuffer(
      this.pageValueCount, page.getDefinitionLevels().toByteArray());
    // defColumnRef reference defColumn and type is SkippableVectorizedRleValuesReader
    this.defColumnRef = (SkippableVectorizedRleValuesReader) this.defColumn;
    try {
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
      if (page instanceof OapParquetDataPageV2) {
        this.currentRowNumber = ((OapParquetDataPageV2) page).getPageRowStartNum();
        currentPageFinalRowNumber = this.currentRowNumber + this.pageValueCount;
      } else {
        throw new IllegalArgumentException("page should be in OapParquetDataPageV2 format");
      }
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
