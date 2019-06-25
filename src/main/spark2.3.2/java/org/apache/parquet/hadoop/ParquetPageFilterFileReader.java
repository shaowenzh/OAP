/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.format.*;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.format.PageType.DATA_PAGE;
import static org.apache.parquet.format.PageType.DATA_PAGE_V2;
import static org.apache.parquet.format.PageType.DICTIONARY_PAGE;

public class ParquetPageFilterFileReader extends ParquetFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetPageFilterFileReader.class);

  public ParquetPageFilterFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    super(configuration, filePath, blocks, columns);
  }

  public ParquetPageFilterFileReader(Configuration configuration, FileMetaData fileMetaData, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    super(configuration, fileMetaData, filePath, blocks, columns);
  }

  public ParquetPageFilterFileReader(Configuration conf, Path file, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    super(conf, file, filter);
  }

  public ParquetPageFilterFileReader(Configuration conf, Path file, ParquetMetadata footer) throws IOException {
    super(conf, file, footer);
  }

  @Override
  public PageReadStore readNextRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    IndexedBlockMetaData block = (IndexedBlockMetaData)blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    this.currentRowGroup = new ColumnChunkPageReadStore(block.getRowCount());
    // prepare the list of consecutive chunks to read them in one scan
    List<ConsecutivePageFilterChunkList> allChunks
      = new ArrayList<ConsecutivePageFilterChunkList>();
    ConsecutivePageFilterChunkList currentChunks = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        long startingPos = mc.getStartingPos();
        // first chunk or not consecutive => new list
        if (currentChunks == null || currentChunks.endPos() != startingPos) {
          currentChunks = new ConsecutivePageFilterChunkList(startingPos, block.getNeedRowIds());
          allChunks.add(currentChunks);
        }
        currentChunks.addChunk(new ChunkDescriptor(columnDescriptor, mc, startingPos, (int)mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    for (ParquetPageFilterFileReader.ConsecutivePageFilterChunkList consecutiveChunks : allChunks) {
      final List<OapChunk> chunks = consecutiveChunks.readAll(f);
      for (OapChunk chunk : chunks) {
        currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
      }
    }

    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  class OapChunk {

    protected final ChunkDescriptor descriptor;

    private List<DataPage> pagesInChunk = new ArrayList<DataPage>();

    DictionaryPage dictionaryPage = null;

    PrimitiveType type = null;

    public OapChunk(ChunkDescriptor descriptor) {
      this.descriptor = descriptor;
      this.type = getFileMetaData().getSchema()
        .getType(descriptor.col.getPath()).asPrimitiveType();
    }

    public void readInSinglePage(DataPage datapage) {
      pagesInChunk.add(datapage);
    }

    public void setDictionaryPage(DictionaryPage dicPage) {
      if (dictionaryPage != null) {
        throw new ParquetDecodingException("more than one dictionary page in column " + descriptor.col);
      }
      dictionaryPage = dicPage;
    }

    public ColumnChunkPageReader.ColumnChunkPageReaderV2 readAllPages() {
      // LOG.info("pages In Chunk: " + pagesInChunk.size());
      CodecFactory.BytesDecompressor decompressor = codecFactory.getDecompressor(descriptor.metadata.getCodec());
      return new ColumnChunkPageReader.ColumnChunkPageReaderV2(decompressor, pagesInChunk, dictionaryPage);
    }
  }

  /**
   * information needed to read a column chunk
   */
  static class ChunkDescriptorWithDicCheck extends ChunkDescriptor {

    /**
     * @param col        column this chunk is part of
     * @param metadata   metadata for the column
     * @param fileOffset offset in the file where this chunk starts
     * @param size       size of the chunk
     */
    protected ChunkDescriptorWithDicCheck(
      ColumnDescriptor col,
      ColumnChunkMetaData metadata,
      long fileOffset,
      int size) {
      super(col, metadata, fileOffset, size);
    }

    public boolean isDicEncoding() {
      return this.metadata.getEncodings().contains(Encoding.PLAIN_DICTIONARY) ||
        this.metadata.getEncodings().contains(Encoding.RLE_DICTIONARY);
    }
  }

  /**
   * describes a list of consecutive column chunks to be read at once.
   *
   * @author Julien Le Dem
   */
  class ConsecutivePageFilterChunkList {

    private final long offset;
    private int length;
    private final List<ChunkDescriptor> chunks = new ArrayList<ChunkDescriptor>();
    private IntList rowIdsList;

    /**
     * @param offset where the first chunk starts
     */
    ConsecutivePageFilterChunkList(long offset, IntList rowIdsList) {
      this.offset = offset;
      this.rowIdsList = rowIdsList;
    }

    /**
     * adds a chunk to the list.
     * It must be consecutive to the previous chunk
     * @param descriptor
     */
    public void addChunk(ChunkDescriptor descriptor) {
      chunks.add(descriptor);
      length += descriptor.size;
    }

    /**
     * @param f file to read the chunks from
     * @return the chunks
     * @throws IOException
     */
    public List<OapChunk> readAll(SeekableInputStream f) throws IOException {
      int minRowId = rowIdsList.get(0);
      int maxRowId = rowIdsList.get(rowIdsList.size() - 1);
      int rowIdCount = this.rowIdsList.size();
      int pageValue, lowLimit, upLimit;
      long tempOffset = this.offset;

      List<OapChunk> result = new ArrayList<OapChunk>(chunks.size());

      f.seek(tempOffset);

      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        PrimitiveType type = getFileMetaData().getSchema()
          .getType(descriptor.col.getPath()).asPrimitiveType();
        OapChunk chunk = new OapChunk(descriptor);
        int currentChunkRowOffset = 0;


        int pageNum = 0;

        while (currentChunkRowOffset < descriptor.metadata.getValueCount()) {
          PageHeader pageHeader = Util.readPageHeader(f);
          tempOffset = f.getPos();
          int uncompressedPageSize = pageHeader.getUncompressed_page_size();
          int compressedPageSize = pageHeader.getCompressed_page_size();
          if (pageHeader.type == DICTIONARY_PAGE) {
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            byte[] pageBytes = new byte[compressedPageSize];
            f.readFully(pageBytes);
            chunk.setDictionaryPage(
              new DictionaryPage(
                BytesInput.from(pageBytes),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                converter.getEncoding(dicHeader.getEncoding())
              )
            );
            tempOffset += compressedPageSize;
          } else if (pageHeader.type ==  DATA_PAGE) {
            pageNum++;
            DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
            pageValue = dataHeaderV1.getNum_values();

            lowLimit = currentChunkRowOffset;
            upLimit = currentChunkRowOffset + pageValue;
            boolean isSkip;
            int rowIdIndex = 0;

            if (lowLimit > maxRowId || upLimit < minRowId) {
              isSkip = true;
            } else {
              isSkip = true;
              while (rowIdIndex < rowIdCount) {
                int rowIdValue = this.rowIdsList.get(rowIdIndex);
                if (rowIdValue >= lowLimit && rowIdValue <= upLimit) {
                  isSkip = false;
                  break;
                }
                rowIdIndex++;
              }
            }

            tempOffset += compressedPageSize;
            if (isSkip) {
              f.seek(tempOffset);
            } else {
              byte[] pageBytes = new byte[compressedPageSize];
              f.readFully(pageBytes);
              chunk.readInSinglePage(
                new OapParquetDataPageV1(
                  BytesInput.from(pageBytes),
                  dataHeaderV1.getNum_values(),
                  uncompressedPageSize,
                  converter.fromParquetStatistics(
                    getFileMetaData().getCreatedBy(),
                    dataHeaderV1.getStatistics(),
                    type),
                  converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                  converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                  converter.getEncoding(dataHeaderV1.getEncoding()),
                  currentChunkRowOffset
                ));
            }
            currentChunkRowOffset += pageValue;
          } else if (pageHeader.type ==  DATA_PAGE_V2) {
            DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
            pageValue = dataHeaderV2.getNum_values();

            lowLimit = currentChunkRowOffset;
            upLimit = currentChunkRowOffset + pageValue;
            boolean isSkip;
            int rowIdIndex = 0;

            if (lowLimit > maxRowId || upLimit < minRowId) {
              isSkip = true;
            } else {
              isSkip = true;
              while (rowIdIndex < rowIdCount) {
                int rowIdValue = this.rowIdsList.get(rowIdIndex);
                if (rowIdValue >= lowLimit && rowIdValue <= upLimit) {
                  isSkip = false;
                  break;
                }
                rowIdIndex++;
              }
            }

            tempOffset += compressedPageSize;
            if (isSkip) {
              f.seek(tempOffset);
            } else {
              int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
              int offset = 0;
              byte[] pageBytes = new byte[compressedPageSize];
              f.readFully(pageBytes);
              BytesInput rlBytes = BytesInput.from(pageBytes, offset, dataHeaderV2.getRepetition_levels_byte_length());
              offset += dataHeaderV2.getRepetition_levels_byte_length();
              BytesInput dlBytes = BytesInput.from(pageBytes, offset, dataHeaderV2.getDefinition_levels_byte_length());
              offset += dataHeaderV2.getDefinition_levels_byte_length();
              BytesInput dataBytes = BytesInput.from(pageBytes, offset, dataSize);
              chunk.readInSinglePage(
                new OapParquetDataPageV2(
                  dataHeaderV2.getNum_rows(),
                  dataHeaderV2.getNum_nulls(),
                  dataHeaderV2.getNum_values(),
                  rlBytes,
                  dlBytes,
                  converter.getEncoding(dataHeaderV2.getEncoding()),
                  dataBytes,
                  uncompressedPageSize,
                  converter.fromParquetStatistics(
                    getFileMetaData().getCreatedBy(),
                    dataHeaderV2.getStatistics(),
                    type),
                  dataHeaderV2.isIs_compressed(),
                  currentChunkRowOffset
                ));
            }
            currentChunkRowOffset += pageValue;
          } else {
            LOG.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
            tempOffset += compressedPageSize;
            f.seek(tempOffset);
          }
        }
        // LOG.info("total pages In Chunk: " + pageNum);
        result.add(chunk);
      }

      return result;
    }

    /**
     * @return the position following the last byte of these chunks
     */
    public long endPos() {
      return offset + length;
    }

    private boolean checkIdInRange(int lowLimit, int upLimit) {
      int minRowId = rowIdsList.get(0);
      int maxRowId = rowIdsList.get(rowIdsList.size() - 1);
      if (lowLimit > maxRowId || minRowId < lowLimit) {
        return false;
      }
      int rowIdIndex = 0;
      int rowIdCount = this.rowIdsList.size();
      while (rowIdIndex < rowIdCount) {
        if (this.rowIdsList.get(rowIdIndex) >= lowLimit && this.rowIdsList.get(rowIdIndex) < upLimit) {
          return true;
        }
        rowIdIndex++;
      }
      return false;
    }
  }
}
