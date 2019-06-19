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
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetPageFilterFileReader extends ParquetFileReader {
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
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    this.currentRowGroup = new ColumnChunkPageReadStore(block.getRowCount());
    // prepare the list of consecutive chunks to read them in one scan
    List<ParquetPageFilterFileReader.ConsecutivePageFilterChunkList> allChunks
      = new ArrayList<ParquetPageFilterFileReader.ConsecutivePageFilterChunkList>();
    ParquetPageFilterFileReader.ConsecutivePageFilterChunkList currentChunks = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        long startingPos = mc.getStartingPos();
        // first chunk or not consecutive => new list
        if (currentChunks == null || currentChunks.endPos() != startingPos) {
          currentChunks = new ParquetPageFilterFileReader.ConsecutivePageFilterChunkList(startingPos);
          allChunks.add(currentChunks);
        }
        currentChunks.addChunk(new ChunkDescriptor(columnDescriptor, mc, startingPos, (int)mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    for (ParquetPageFilterFileReader.ConsecutivePageFilterChunkList consecutiveChunks : allChunks) {
      final List<Chunk> chunks = consecutiveChunks.readAll(f);
      for (Chunk chunk : chunks) {
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

    /**
     * @param offset where the first chunk starts
     */
    ConsecutivePageFilterChunkList(long offset) {
      this.offset = offset;
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
    public List<Chunk> readAll(SeekableInputStream f) throws IOException {
      List<Chunk> result = new ArrayList<Chunk>(chunks.size());
      f.seek(offset);
      byte[] chunksBytes = new byte[length];
      f.readFully(chunksBytes);
      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      int currentChunkOffset = 0;
      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        if (i < chunks.size() - 1) {
          result.add(new Chunk(descriptor, chunksBytes, currentChunkOffset));
        } else {
          // because of a bug, the last chunk might be larger than descriptor.size
          result.add(new WorkaroundChunk(descriptor, chunksBytes, currentChunkOffset, f));
        }
        currentChunkOffset += descriptor.size;
      }
      return result ;
    }

    /**
     * @return the position following the last byte of these chunks
     */
    public long endPos() {
      return offset + length;
    }

  }
}
