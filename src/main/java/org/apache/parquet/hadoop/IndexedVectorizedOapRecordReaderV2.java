package org.apache.parquet.hadoop;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.IndexedBlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.utils.Collections3;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper;
import org.apache.spark.sql.execution.datasources.parquet.SkippableVectorizedColumnReaderV2;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType$;

public class IndexedVectorizedOapRecordReaderV2 extends IndexedVectorizedOapRecordReader{
  private static final Logger LOG =
    LoggerFactory.getLogger(IndexedVectorizedOapRecordReaderV2.class);
  private int currentRowGroupNum = 0;

  private IntList currentRowNeedList;

  private ParquetMetadata filteredMeta;

  public IndexedVectorizedOapRecordReaderV2(
    Path file,
    Configuration configuration,
    ParquetFooter footer,
    int[] globalRowIds) {
    super(file, configuration, footer, globalRowIds);
  }

  public IndexedVectorizedOapRecordReaderV2(
    Path file,
    Configuration configuration,
    int[] globalRowIds) throws IOException {
    super(file, configuration, globalRowIds);
  }

  @Override
  public void initialize() throws IOException, InterruptedException {
    this.filteredMeta = footer.toParquetMetadata(globalRowIds);
    this.fileSchema = filteredMeta.getFileMetaData().getSchema();
    Map<String, String> fileMetadata = filteredMeta.getFileMetaData().getKeyValueMetaData();
    ReadSupport.ReadContext readContext = new ParquetReadSupportWrapper().init(new InitContext(
      configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema));
    this.requestedSchema = readContext.getRequestedSchema();
    String sparkRequestedSchemaString =
      configuration.get(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA());
    this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
    this.reader = OapParquetPageFilterFileReader.open(configuration, file, filteredMeta);
    this.reader.setRequestedSchema(requestedSchema);

    super.initializeInternal();
    this.totalRowCount = globalRowIds.length;
  }

  @Override
  public boolean nextBatch() throws IOException {
    if (rowsReturned >= totalRowCount) {
      return false;
    }
    checkEndOfRowGroup();
    nextBatchBasedOnNeedRow();
    return true;
  }

  @Override
  protected void readNextRowGroup() throws IOException {
    OapParquetFileReader.RowGroupDataAndRowIds rowGroupDataAndRowIds =
      reader.readNextRowGroupAndRowIds();
    initColumnReaders(rowGroupDataAndRowIds.getPageReadStore());
    currentRowGroupNum++;
  }

  @Override
  protected void initColumnReaders(PageReadStore pages) throws IOException {
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
        + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<Type> types = requestedSchema.asGroupType().getFields();
    columnReaders = new SkippableVectorizedColumnReaderV2[columns.size()];
    for (int i = 0; i < columns.size(); ++i) {
      if (missingColumns[i]) continue;
      columnReaders[i] = new SkippableVectorizedColumnReaderV2(columns.get(i),
        types.get(i).getOriginalType(), pages.getPageReader(columns.get(i)),
        TimeZone.getDefault());
    }
    totalCountLoadedSoFar +=
      ((IndexedBlockMetaData)this.filteredMeta.getBlocks().get(currentRowGroupNum))
        .getNeedRowIds().size();
    currentRowNeedList =
      ((IndexedBlockMetaData)this.filteredMeta.getBlocks().get(currentRowGroupNum))
        .getNeedRowIds();
  }

  private void nextBatchBasedOnNeedRow() throws IOException {
    TreeMap<Integer, Integer> rowFetchTreeMap = rowIdsToFetchList();
    TreeMap<Integer, Integer> tempRowTreeMap = rowIdsToFetchList();
    ArrayList<Integer> treeMapRemoveList = new ArrayList<>();
    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(0);

    int num = (int) Math.min((long) CAPACITY,
      totalCountLoadedSoFar - rowsReturned);

    int retrieveCount = 0;

    //Todo need to optimize this part of code
    for (Map.Entry<Integer, Integer> entry : rowFetchTreeMap.entrySet()) {
      retrieveCount += entry.getValue();
      if (retrieveCount > num) {
        tempRowTreeMap.putIfAbsent(entry.getKey().intValue(), num);
        treeMapRemoveList.add(entry.getKey());
        rowFetchTreeMap.putIfAbsent(
          entry.getKey() + retrieveCount - num, entry.getValue() - (retrieveCount - num));
      } else if (retrieveCount <= num) {
        tempRowTreeMap.putIfAbsent(entry.getKey().intValue(), entry.getValue().intValue());
        treeMapRemoveList.add(entry.getKey());
      }
    }

    for (Integer v : treeMapRemoveList) {
      rowFetchTreeMap.remove(v);
    }

    for (int i = 0; i < columnReaders.length; ++i) {
      if (columnReaders[i] == null) continue;
      WritableColumnVector dictionaryIds = null;
      if (((SkippableVectorizedColumnReaderV2)columnReaders[i]).hasDictionary()) {
        dictionaryIds = columnVectors[i].reserveDictionaryIds(num);
      }

      int columnRowId = 0;
      for (Map.Entry<Integer, Integer> entry : tempRowTreeMap.entrySet()) {
          ((SkippableVectorizedColumnReaderV2)columnReaders[i]).readBatch(
            entry.getKey(),
            entry.getValue(),
            columnVectors[i],
            dictionaryIds,
            columnarBatch.column(i).dataType(),
            columnRowId);
        columnRowId += entry.getValue();
      }
    }
    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
  }

  private TreeMap<Integer, Integer> rowIdsToFetchList() {
    LOG.info("first row: " +
      currentRowNeedList.get(0) +
      "last row: " + currentRowNeedList.get(currentRowNeedList.size() - 1));
    // value in RowNeedList should be all different and ascending
    TreeMap<Integer, Integer> rowFetchHashSet = new TreeMap<Integer, Integer>();
    if (currentRowNeedList.size() == 0) {
      return rowFetchHashSet;
    }
    int start = currentRowNeedList.get(0);
    int prev = start;
    int count = 1;
    for(int v: currentRowNeedList) {
      int diff = v - prev;
      if (diff == 1) {
        count++;
      } else if (diff > 1) {
        rowFetchHashSet.putIfAbsent(start, count);
        start = v;
        count = 1;
      } else if (diff < 0) {
        throw new IllegalArgumentException("not ascending needRowIds list for this row group");
      }
      prev = v;
    }
    rowFetchHashSet.putIfAbsent(start, count);
    return rowFetchHashSet;
  }
}
