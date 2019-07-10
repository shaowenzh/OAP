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

package org.apache.spark.sql.execution.datasources.oap.index

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils

class IndexCreateSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  // TODO move Parquet TestSuite from FilterSuite
  import testImplicits._

  private var currentParquetPath: String = _
  private var defaultEis: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    // In this suite we don't want to skip index even if the cost is higher.
    defaultEis = sqlContext.conf.getConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION)
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, false)
  }

  override def afterAll(): Unit = {
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, defaultEis)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentParquetPath = path + "/test"
  }

  test("OAP#1061 Index path fault for path like /*.parquet") {
    val dataDF =
      (1 to 300).map { i => (i, s"this is test $i") }.toDF("a", "b")
    dataDF.write.parquet(currentParquetPath);
    spark.read.parquet(s"${currentParquetPath}/*.parquet")
      .createOrReplaceTempView("parquet_test")
    withIndex(TestIndex("parquet_test", "indexA")) {
      // create index
      sql("create oindex indexA on parquet_test (a)")
      val beforeQuery = OapRuntime.getOrCreate.fiberCacheManager.cacheStats.indexFiberCount
      val df = sql("SELECT b FROM parquet_test WHERE a = 1 or a = 2")
      checkAnswer(df, Row("this is test 1") :: Row("this is test 2") :: Nil)
      val afterQuery = OapRuntime.getOrCreate.fiberCacheManager.cacheStats.indexFiberCount
      assert(afterQuery == beforeQuery + 4)
    }
    sqlContext.dropTempTable("parquet_test")
  }
}
