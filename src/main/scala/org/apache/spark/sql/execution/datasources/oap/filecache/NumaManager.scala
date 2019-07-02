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

package org.apache.spark.sql.execution.datasources.oap.filecache

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.sql.execution.datasources.oap.utils.PersistentMemoryConfigUtils
import org.apache.spark.sql.internal.oap.OapConf

class NumaManager(sc: SparkContext) extends Logging {
  logWarning(s"initialize numa manager")
  val conf = sc.conf
  val numaCountPerNode = PersistentMemoryConfigUtils.totalNumaNode(conf)
  private val hostToExecutors
  = new ConcurrentHashMap[String, mutable.HashMap[String, Int]]()

  def isNumaPathNeeded(): Boolean = {
    val memoryManagerType =
      conf.get(OapConf.OAP_FIBERCACHE_MEMORY_MANAGER.key, "offheap").toLowerCase
    memoryManagerType match {
      case "pm" => true
      case "mix" => true
      case _ => false
    }
  }

  def getNumaId(executorId: String): Int = {
    hostToExecutors.asScala.foreach{
      v => {
        v._2.get(executorId) match {
          case Some(numaId) =>
            logWarning(s"Get numa id: ${v} for executor id: ${executorId}, ")
            return numaId
        }
      }
    }
    throw new UnsupportedOperationException(
      s"Can't get expected Numa Id for executor ${executorId}")
  }


  def getOrRegisterExecutorNumaId(
    executorId: String,
    host: String): Option[Int] = {
    val removedList = getNodeRemovedList(host)
    removedList.foreach(
      v => removeExecutor(v, host)
    )
    calAndGetExecutorNumaId(executorId, host)
  }

  def calAndGetExecutorNumaId(
    executorId: String,
    host: String): Option[Int] = {
    if (this.hostToExecutors.containsKey(host)) {
      if (!this.hostToExecutors.get(host).keySet.contains(executorId)) {
        (0 until this.numaCountPerNode).foreach {
          v =>
            logWarning(
              s"executor id: ${executorId}, numaCountPerNode: ${numaCountPerNode}, numa id: ${v}")
            if (!this.hostToExecutors.get(host).values.toSeq.contains(v)) {
            this.hostToExecutors.get(host) += (executorId -> v)
            logWarning(s"Numa Manager added executor id: ${executorId}, numa id: ${v}")
            return Some(v)
          }
        }
        None
      } else {
        this.hostToExecutors.get(host).get(executorId)
      }
    } else {
      this.hostToExecutors.putIfAbsent(host, mutable.HashMap[String, Int](executorId -> 0))
      logWarning(s"Numa Manager added first executor id: ${executorId}, numa id: 0")
      Some(0)
    }
  }

  private def removeExecutor(executorId: String, host: String): Unit = {
    if (hostToExecutors.get(host) != null) {
      hostToExecutors.get(host).remove(executorId)
    }
    if (hostToExecutors.get(host).size == 0) {
      hostToExecutors.remove(host)
    }
  }

  private def getNodeRemovedList(host: String): Seq[String] = {
    var removedList = Seq.empty[String].toBuffer
    val optIds = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl].getExecutorsAliveOnHost(host)
    optIds match {
      case Some(ids) =>
        logWarning(s"Get available id count ${ids.size}")
        if (this.hostToExecutors.containsKey(host)) {
          val savedIdHashMap = this.hostToExecutors.get(host)
          for (elem <- savedIdHashMap) {
            if (!ids.contains(elem._1)) {
              removedList += elem._1
            }
          }
        }
        removedList
      case None => removedList
    }
  }
}

object NumaManager {
  private var nm: NumaManager = _

  def getOrCreate: NumaManager = if (nm == null) init() else nm

  def init(): NumaManager = synchronized {
    val sc = SparkContext.getActive.get
    new NumaManager(sc)
  }
}
