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
package org.apache.spark.sql.oap.rpc

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.execution.datasources.oap.filecache.NumaManager
import org.apache.spark.sql.oap.rpc.OapMessages.{AskExecutorIdNumPerHost, ReplyExecutorIdNumPerHost}

private[spark] class OapPmRpcDriverEndpoint(
  override val rpcEnv: RpcEnv,
  numaManager: NumaManager)
  extends ThreadSafeRpcEndpoint with Logging {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case AskExecutorIdNumPerHost(executorId) =>
      context.reply(handleAskExecutorIdNum(executorId))
    case _ =>
  }

  private def handleAskExecutorIdNum(
    executorId: String): ReplyExecutorIdNumPerHost = {
    ReplyExecutorIdNumPerHost(executorId, numaManager.getNumaId(executorId))
  }
}

private[spark] object OapPmRpcDriverEndpoint {
  val DRIVER_PM_ENDPOINT_NAME = "OapRpcPmDriverEndpoint"
}
