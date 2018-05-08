/**
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

package kafka.server

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{Exit, Logging, VerifiableProperties}

// TODO: by zmyer
object KafkaServerStartable {
  def fromProps(serverProps: Properties) = {
    //日志采集器
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    //创建kafka服务器启动对象
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps, false), reporters)
  }
}

// TODO: by zmyer
class KafkaServerStartable(val staticServerConfig: KafkaConfig, reporters: Seq[KafkaMetricsReporter]) extends Logging {
  //创建kafka服务器对象
  private val server = new KafkaServer(staticServerConfig, kafkaMetricsReporters = reporters)

  def this(serverConfig: KafkaConfig) = this(serverConfig, Seq.empty)

  // TODO: by zmyer
  def startup() {
    //启动kafka服务器
    try server.startup()
    catch {
      case _: Throwable =>
        // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
        fatal("Exiting Kafka.")
        Exit.exit(1)
    }
  }

  // TODO: by zmyer
  def shutdown() {
    //关闭服务器
    try server.shutdown()
    catch {
      case _: Throwable =>
        fatal("Halting Kafka.")
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Exit.halt(1)
    }
  }

  /**
   * Allow setting broker state from the startable.
   * This is needed when a custom kafka server startable want to emit new states that it introduces.
   */
  // TODO: by zmyer
  def setServerState(newState: Byte) {
    //设置broker状态信息
    server.brokerState.newState(newState)
  }

  // TODO: by zmyer
  def awaitShutdown(): Unit = server.awaitShutdown()

}


