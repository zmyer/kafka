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

import kafka.utils.TestUtils
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata
import org.junit.Test

import scala.collection.JavaConverters._

class CreateTopicsRequestWithPolicyTest extends AbstractCreateTopicsRequestTest {
  import CreateTopicsRequestWithPolicyTest._

  override def propertyOverrides(properties: Properties): Unit = {
    super.propertyOverrides(properties)
    properties.put(KafkaConfig.CreateTopicsPolicyClassNameProp, classOf[Policy].getName)
  }

  @Test
  def testValidCreateTopicsRequests() {
    val timeout = 10000
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(
      Map("topic1" -> new CreateTopicsRequest.TopicDetails(5, 1.toShort)).asJava, timeout).build())
    validateValidCreateTopicsRequests(new CreateTopicsRequest.Builder(
      Map("topic2" -> new CreateTopicsRequest.TopicDetails(5, 3.toShort)).asJava, timeout, true).build())
  }

  @Test
  def testErrorCreateTopicsRequests() {
    val timeout = 10000
    val existingTopic = "existing-topic"
    TestUtils.createTopic(zkUtils, existingTopic, 1, 1, servers)

    // Policy violations
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(
      Map("topic3" -> new CreateTopicsRequest.TopicDetails(4, 1.toShort)).asJava, timeout).build(),
      Map("topic3" -> error(Errors.POLICY_VIOLATION, Some("Topics should have at least 5 partitions, received 4"))))

    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(
      Map("topic4" -> new CreateTopicsRequest.TopicDetails(4, 1.toShort)).asJava, timeout, true).build(),
      Map("topic4" -> error(Errors.POLICY_VIOLATION, Some("Topics should have at least 5 partitions, received 4"))))

    // Check that basic errors still work
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(
      Map(existingTopic -> new CreateTopicsRequest.TopicDetails(5, 1.toShort)).asJava, timeout).build(),
      Map(existingTopic -> error(Errors.TOPIC_ALREADY_EXISTS, Some("""Topic "existing-topic" already exists."""))))
    validateErrorCreateTopicsRequests(new CreateTopicsRequest.Builder(
      Map("error-replication" -> new CreateTopicsRequest.TopicDetails(10, (numBrokers + 1).toShort)).asJava, timeout, true).build(),
      Map("error-replication" -> error(Errors.INVALID_REPLICATION_FACTOR, Some("replication factor: 4 larger than available brokers: 3"))))
  }

}

object CreateTopicsRequestWithPolicyTest {
  class Policy extends CreateTopicPolicy {
    def validate(requestMetadata: RequestMetadata): Unit =
      if (requestMetadata.numPartitions < 5)
        throw new PolicyViolationException(s"Topics should have at least 5 partitions, received ${requestMetadata.numPartitions}")
  }
}
