/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.rabbitmq;

import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.AMQConnection;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MessageBuilderTest {
  MessageBuilder messageBuilder;

  @BeforeEach
  public void before() {
    this.messageBuilder = new MessageBuilder();
  }

  void assertField(final Object expected, final Struct struct, final String fieldName) {
    assertEquals(expected, struct.get(fieldName), fieldName + " does not match.");
  }

  @Test
  public void envelope() {
    final Envelope input = new Envelope(
        13246312L,
        true,
        "exchange",
        "routingKey"
    );

    final Struct actual = this.messageBuilder.envelope(input);
    assertNotNull(actual, "actual should not be null.");
    assertField(input.getDeliveryTag(), actual, MessageBuilder.FIELD_ENVELOPE_DELIVERYTAG);
    assertField(input.getExchange(), actual, MessageBuilder.FIELD_ENVELOPE_EXCHANGE);
    assertField(input.getRoutingKey(), actual, MessageBuilder.FIELD_ENVELOPE_ROUTINGKEY);
    assertField(input.isRedeliver(), actual, MessageBuilder.FIELD_ENVELOPE_ISREDELIVER);

    Map<String, Object> _clientProperties = AMQConnection.defaultClientProperties();

  }



}
