/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.LinkedHashMap;
import java.util.Map;

class MessageBuilder {

  static final String FIELD_ENVELOPE_DELIVERYTAG = "deliveryTag";
  static final String FIELD_ENVELOPE_ISREDELIVER = "isRedeliver";
  static final String FIELD_ENVELOPE_EXCHANGE = "exchange";
  static final String FIELD_ENVELOPE_ROUTINGKEY = "routingKey";

  static final Schema SCHEMA_ENVELOPE = SchemaBuilder.struct()
      .name("com.github.jcustenborder.kafka.connect.rabbitmq.Envelope")
      .doc("Encapsulates a group of parameters used for AMQP's Basic methods. See " +
          "[Envelope](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Envelope.html)")
      .field(FIELD_ENVELOPE_DELIVERYTAG, SchemaBuilder.int64().doc("The delivery tag included in this parameter envelope. See [Envelope.getDeliveryTag()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Envelope.html#getDeliveryTag--)").build())
      .field(FIELD_ENVELOPE_ISREDELIVER, SchemaBuilder.bool().doc("The redelivery flag included in this parameter envelope. See [Envelope.isRedeliver()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Envelope.html#isRedeliver--)").build())
      .field(FIELD_ENVELOPE_EXCHANGE, SchemaBuilder.string().optional().doc("The name of the exchange included in this parameter envelope. See [Envelope.getExchange()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Envelope.html#getExchange--)"))
      .field(FIELD_ENVELOPE_ROUTINGKEY, SchemaBuilder.string().optional().doc("The routing key included in this parameter envelope. See [Envelope.getRoutingKey()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Envelope.html#getRoutingKey--)").build())
      .build();

  Struct envelope(Envelope envelope) {
    return new Struct(SCHEMA_ENVELOPE)
        .put(FIELD_ENVELOPE_DELIVERYTAG, envelope.getDeliveryTag())
        .put(FIELD_ENVELOPE_ISREDELIVER, envelope.isRedeliver())
        .put(FIELD_ENVELOPE_EXCHANGE, envelope.getExchange())
        .put(FIELD_ENVELOPE_ROUTINGKEY, envelope.getRoutingKey());
  }

  static final Schema HEADER_VALUE;

  static {
    SchemaBuilder builder = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.rabbitmq.BasicProperties.HeaderValue")
        .doc("Used to store the value of a header value. The `type` field stores the type of the data and the corresponding " +
            "field to read the data from.")
        .field("type", SchemaBuilder.string().doc("Used to define the type for the HeaderValue. " +
            "This will define the corresponding field which will contain the value in it's original type.").build()
        );

    for (Schema.Type v : Schema.Type.values()) {
      if (Schema.Type.ARRAY == v || Schema.Type.MAP == v || Schema.Type.STRUCT == v) {
        continue;
      }
      final String doc = String.format("Storage for when the `type` field is set to `%s`. Null otherwise.", v);

      Schema fieldSchema = SchemaBuilder.type(v)
          .doc(doc)
          .optional()
          .build();
      builder.field(v.name(), fieldSchema);
    }

    HEADER_VALUE = builder.build();
  }

  private static final String FIELD_BASIC_PROPERTIES_CONTENTTYPE = "contentType";
  private static final String FIELD_BASIC_PROPERTIES_CONTENTENCODING = "contentEncoding";
  private static final String FIELD_BASIC_PROPERTIES_HEADERS = "headers";
  private static final String FIELD_BASIC_PROPERTIES_DELIVERYMODE = "deliveryMode";
  private static final String FIELD_BASIC_PROPERTIES_PRIORITY = "priority";
  private static final String FIELD_BASIC_PROPERTIES_CORRELATIONID = "correlationId";
  private static final String FIELD_BASIC_PROPERTIES_REPLYTO = "replyTo";
  private static final String FIELD_BASIC_PROPERTIES_EXPIRATION = "expiration";
  private static final String FIELD_BASIC_PROPERTIES_MESSAGEID = "messageId";
  private static final String FIELD_BASIC_PROPERTIES_TIMESTAMP = "timestamp";
  private static final String FIELD_BASIC_PROPERTIES_TYPE = "type";
  private static final String FIELD_BASIC_PROPERTIES_USERID = "userId";
  private static final String FIELD_BASIC_PROPERTIES_APPID = "appId";
  private static final String FIELD_BASIC_PROPERTIES_CLUSTERID = "clusterId";

  static final Schema SCHEMA_KEY = SchemaBuilder.struct()
      .name("com.github.jcustenborder.kafka.connect.rabbitmq.MessageKey")
      .doc("Key used for partition assignment in Kafka.")
      .field(
          FIELD_BASIC_PROPERTIES_MESSAGEID,
          SchemaBuilder.string().optional().doc("The value in the messageId field. " +
              "[BasicProperties.getMessageId()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getMessageId--)").build()
      )
      .build();

  static final Schema SCHEMA_BASIC_PROPERTIES = SchemaBuilder.struct()
      .name("com.github.jcustenborder.kafka.connect.rabbitmq.BasicProperties")
      .doc("Corresponds to the [BasicProperties](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html)")
      .field(
          FIELD_BASIC_PROPERTIES_CONTENTTYPE,
          SchemaBuilder.string().optional().doc("The value in the contentType field. " +
              "See [BasicProperties.getContentType()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getContentType--)")
              .build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_CONTENTENCODING,
          SchemaBuilder.string().optional().doc("The value in the contentEncoding field. " +
              "See [BasicProperties.getContentEncoding()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getContentEncoding--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_HEADERS,
          SchemaBuilder.map(Schema.STRING_SCHEMA, HEADER_VALUE).build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_DELIVERYMODE,
          SchemaBuilder.int32().optional().doc("The value in the deliveryMode field. " +
              "[BasicProperties.html.getDeliveryMode()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getDeliveryMode--) ").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_PRIORITY,
          SchemaBuilder.int32().optional().doc("The value in the priority field. " +
              "[BasicProperties.getPriority()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getPriority--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_CORRELATIONID,
          SchemaBuilder.string().optional().doc("The value in the correlationId field. " +
              "See [BasicProperties.getCorrelationId()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getCorrelationId--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_REPLYTO,
          SchemaBuilder.string().optional().doc("The value in the replyTo field. " +
              "[BasicProperties.getReplyTo()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getReplyTo--)")
      )
      .field(
          FIELD_BASIC_PROPERTIES_EXPIRATION,
          SchemaBuilder.string().optional().doc("The value in the expiration field. " +
              "See [BasicProperties.getExpiration()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getExpiration--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_MESSAGEID,
          SchemaBuilder.string().optional().doc("The value in the messageId field. " +
              "[BasicProperties.getMessageId()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getMessageId--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_TIMESTAMP, Timestamp.builder().optional().doc("The value in the timestamp field. " +
              "[BasicProperties.getTimestamp()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getTimestamp--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_TYPE, SchemaBuilder.string().optional().doc("The value in the type field. " +
              "[BasicProperties.getType()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getType--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_USERID,
          SchemaBuilder.string().optional().doc("The value in the userId field. " +
              "[BasicProperties.getUserId()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getUserId--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_APPID,
          SchemaBuilder.string().optional().doc("The value in the appId field. " +
              "[BasicProperties.getAppId()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/BasicProperties.html#getAppId--)").build()
      )
      .field(
          FIELD_BASIC_PROPERTIES_CLUSTERID,
          SchemaBuilder.string().optional().doc(
              "[AMQP.BasicProperties.getClusterId()](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/AMQP.BasicProperties.html#getClusterId--)").build()
      )
      .build();

  Map<String, Struct> headers(AMQP.BasicProperties basicProperties) {
    Map<String, Struct> headers = new LinkedHashMap<>(basicProperties.getHeaders().size());

//    for (Map.Entry<String, Object> kvp : basicProperties.getHeaders().entrySet()) {
//      Struct struct = new Struct(HEADER_VALUE);
//      String storageField;
//      
//      if (kvp.getValue() instanceof String) {
//        storageField = Schema.Type.STRING.name();
//      } 
//
//
//    }

    return headers;
  }

  Struct basicProperties(AMQP.BasicProperties basicProperties) {
    Map<String, Struct> headers = headers(basicProperties);
    return new Struct(SCHEMA_BASIC_PROPERTIES)
        .put(FIELD_BASIC_PROPERTIES_CONTENTTYPE, basicProperties.getContentType())
        .put(FIELD_BASIC_PROPERTIES_CONTENTENCODING, basicProperties.getContentEncoding())
        .put(FIELD_BASIC_PROPERTIES_HEADERS, headers)
        .put(FIELD_BASIC_PROPERTIES_DELIVERYMODE, basicProperties.getDeliveryMode())
        .put(FIELD_BASIC_PROPERTIES_PRIORITY, basicProperties.getPriority())
        .put(FIELD_BASIC_PROPERTIES_CORRELATIONID, basicProperties.getCorrelationId())
        .put(FIELD_BASIC_PROPERTIES_REPLYTO, basicProperties.getReplyTo())
        .put(FIELD_BASIC_PROPERTIES_EXPIRATION, basicProperties.getExpiration())
        .put(FIELD_BASIC_PROPERTIES_MESSAGEID, basicProperties.getMessageId())
        .put(FIELD_BASIC_PROPERTIES_TIMESTAMP, basicProperties.getTimestamp())
        .put(FIELD_BASIC_PROPERTIES_TYPE, basicProperties.getType())
        .put(FIELD_BASIC_PROPERTIES_USERID, basicProperties.getUserId())
        .put(FIELD_BASIC_PROPERTIES_APPID, basicProperties.getAppId())
        .put(FIELD_BASIC_PROPERTIES_CLUSTERID, basicProperties.getClusterId());
  }

  static final String FIELD_MESSAGE_CONSUMERTAG = "consumerTag";
  static final String FIELD_MESSAGE_ENVELOPE = "envelope";
  static final String FIELD_MESSAGE_BASICPROPERTIES = "basicProperties";
  static final String FIELD_MESSAGE_BODY = "body";


  static final Schema MESSAGE_SCHEMA = SchemaBuilder.struct()
      .name("com.github.jcustenborder.kafka.connect.rabbitmq.Message")
      .doc("Message as it is delivered to the [RabbitMQ Consumer](https://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Consumer.html#handleDelivery-java.lang.String-com.rabbitmq.client.Envelope-com.rabbitmq.client.AMQP.BasicProperties-byte:A-) ")
      .field(FIELD_MESSAGE_CONSUMERTAG, SchemaBuilder.string().doc("The consumer tag associated with the consumer").build())
      .field(FIELD_MESSAGE_ENVELOPE, SCHEMA_ENVELOPE)
      .field(FIELD_MESSAGE_BASICPROPERTIES, SCHEMA_BASIC_PROPERTIES)
      .field(FIELD_MESSAGE_BODY, SchemaBuilder.bytes().doc("The message body (opaque, client-specific byte array)").build())
      .build();

  Struct message(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] body) {
    return new Struct(MESSAGE_SCHEMA)
        .put(FIELD_MESSAGE_CONSUMERTAG, consumerTag)
        .put(FIELD_MESSAGE_ENVELOPE, envelope(envelope))
        .put(FIELD_MESSAGE_BASICPROPERTIES, basicProperties(basicProperties))
        .put(FIELD_MESSAGE_BODY, body);
  }
}
