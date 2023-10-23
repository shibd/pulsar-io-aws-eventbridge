/**
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
package org.apache.pulsar.io.eventbridge.sink.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Record convert test.
 */
public abstract class RecordConvertTest {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected abstract Pair<Schema<GenericRecord>, GenericRecord> getRecordAndSchema();

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertToJson() {
        MessageId mockMessageId = Mockito.mock(MessageId.class);
        Mockito.when(mockMessageId.toString()).thenReturn("1:1:123");
        Message mockMessage = Mockito.mock(Message.class);
        Mockito.when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        String producerName = "my-producer";
        long sequenceId = 1L;
        Mockito.when(mockMessage.getProducerName()).thenReturn(producerName);
        Mockito.when(mockMessage.getSequenceId()).thenReturn(sequenceId);
        long mockEventTime = System.currentTimeMillis();
        Pair<Schema<GenericRecord>, GenericRecord> recordAndSchema = getRecordAndSchema();
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public Schema<GenericRecord> getSchema() {
                return recordAndSchema.getLeft();
            }

            @Override
            public GenericRecord getValue() {
                return recordAndSchema.getRight();
            }

            @Override
            public Optional<Message<GenericRecord>> getMessage() {
                return Optional.of(mockMessage);
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(mockEventTime);
            }
        };

        HashSet<String> metaDataFields = new HashSet<>();
        metaDataFields.add("event_time");
        metaDataFields.add("message_id");
        metaDataFields.add("sequence_id");
        metaDataFields.add("producer_name");
        DefaultRecordConvert recordConvert = new DefaultRecordConvert(metaDataFields);
        try {
            String jsonString = recordConvert.convertToJson((Record<GenericObject>) record);
            System.out.println(jsonString);
            // assert user data
            JsonNode jsonNode = MAPPER.readTree(jsonString);
            JsonNode data = jsonNode.get("data");
            Assert.assertNotNull(data);
            // assert meta data
            Assert.assertEquals(mockEventTime, jsonNode.get("event_time").asLong());
            Assert.assertEquals(mockMessageId.toString(), jsonNode.get("message_id").asText());
            Assert.assertNull(jsonNode.get("partition"));
            Assert.assertEquals(sequenceId, jsonNode.get("sequence_id").asLong());
            Assert.assertEquals(producerName, jsonNode.get("producer_name").asText());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
