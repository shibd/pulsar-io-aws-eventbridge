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
package org.apache.pulsar.io.eventbridge.sink.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * Meta data utils.
 */
@Slf4j
public abstract class MetaDataUtils {

    private static final Map<String, Function<Record<GenericObject>, Optional<Object>>> metaDataConvert =
            new HashMap<>();

    static {
        metaDataConvert.put("schema_version", (record, ctx) -> record.getMessage().map(Message::getSchemaVersion));
        metaDataConvert.put("partition", (record, ctx) -> record.getPartitionIndex().map(idx -> idx));
        metaDataConvert.put("event_time", (record, ctx) -> record.getEventTime().map(time -> time));
        metaDataConvert.put("publish_time", (record, ctx) -> record.getMessage().map(Message::getPublishTime));
        metaDataConvert.put("message_id",
                (record, ctx) -> record.getMessage().map(msg -> msg.getMessageId().toString()));
        metaDataConvert.put("sequence_id", (record, ctx) -> record.getMessage().map(Message::getSequenceId));
        metaDataConvert.put("producer_name", (record, ctx) -> record.getMessage().map(Message::getProducerName));
        metaDataConvert.put("properties", (record, ctx) -> Optional.of(record.getProperties()));
        metaDataConvert.put("key", (record, ctx) -> record.getKey().map(String::toString));
    }

    public static boolean isSupportMetaData(String fieldName) {
        return metaDataConvert.containsKey(fieldName);
    }

    /**
     * Convert meta data field.
     *
     * @return If convert failed, will be return optional.empty().
     */
    public static Optional<Object> convert(String fieldName, Record<GenericObject> record) {
        try {
            return metaDataConvert.get(fieldName).process(record, null);
        } catch (Exception e) {
            log.warn("Failed to convert meta data field: {}, this field is ignored", fieldName, e);
            return Optional.empty();
        }
    }

}
