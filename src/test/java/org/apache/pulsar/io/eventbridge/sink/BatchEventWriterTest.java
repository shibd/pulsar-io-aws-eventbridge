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
package org.apache.pulsar.io.eventbridge.sink;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.eventbridge.sink.exception.EBConnectorDirectFailException;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

/**
 * Batch event writer test.
 */
public class BatchEventWriterTest {

    @NotNull
    private static Record<GenericObject> getGenericObjectRecord(String topicName, CountDownLatch countDownLatch) {
        return new Record<>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of(topicName);
            }

            @Override
            public GenericObject getValue() {
                return null;
            }

            @Override
            public void ack() {
                countDownLatch.countDown();
            }
        };
    }

    @NotNull
    private static EventBridgeConfig getEventBridgeConfig(long batchMaxSize, long batchMaxBytesSize,
                                                          long batchMaxTimeMs) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        configMap.put("batchMaxSize", batchMaxSize);
        configMap.put("batchMaxBytesSize", batchMaxBytesSize);
        configMap.put("batchMaxTimeMs", batchMaxTimeMs);
        return EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }

    @SneakyThrows
    @Test(dataProvider = "batchFlush")
    public void testAppendBase(long batchMaxSize, long batchMaxByteSize, long batchMaxTimeMs, int expectedFlushCount) {
        // clear mock cache.
        Mockito.clearAllCaches();

        EventBridgeConfig
                eventBridgeConfig = getEventBridgeConfig(batchMaxSize, batchMaxByteSize, batchMaxTimeMs);
        String sinkName = "test-sink";
        String data = "{\"test-json\": \"test-value\"}";
        String topicName = "test-topic";

        EventBridgeClient eventBridgeClient = mock(EventBridgeClient.class);
        when(eventBridgeClient.putEvents((PutEventsRequest) Mockito.any())).then(putEventRequest -> {
            PutEventsRequest putEventsRequest = putEventRequest.getArgument(0);
            List<PutEventsResultEntry> resultEntryList = new ArrayList<>();
            for (PutEventsRequestEntry entry : putEventsRequest.entries()) {
                Assert.assertEquals(eventBridgeConfig.getEventBusName(), entry.eventBusName());
                Assert.assertEquals(eventBridgeConfig.getEventBusResourceName(), entry.resources().get(0));
                Assert.assertEquals(data, entry.detail());
                Assert.assertEquals(sinkName, entry.source());
                Assert.assertEquals(topicName, entry.detailType());
                resultEntryList.add(PutEventsResultEntry.builder().build());
            }
            return PutEventsResponse.builder().failedEntryCount(0).entries(resultEntryList).build();
        });

        CountDownLatch countDownLatch = new CountDownLatch(10);
        Record<GenericObject> record = getGenericObjectRecord(topicName, countDownLatch);

        BatchEventWriter batchEventWriter = new BatchEventWriter(sinkName, eventBridgeConfig, eventBridgeClient,
                mock(SinkContext.class));
        for (int i = 0; i < 10; i++) {
            batchEventWriter.append(data, record);
        }
        countDownLatch.await();
        Assert.assertEquals(0, countDownLatch.getCount());
        verify(eventBridgeClient, Mockito.times(expectedFlushCount)).putEvents((PutEventsRequest) Mockito.any());
    }

    @DataProvider(name = "batchFlush")
    public Object[][] batchFlushProvider() {
        return new Object[][]{
                // Testing when a single message is greater than batchMaxByteSize also triggers a refresh
                {-1, 50, -1, 10},
                {10, EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE, -1, 1},
                {-1, 650, 500, 2},
                {-1, EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE, 500, 1},
        };
    }

    @SneakyThrows
    @Test
    public void testAppendFailedAndRetrySuccess() {
        EventBridgeConfig
                eventBridgeConfig = getEventBridgeConfig(10, EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE, -1);
        eventBridgeConfig.setMaxRetryCount(10);
        eventBridgeConfig.setIntervalRetryTimeMs(0);

        String sinkName = "test-sink";
        String data = "{\"test-json\": \"test-value\"}";
        String topicName = "test-topic";

        EventBridgeClient eventBridgeClient = mock(EventBridgeClient.class);
        int failedNum = 10;
        AtomicInteger mockFailedNum = new AtomicInteger(failedNum);
        when(eventBridgeClient.putEvents((PutEventsRequest) Mockito.any())).then(putEventRequest -> {
            PutEventsRequest putEventsRequest = putEventRequest.getArgument(0);

            List<PutEventsRequestEntry> requestEntries = putEventsRequest.entries();
            List<PutEventsResultEntry> resultEntries = new ArrayList<>();
            // mock failed entries
            for (int i = 0; i < mockFailedNum.get(); i++) {
                resultEntries.add(PutEventsResultEntry.builder().errorCode("mock-failed").build());
            }
            // mock success entries
            for (int i = mockFailedNum.get(); i < requestEntries.size(); i++) {
                resultEntries.add(PutEventsResultEntry.builder().build());
            }
            return PutEventsResponse.builder()
                    .failedEntryCount(mockFailedNum.getAndDecrement())
                    .entries(resultEntries)
                    .build();
        });

        CountDownLatch countDownLatch = new CountDownLatch(10);
        Record<GenericObject> record = getGenericObjectRecord(topicName, countDownLatch);

        BatchEventWriter batchEventWriter = new BatchEventWriter(sinkName, eventBridgeConfig, eventBridgeClient,
                mock(SinkContext.class));
        for (int i = 0; i < 10; i++) {
            batchEventWriter.append(data + i, record);
        }
        countDownLatch.await();
        Assert.assertEquals(0, countDownLatch.getCount());
        verify(eventBridgeClient, Mockito.times(failedNum + 1)).putEvents((PutEventsRequest) Mockito.any());
    }

    @SneakyThrows
    @Test
    public void testAppendFailedAndRetryFiled() {
        EventBridgeConfig
                eventBridgeConfig = getEventBridgeConfig(10, EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE, -1);
        eventBridgeConfig.setMaxRetryCount(10);
        eventBridgeConfig.setIntervalRetryTimeMs(0);

        String sinkName = "test-sink";
        String data = "{\"test-json\": \"test-value\"}";
        String topicName = "test-topic";

        EventBridgeClient eventBridgeClient = mock(EventBridgeClient.class);
        int failedNum = 5;
        AtomicInteger mockFailedNum = new AtomicInteger(failedNum);
        when(eventBridgeClient.putEvents((PutEventsRequest) Mockito.any())).then(putEventRequest -> {
            PutEventsRequest putEventsRequest = putEventRequest.getArgument(0);

            List<PutEventsRequestEntry> requestEntries = putEventsRequest.entries();
            List<PutEventsResultEntry> resultEntries = new ArrayList<>();
            // mock failed entries
            for (int i = 0; i < mockFailedNum.get(); i++) {
                resultEntries.add(PutEventsResultEntry.builder().errorCode("mock-failed").build());
            }
            // mock success entries
            for (int i = mockFailedNum.get(); i < requestEntries.size(); i++) {
                resultEntries.add(PutEventsResultEntry.builder().build());
            }
            return PutEventsResponse.builder()
                    .failedEntryCount(mockFailedNum.get())
                    .entries(resultEntries)
                    .build();
        });

        CountDownLatch countDownLatch = new CountDownLatch(10);
        Record<GenericObject> record = getGenericObjectRecord(topicName, countDownLatch);

        BatchEventWriter batchEventWriter = new BatchEventWriter(sinkName, eventBridgeConfig, eventBridgeClient,
                mock(SinkContext.class));
        for (int i = 0; i < 10; i++) {
            batchEventWriter.append(data + i, record);
        }
        countDownLatch.await(500, TimeUnit.MILLISECONDS);
        // success 5 entries, failed 5 entries, to assert will partially ack record.
        Assert.assertEquals(5, countDownLatch.getCount());
        verify(eventBridgeClient, Mockito.times((int) eventBridgeConfig.getMaxRetryCount() + 1)).putEvents(
                (PutEventsRequest) Mockito.any());
    }

    @SneakyThrows
    @Test(expectedExceptions = EBConnectorDirectFailException.class)
    public void testSingleMessageLargeAWSLimit() {
        EventBridgeConfig
                eventBridgeConfig = getEventBridgeConfig(10, EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE, -1);
        EventBridgeClient eventBridgeClient = mock(EventBridgeClient.class);
        BatchEventWriter batchEventWriter = new BatchEventWriter("test-sink", eventBridgeConfig, eventBridgeClient,
                mock(SinkContext.class));
        byte[] bytes = new byte[Math.toIntExact(EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE)];
        Record<GenericObject> record = getGenericObjectRecord("test-topic", null);
        batchEventWriter.append(new String(bytes), record);
    }

}
