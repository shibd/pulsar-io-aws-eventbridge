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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.io.core.SinkContext;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * EventBridge config test.
 */
public class EventBridgeConfigTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadRequiredNotSet() {
        // Test not set required config, will exception.
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("batchMaxSize", 100);
        EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }

    @Test
    public void testLoadSuccess() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("batchMaxSize", 10);
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        EventBridgeConfig eventBridgeConfig = EventBridgeConfig.load(configMap, mock(SinkContext.class));
        Assert.assertEquals("testEventBusName", eventBridgeConfig.getEventBusName());
        Assert.assertEquals(10, eventBridgeConfig.getBatchMaxSize());
        Assert.assertEquals("test-region", eventBridgeConfig.getRegion());
        // assert set default value.
        Assert.assertEquals(1000, eventBridgeConfig.getBatchPendingQueueSize());
        Assert.assertEquals(640, eventBridgeConfig.getBatchMaxBytesSize());
        Assert.assertEquals(5000, eventBridgeConfig.getBatchMaxTimeMs());
        Assert.assertEquals(100, eventBridgeConfig.getMaxRetryCount());
        Assert.assertEquals(1000, eventBridgeConfig.getIntervalRetryTimeMs());
        Set<String> metaDataField = eventBridgeConfig.getMetaDataFields();
        Assert.assertTrue(metaDataField.contains("message_id"));
        Assert.assertTrue(metaDataField.contains("event_time"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadVerifyFailedBatchMaxSizeGreaterPendingQueueSize() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        configMap.put("batchMaxSize", 10);
        configMap.put("batchPendingQueueSize", 5);
        EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadVerifyFailedBatchMaxSizeInvalid() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        configMap.put("batchMaxSize", 100);
        EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadVerifyFailedBatchMaxBytesSizeInvalid() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        configMap.put("batchMaxBytesSize", 0);
        EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadVerifyFailedRetryCountInvalid() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        configMap.put("retryCount", -1);
        EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLoadVerifyFailedIntervalRetryTimeMsInvalid() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("accessKeyId", "test_access_key_id");
        configMap.put("secretAccessKey", "test_secret_access_key");
        configMap.put("eventBusName", "testEventBusName");
        configMap.put("region", "test-region");
        configMap.put("eventBusResourceName", "test-arn");
        configMap.put("intervalRetryTimeMs", -1);
        EventBridgeConfig.load(configMap, mock(SinkContext.class));
    }
}
