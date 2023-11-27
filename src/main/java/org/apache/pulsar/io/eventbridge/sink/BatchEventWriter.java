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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.eventbridge.sink.exception.EBConnectorDirectFailException;
import org.apache.pulsar.io.eventbridge.sink.utils.MetricContent;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

/**
 * Batch put event and writer.
 */
@Slf4j
public class BatchEventWriter implements Closeable {

    private final EventBridgeConfig eventBridgeConfig;
    private final EventBridgeClient eventBridgeClient;
    private final ScheduledExecutorService flushExecutor;
    private final ArrayBlockingQueue<PendingFlushRequest> pendingFlushEntryQueue;

    private final AtomicLong currentBatchSize = new AtomicLong(0);
    private final AtomicLong currentBatchByteSize = new AtomicLong(0);
    private final AtomicBoolean isFlushRunning = new AtomicBoolean();
    private final String sinkName;
    private final SinkContext sinkContext;
    private volatile long lastFlushTime;

    @Getter
    @Builder
    static class PendingFlushRequest {
        private PutEventsRequestEntry putEventsResultEntry;
        private Record<GenericObject> record;
        private long entrySize;
    }

    public BatchEventWriter(String sinkName, EventBridgeConfig eventBridgeConfig, EventBridgeClient eventBridgeClient,
                            SinkContext sinkContext) {
        this.sinkName = sinkName;
        this.eventBridgeConfig = eventBridgeConfig;
        this.eventBridgeClient = eventBridgeClient;
        this.pendingFlushEntryQueue = new ArrayBlockingQueue<>(eventBridgeConfig.getBatchPendingQueueSize());
        this.sinkContext = sinkContext;
        this.flushExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("pulsar-io-aws-event-bridge-flush-%d")
                .build());
        if (eventBridgeConfig.getBatchMaxTimeMs() > 0) {
            flushExecutor.scheduleAtFixedRate(this::tryFlush, this.eventBridgeConfig.getBatchMaxTimeMs(),
                    this.eventBridgeConfig.getBatchMaxTimeMs(), TimeUnit.MILLISECONDS);
        }
        this.lastFlushTime = System.currentTimeMillis();
    }

    public void append(String jsonString, Record<GenericObject> record) throws InterruptedException {
        PutEventsRequestEntry entry = PutEventsRequestEntry.builder()
                .eventBusName(eventBridgeConfig.getEventBusName())
                .source(sinkName)
                .detail(jsonString)
                .detailType(record.getTopicName().get())
                .time(Instant.now())
                .build();

        long entrySize = calcSize(entry);
        if (entrySize > EventBridgeConfig.DEFAULT_MAX_BATCH_BYTES_SIZE) {
            throw new EBConnectorDirectFailException(
                    "A single message cannot be larger than 256KB, the message size: " + entrySize
                            + ". More details refer: https://docs.aws.amazon"
                            + ".com/eventbridge/latest/userguide/eb-putevent-size.html");
        }
        pendingFlushEntryQueue.put(
                PendingFlushRequest.builder().putEventsResultEntry(entry).record(record).entrySize(entrySize).build());
        currentBatchSize.incrementAndGet();
        currentBatchByteSize.addAndGet(entrySize);
        tryFlush();
    }

    private void tryFlush() {
        boolean isFlushByBatchMaxSize = eventBridgeConfig.getBatchMaxSize() > 0
                && currentBatchSize.get() >= eventBridgeConfig.getBatchMaxSize();
        boolean isFlushByBatchMaxBytesSize = currentBatchByteSize.get() >= eventBridgeConfig.getBatchMaxBytesSize();
        boolean isFlushByBatchMaxTimeMs = (eventBridgeConfig.getBatchMaxTimeMs() > 0
                && System.currentTimeMillis() - lastFlushTime > eventBridgeConfig.getBatchMaxTimeMs());

        if (isFlushByBatchMaxSize || isFlushByBatchMaxBytesSize || isFlushByBatchMaxTimeMs) {
            if (isFlushRunning.compareAndSet(false, true)) {
                flushExecutor.submit(() -> {
                    try {
                        flush();
                    } catch (Exception e) {
                        sinkContext.recordMetric(MetricContent.FAIL_FLUSH_COUNT, 1);
                        log.error("Caught unexpected exception: {}", e.getMessage(), e);
                    } finally {
                        lastFlushTime = System.currentTimeMillis();
                        isFlushRunning.compareAndSet(true, false);
                    }
                    tryFlush();
                });
            }
        }
    }

    private void flush() {
        if (pendingFlushEntryQueue.isEmpty()) {
            log.info(
                    "Skip flush events, because pending flush queue is empty. currentBatchSize: {},  "
                            + "currentBatchByteSize: {}",
                    currentBatchSize.get(), currentBatchByteSize.get());
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Start flush events, currentBatchSize: {}, currentBatchByteSize: {} ", currentBatchSize.get(),
                    currentBatchByteSize.get());
        }

        // The pop messages cannot be larger than batchBytes and batchSize.
        // In any case, as long as there is a message in the queue, we will first add one to it.
        long popEventSize = 0;
        long popEventBytesSize = 0;
        List<PendingFlushRequest> pendingFlushRequestList = new ArrayList<>();
        PendingFlushRequest peekFlushRequest;
        do {
            PendingFlushRequest poolFlushRequest = pendingFlushEntryQueue.poll();
            pendingFlushRequestList.add(poolFlushRequest);
            popEventSize++;
            popEventBytesSize += poolFlushRequest.entrySize;
            peekFlushRequest = pendingFlushEntryQueue.peek();
        } while (peekFlushRequest != null && (eventBridgeConfig.getBatchMaxSize() < 0
                || pendingFlushRequestList.size() < eventBridgeConfig.getBatchMaxSize())
                && popEventBytesSize + peekFlushRequest.entrySize < eventBridgeConfig.getBatchMaxBytesSize());
        if (log.isDebugEnabled()) {
            log.debug("Actual flush events, size: {}, bytes size: {}", popEventSize, popEventBytesSize);
        }
        try {
            retryWriteEvents(pendingFlushRequestList);
        } catch (AwsServiceException | SdkClientException e) {
            // For aws exception, not to retry and not ack. just wait next receive message after retry.
            // Such abnormalities are generally irreversible and require manual intervention.
            sinkContext.recordMetric(MetricContent.FAIL_FLUSH_COUNT, 1);
            log.error(e.getMessage(), e);
        } finally {
            // Rest status. Whether the refresh was successful or not,
            // we need to subtract the size value because we have already taken it out of the queue.
            currentBatchSize.addAndGet(-1 * popEventSize);
            currentBatchByteSize.addAndGet(-1 * popEventBytesSize);
            if (log.isDebugEnabled()) {
                log.debug("End flush events, currentBatchSize: {}, currentBatchByteSize: {}", currentBatchSize.get(),
                        currentBatchByteSize.get());
            }
        }
    }

    private void retryWriteEvents(List<PendingFlushRequest> pendingFlushRequestList) {
        long retryNum = 0;
        do {
            PutEventsRequest putEventsRequest = PutEventsRequest.builder().entries(
                            pendingFlushRequestList.stream().map(PendingFlushRequest::getPutEventsResultEntry).toList())
                    .build();
            PutEventsResponse putEventsResponse = eventBridgeClient.putEvents(putEventsRequest);
            final List<PendingFlushRequest> failedFlushRequestList = new ArrayList<>();
            if (putEventsResponse.failedEntryCount() > 0) {
                final List<PutEventsResultEntry> putEventsResultEntryList = putEventsResponse.entries();
                for (int i = 0; i < putEventsResultEntryList.size(); i++) {
                    final PendingFlushRequest pendingFlushRequest = pendingFlushRequestList.get(i);
                    final PutEventsResultEntry putEventsResultEntry = putEventsResultEntryList.get(i);
                    if (putEventsResultEntry.errorCode() != null) {
                        log.error("Failed to send event to AWS EventBridge, errorCode: {}, errorMessage: {}",
                                putEventsResultEntry.errorCode(), putEventsResultEntry.errorMessage());
                        failedFlushRequestList.add(pendingFlushRequest);
                    } else {
                        // ack success msg.
                        pendingFlushRequest.record.ack();
                        sinkContext.recordMetric(MetricContent.ACK_COUNT, 1);
                    }
                }
                log.warn(
                        "Failed to send {} events to AWS EventBridge, wait for {} ms retry. currentRetryNum:{}, "
                                + "maxRetryCount:{}",
                        failedFlushRequestList.size(), eventBridgeConfig.getIntervalRetryTimeMs(), retryNum,
                        eventBridgeConfig.getMaxRetryCount());
                if (eventBridgeConfig.getIntervalRetryTimeMs() > 0) {
                    try {
                        Thread.sleep(eventBridgeConfig.getIntervalRetryTimeMs());
                    } catch (InterruptedException e) {
                        log.error("Failed to sleep for retry", e);
                        break;
                    }
                }
            } else {
                // not failed entry, ack all msg.
                pendingFlushRequestList.forEach(pendingFlushRequest -> pendingFlushRequest.record.ack());
                sinkContext.recordMetric(MetricContent.ACK_COUNT, pendingFlushRequestList.size());
            }
            pendingFlushRequestList = failedFlushRequestList;
            if (!pendingFlushRequestList.isEmpty()) {
                sinkContext.recordMetric(MetricContent.RETRY_FLUSH_COUNT, 1);
            }
        } while (!pendingFlushRequestList.isEmpty() && retryNum++ < eventBridgeConfig.getMaxRetryCount());

        for (PendingFlushRequest pendingFlushRequest : pendingFlushRequestList) {
            log.error("The event send failed after retry {}", pendingFlushRequest.putEventsResultEntry);
        }
    }

    private long calcSize(PutEventsRequestEntry entry) {
        int size = 0;
        if (entry.time() != null) {
            size += 14;
        }
        size += entry.source().getBytes(StandardCharsets.UTF_8).length;
        size += entry.detailType().getBytes(StandardCharsets.UTF_8).length;
        if (entry.detail() != null) {
            size += entry.detail().getBytes(StandardCharsets.UTF_8).length;
        }
        if (entry.resources() != null) {
            for (String resource : entry.resources()) {
                if (resource != null) {
                    size += resource.getBytes(StandardCharsets.UTF_8).length;
                }
            }
        }
        return size;
    }

    @Override
    public void close() throws IOException {
        try {
            flushExecutor.submit(this::flush).get();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
        eventBridgeClient.close();
    }
}