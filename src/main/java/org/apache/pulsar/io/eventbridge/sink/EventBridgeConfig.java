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

import java.io.Serial;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.pulsar.io.eventbridge.sink.utils.MetaDataUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * The EventBridge sink Config.
 */
@Data
@Slf4j
public class EventBridgeConfig implements Serializable {

    protected static final long DEFAULT_MAX_BATCH_BYTES_SIZE = 256000;
    @Serial
    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "The EventBridge access key ID: "
                    + "https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html")
    private String accessKeyId;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "The EventBridge secret access key: "
                    + "https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html")
    private String secretAccessKey;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The region where AWS EventBridge service is located. All AWS region: "
                    + "https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/regions/Region.html")
    private String region;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The EventBus name.")
    private String eventBusName;

    // STS auth config.
    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The aws sts roleArn. Example: arn:aws:iam::598203581484:role/test-role")
    private String role;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The aws sts role session name. Name it yourself.")
    private String roleSessionName;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The AWS STS endpoint. By default, the default STS endpoint: https://sts.amazonaws.com is "
                    + "used. See: https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html for more details.")
    private String stsEndpoint;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The aws sts region, defaults to the 'region' config or env region.")
    private String stsRegion;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The EventBus Aws resource name(ARN). "
                    + "Example: arn:aws:events:ap-northeast-1:598263551484:event-bus/my_eventbus ")
    private String eventBusResourceName;

    @FieldDoc(required = false,
            defaultValue = "event_time,message_id",
            help = "The metadata field will add to the event. separate multiple fields with commas."
                    + "optional: schema_version, partition, event_time, publish_time"
                    + "message_id, sequence_id, producer_name, key, properties")
    private String metaDataField;

    @FieldDoc(required = false,
            defaultValue = "1000",
            help = "Pending Queue size, This value must greater than batchMaxSize.")
    private int batchPendingQueueSize;

    @FieldDoc(required = false,
            defaultValue = "10",
            help = "MMaximum number of batch messages. The number must be less than or equal to 10 (AWS Required).")
    private int batchMaxSize;

    @FieldDoc(required = false,
            defaultValue = "640",
            help = "Maximum number of batch bytes payload size. This value cannot be greater than 512KB.")
    private long batchMaxBytesSize;

    @FieldDoc(required = false,
            defaultValue = "5000",
            help = "Batch max wait time: milliseconds.")
    private long batchMaxTimeMs;

    @FieldDoc(required = false,
            defaultValue = "100",
            help = "Maximum retry send event count, when the event put failed.")
    private long maxRetryCount;

    @FieldDoc(required = false,
            defaultValue = "1000",
            help = "The interval time(milliseconds) for each retry, when the event put failed.")
    private long intervalRetryTimeMs;

    public static EventBridgeConfig load(Map<String, Object> map, SinkContext sinkContext) {
        EventBridgeConfig
                eventBridgeConfig = IOConfigUtils.loadWithSecrets(map, EventBridgeConfig.class, sinkContext);
        eventBridgeConfig.verifyConfig();
        return eventBridgeConfig;
    }

    public void verifyConfig() {
        if (batchMaxSize > 10) {
            throw new IllegalArgumentException("batchMaxSize must less than or equal to 10(AWS Required)");
        }
        if (batchMaxBytesSize <= 0 || batchMaxBytesSize > DEFAULT_MAX_BATCH_BYTES_SIZE) {
            throw new IllegalArgumentException(
                    "batchMaxBytesSize must greater 0 and less than or equal to 256KB(AWS Required),"
                            + " Refer: https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html");
        }
        if (batchPendingQueueSize <= batchMaxSize) {
            throw new IllegalArgumentException("batchPendingQueueSize must be greater than batchMaxSize");
        }
        if (maxRetryCount < 0) {
            throw new IllegalArgumentException("maxRetryCount must be greater than or equal 0");
        }
        if (intervalRetryTimeMs < 0) {
            throw new IllegalArgumentException("intervalRetryTimeMs must be greater than or equal to 0");
        }
    }

    /**
     * Split meta data field.
     */
    public Set<String> getMetaDataFields() {
        return Optional.ofNullable(metaDataField)
                .map(__ -> Arrays.stream(metaDataField.split(","))
                        .map(field -> {
                            String trimField = field.trim();
                            if (trimField.contains(" ")) {
                                throw new IllegalArgumentException(
                                        "There cannot be spaces in the field: " + metaDataField);
                            }
                            if (!MetaDataUtils.isSupportMetaData(trimField)) {
                                throw new IllegalArgumentException(
                                        "The field: " + trimField + " is not supported");
                            }
                            return trimField;
                        }).collect(Collectors.toSet())
                ).orElse(new HashSet<>());
    }

    public AwsCredentialsProvider getAwsCredentialsProvider() {
        AwsCredentialsProvider chain = DefaultCredentialsProvider.builder().build();
        if (StringUtils.isNotEmpty(this.getAccessKeyId())
                && StringUtils.isNotEmpty(this.getSecretAccessKey())) {
            chain = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(this.getAccessKeyId(), this.getSecretAccessKey()));
        }

        if (StringUtils.isNotEmpty(this.getRole()) && StringUtils.isNotEmpty(this.getRoleSessionName())) {
            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(this.getRole())
                    .roleSessionName(this.getRoleSessionName())
                    .build();
            software.amazon.awssdk.services.sts.StsClientBuilder stsb = StsClient.builder().credentialsProvider(chain);
            // try both regions, use the basic region first, then more specific sts region
            if (StringUtils.isNotEmpty(this.getRegion())) {
                stsb = stsb.region(Region.of(this.getRegion()));
            }
            if (StringUtils.isNotEmpty(this.getStsRegion())) {
                stsb = stsb.region(Region.of(this.getStsRegion()));
            }
            if (StringUtils.isNotEmpty(this.getStsEndpoint())) {
                stsb = stsb.endpointOverride(URI.create(this.getStsEndpoint()));
            }

            StsClient stsClient = stsb.build();

            return StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient).refreshRequest(assumeRoleRequest)
                    .asyncCredentialUpdateEnabled(true)
                    .build();
        }
        return chain;
    }
}
