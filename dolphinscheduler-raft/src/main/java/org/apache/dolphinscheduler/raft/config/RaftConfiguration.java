/*
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

package org.apache.dolphinscheduler.raft.config;

import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Slf4j
@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "raft")
public class RaftConfiguration {

    /**
     * Raft peer addresses (including current node)
     * e.g. ["master-1:9234", "master-2:9234", "master-3:9234"]
     */
    @NotEmpty(message = "Raft peers cannot be empty")
    private List<String> peers;

    /**
     * Current node address
     * Will be inferred from master config if not specified
     */
    private String nodeAddress;

    /**
     * Data directory for Raft logs, snapshots and metadata
     */
    @NotNull(message = "Data directory cannot be null")
    private String dataDir = "./data/raft";

    /**
     * Election timeout in milliseconds (optimized: 2000-5000ms)
     */
    @Min(value = 1000, message = "Election timeout must be at least 1000ms")
    private int electionTimeoutMs = 2000;

    /**
     * Heartbeat interval in milliseconds (optimized: 500-1000ms)
     */
    @Min(value = 100, message = "Heartbeat interval must be at least 100ms")
    private int heartbeatIntervalMs = 500;

    /**
     * Snapshot interval in seconds (optimized: 600-1800s)
     */
    @Min(value = 60, message = "Snapshot interval must be at least 60s")
    private int snapshotIntervalSecs = 600;

    /**
     * Number of log entries per batch during replication
     */
    @Min(value = 1, message = "Max logs batch size must be at least 1")
    private int maxLogsBatchSize = 1024;

    /**
     * Enable metrics collection
     */
    private boolean enableMetrics = true;

    /**
     * RPC request timeout in milliseconds
     */
    @Min(value = 1000, message = "RPC request timeout must be at least 1000ms")
    private int rpcRequestTimeoutMs = 5000;

    /**
     * RPC connection timeout in milliseconds
     */
    @Min(value = 1000, message = "RPC connection timeout must be at least 1000ms")
    private int rpcConnectTimeoutMs = 3000;

    /**
     * Maximum number of entries in log cache
     */
    @Min(value = 100, message = "Log cache size must be at least 100")
    private int logCacheSize = 1000;

    /**
     * Lease cleanup interval in seconds
     */
    @Min(value = 5, message = "Lease cleanup interval must be at least 5s")
    private int leaseCleanupIntervalSecs = 30;

    /**
     * Enable leader lease optimization
     */
    private boolean enableLeaderLease = true;

    /**
     * Leader lease timeout in milliseconds
     */
    @Min(value = 1000, message = "Leader lease timeout must be at least 1000ms")
    private int leaderLeaseTimeoutMs = 10000;

    /**
     * Maximum number of concurrent requests
     */
    @Min(value = 1, message = "Max concurrent requests must be at least 1")
    private int maxConcurrentRequests = 100;

    public void validate() {
        if (heartbeatIntervalMs >= electionTimeoutMs) {
            log.warn("Heartbeat interval ({}) should be less than election timeout ({})", 
                    heartbeatIntervalMs, electionTimeoutMs);
        }
        
        if (nodeAddress == null && (peers == null || peers.isEmpty())) {
            throw new IllegalArgumentException("Either nodeAddress or peers must be specified");
        }
        
        log.info("Raft configuration validated successfully: peers={}, nodeAddress={}, dataDir={}", 
                peers, nodeAddress, dataDir);
    }
}