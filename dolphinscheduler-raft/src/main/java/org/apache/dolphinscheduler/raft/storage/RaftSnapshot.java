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

package org.apache.dolphinscheduler.raft.storage;

import org.apache.dolphinscheduler.raft.core.RaftRegistryStateMachine;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized Raft snapshot implementation with JSON serialization
 */
@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RaftSnapshot {
    
    private static final String SNAPSHOT_FILE = "snapshot.json";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private Map<String, String> kvStore = new ConcurrentHashMap<>();
    private Map<String, Long> ephemeralNodes = new ConcurrentHashMap<>();
    private Map<String, RaftRegistryStateMachine.LockInfo> distributedLocks = new ConcurrentHashMap<>();
    private long logIndex;
    private long timestamp;
    
    public RaftSnapshot(Map<String, String> kvStore, 
                       Map<String, Long> ephemeralNodes,
                       Map<String, RaftRegistryStateMachine.LockInfo> distributedLocks,
                       long logIndex) {
        this.kvStore = new ConcurrentHashMap<>(kvStore);
        this.ephemeralNodes = new ConcurrentHashMap<>(ephemeralNodes);
        this.distributedLocks = new ConcurrentHashMap<>(distributedLocks);
        this.logIndex = logIndex;
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * Save snapshot to storage with optimized JSON format
     */
    public void save(SnapshotWriter writer) throws IOException {
        log.info("Saving Raft snapshot at index {} with {} keys", logIndex, kvStore.size());
        
        try {
            String snapshotJson = OBJECT_MAPPER.writeValueAsString(this);
            
            try (OutputStream out = writer.addFile(SNAPSHOT_FILE)) {
                out.write(snapshotJson.getBytes("UTF-8"));
            }
            
            log.info("Snapshot saved successfully: {} keys, {} ephemeral nodes, {} locks", 
                    kvStore.size(), ephemeralNodes.size(), distributedLocks.size());
        } catch (Exception e) {
            log.error("Failed to save snapshot", e);
            throw new IOException("Snapshot save failed", e);
        }
    }
    
    /**
     * Load snapshot from storage with validation
     */
    public static RaftSnapshot load(SnapshotReader reader) throws IOException {
        log.info("Loading Raft snapshot...");
        
        try {
            try (InputStream in = reader.getFile(SNAPSHOT_FILE)) {
                if (in == null) {
                    throw new IOException("Snapshot file not found: " + SNAPSHOT_FILE);
                }
                
                byte[] data = readAllBytes(in);
                String snapshotJson = new String(data, "UTF-8");
                
                RaftSnapshot snapshot = OBJECT_MAPPER.readValue(snapshotJson, RaftSnapshot.class);
                
                // Validate loaded snapshot
                if (snapshot.kvStore == null) {
                    snapshot.kvStore = new ConcurrentHashMap<>();
                }
                if (snapshot.ephemeralNodes == null) {
                    snapshot.ephemeralNodes = new ConcurrentHashMap<>();
                }
                if (snapshot.distributedLocks == null) {
                    snapshot.distributedLocks = new ConcurrentHashMap<>();
                }
                
                log.info("Snapshot loaded successfully: {} keys, {} ephemeral nodes, {} locks at index {}", 
                        snapshot.kvStore.size(), snapshot.ephemeralNodes.size(), 
                        snapshot.distributedLocks.size(), snapshot.logIndex);
                        
                return snapshot;
            }
        } catch (Exception e) {
            log.error("Failed to load snapshot", e);
            throw new IOException("Snapshot load failed", e);
        }
    }
    
    private static byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[8192];
        int bytesRead;
        
        while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, bytesRead);
        }
        
        return buffer.toByteArray();
    }
    
    /**
     * Get snapshot metadata for monitoring
     */
    public SnapshotMetadata getMetadata() {
        return SnapshotMetadata.builder()
                .logIndex(logIndex)
                .timestamp(timestamp)
                .kvStoreSize(kvStore.size())
                .ephemeralNodesSize(ephemeralNodes.size())
                .distributedLocksSize(distributedLocks.size())
                .build();
    }
    
    @lombok.Builder
    @lombok.Data
    public static class SnapshotMetadata {
        private long logIndex;
        private long timestamp;
        private int kvStoreSize;
        private int ephemeralNodesSize;
        private int distributedLocksSize;
    }
}