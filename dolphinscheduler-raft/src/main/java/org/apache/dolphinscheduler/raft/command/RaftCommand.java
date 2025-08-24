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

package org.apache.dolphinscheduler.raft.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Optimized Raft command with better serialization and validation
 */
@Slf4j
@Data
public class RaftCommand {
    
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    public enum Type {
        PUT,
        DELETE,
        PUT_EPHEMERAL,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        HEARTBEAT,
        BATCH_PUT,
        BATCH_DELETE
    }
    
    private final Type type;
    private final String key;
    private final String value;
    private final long ttl;
    private final String owner;
    private final long timestamp;
    private final String requestId;
    
    // For batch operations
    private final java.util.Map<String, String> batch;
    
    @JsonCreator
    public RaftCommand(
            @JsonProperty("type") Type type,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("ttl") long ttl,
            @JsonProperty("owner") String owner,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("requestId") String requestId,
            @JsonProperty("batch") java.util.Map<String, String> batch) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.ttl = ttl;
        this.owner = owner;
        this.timestamp = timestamp == 0 ? System.currentTimeMillis() : timestamp;
        this.requestId = requestId != null ? requestId : generateRequestId();
        this.batch = batch;
    }
    
    // Factory methods
    public static RaftCommand put(String key, String value) {
        return new RaftCommand(Type.PUT, key, value, 0, null, 0, null, null);
    }
    
    public static RaftCommand delete(String key) {
        return new RaftCommand(Type.DELETE, key, null, 0, null, 0, null, null);
    }
    
    public static RaftCommand putEphemeral(String key, String value, long ttlMs) {
        return new RaftCommand(Type.PUT_EPHEMERAL, key, value, ttlMs, null, 0, null, null);
    }
    
    public static RaftCommand acquireLock(String key, String owner, long ttlMs) {
        return new RaftCommand(Type.ACQUIRE_LOCK, key, null, ttlMs, owner, 0, null, null);
    }
    
    public static RaftCommand releaseLock(String key, String owner) {
        return new RaftCommand(Type.RELEASE_LOCK, key, null, 0, owner, 0, null, null);
    }
    
    public static RaftCommand heartbeat(String owner) {
        return new RaftCommand(Type.HEARTBEAT, null, null, 0, owner, 0, null, null);
    }
    
    public static RaftCommand batchPut(java.util.Map<String, String> batch) {
        return new RaftCommand(Type.BATCH_PUT, null, null, 0, null, 0, null, batch);
    }
    
    public static RaftCommand batchDelete(java.util.Map<String, String> batch) {
        return new RaftCommand(Type.BATCH_DELETE, null, null, 0, null, 0, null, batch);
    }
    
    /**
     * Serialize command to bytes with optimized JSON
     */
    public byte[] serialize() throws IOException {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(this);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize command: {}", this, e);
            throw new IOException("Command serialization failed", e);
        }
    }
    
    /**
     * Deserialize command from bytes
     */
    public static RaftCommand deserialize(byte[] data) throws IOException {
        try {
            String json = new String(data, StandardCharsets.UTF_8);
            return OBJECT_MAPPER.readValue(json, RaftCommand.class);
        } catch (Exception e) {
            log.error("Failed to deserialize command from: {}", new String(data, StandardCharsets.UTF_8), e);
            throw new IOException("Command deserialization failed", e);
        }
    }
    
    /**
     * Generate unique request ID
     */
    private static String generateRequestId() {
        return System.currentTimeMillis() + "-" + 
               Thread.currentThread().getId() + "-" +
               (int)(Math.random() * 1000);
    }
    
    /**
     * Validate command data
     */
    public void validate() throws IllegalArgumentException {
        if (type == null) {
            throw new IllegalArgumentException("Command type cannot be null");
        }
        
        switch (type) {
            case PUT:
            case PUT_EPHEMERAL:
                if (key == null || key.trim().isEmpty()) {
                    throw new IllegalArgumentException("Key cannot be null or empty for PUT operations");
                }
                break;
            case DELETE:
                if (key == null || key.trim().isEmpty()) {
                    throw new IllegalArgumentException("Key cannot be null or empty for DELETE operations");
                }
                break;
            case ACQUIRE_LOCK:
            case RELEASE_LOCK:
                if (key == null || key.trim().isEmpty() || owner == null || owner.trim().isEmpty()) {
                    throw new IllegalArgumentException("Key and owner cannot be null or empty for LOCK operations");
                }
                break;
            case BATCH_PUT:
            case BATCH_DELETE:
                if (batch == null || batch.isEmpty()) {
                    throw new IllegalArgumentException("Batch cannot be null or empty for BATCH operations");
                }
                break;
            case HEARTBEAT:
                if (owner == null || owner.trim().isEmpty()) {
                    throw new IllegalArgumentException("Owner cannot be null or empty for HEARTBEAT operations");
                }
                break;
            default:
                // No additional validation needed
        }
    }
    
    /**
     * Check if command has expired based on TTL
     */
    public boolean isExpired() {
        return ttl > 0 && (System.currentTimeMillis() - timestamp) > ttl;
    }
    
    @Override
    public String toString() {
        return String.format("RaftCommand{type=%s, key='%s', value='%s', ttl=%d, owner='%s', requestId='%s'}", 
                type, key, value != null ? value.substring(0, Math.min(50, value.length())) : null, 
                ttl, owner, requestId);
    }
}