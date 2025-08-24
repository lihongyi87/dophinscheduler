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

package org.apache.dolphinscheduler.raft.core;

import org.apache.dolphinscheduler.raft.command.RaftCommand;
import org.apache.dolphinscheduler.raft.config.RaftConfiguration;
import org.apache.dolphinscheduler.raft.event.RaftEventType;
import org.apache.dolphinscheduler.raft.event.RaftRegistryEvent;
import org.apache.dolphinscheduler.raft.event.RaftRegistryEventListener;
import org.apache.dolphinscheduler.raft.metrics.RaftMetrics;
import org.apache.dolphinscheduler.raft.storage.RaftSnapshot;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimized Raft state machine with prefix tree and batch operations support
 */
@Slf4j
public class RaftRegistryStateMachine extends StateMachineAdapter {

    // Optimized data structures
    private final ConcurrentSkipListMap<String, String> kvStore = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, Long> ephemeralNodes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LockInfo> distributedLocks = new ConcurrentHashMap<>();
    private final AtomicLong logIndex = new AtomicLong(0);

    // Prefix tree for efficient children queries
    private final PrefixTree prefixTree = new PrefixTree();

    // Event listeners with path-based organization
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<RaftRegistryEventListener>> eventListeners = 
            new ConcurrentHashMap<>();

    // Configuration and metrics
    private final RaftConfiguration config;
    private final RaftMetrics metrics;

    // Optimized lease management
    private final ScheduledExecutorService leaseExecutor;
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    // Batch operation support
    private final LinkedBlockingQueue<RaftCommand> batchQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService batchProcessor;

    public RaftRegistryStateMachine(RaftConfiguration config, RaftMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
        
        // Optimized thread pools
        this.leaseExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-lease-manager");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thread, ex) -> 
                log.error("Uncaught exception in lease manager", ex));
            return t;
        });

        this.batchProcessor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-batch-processor");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thread, ex) -> 
                log.error("Uncaught exception in batch processor", ex));
            return t;
        });

        // Start background tasks
        startLeaseManager();
        startBatchProcessor();
    }

    @Override
    public void onApply(Iterator iter) {
        List<RaftCommand> commands = new ArrayList<>();
        List<Closure> closures = new ArrayList<>();

        // Collect batch of commands
        while (iter.hasNext()) {
            try {
                ByteBuffer data = iter.getData();
                RaftCommand command = RaftCommand.deserialize(data.array());
                commands.add(command);
                closures.add(iter.done());
                
                logIndex.set(iter.getIndex());
                metrics.incrementCommandsApplied();
            } catch (Exception e) {
                log.error("Failed to deserialize command at index {}", iter.getIndex(), e);
                if (iter.done() != null) {
                    iter.done().run(new Status(RaftError.ESTATEMACHINE, 
                            "Command deserialization failed: %s", e.getMessage()));
                }
                metrics.incrementCommandErrors();
            }
            iter.next();
        }

        // Apply commands in batch for better performance
        applyCommandsBatch(commands, closures);
    }

    private void applyCommandsBatch(List<RaftCommand> commands, List<Closure> closures) {
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < commands.size(); i++) {
            RaftCommand command = commands.get(i);
            Closure closure = closures.get(i);
            
            try {
                command.validate();
                Object result = applyCommand(command);
                
                if (closure != null) {
                    closure.run(Status.OK());
                }
                
                metrics.recordCommandLatency(System.currentTimeMillis() - command.getTimestamp());
            } catch (Exception e) {
                log.error("Failed to apply command: {}", command, e);
                if (closure != null) {
                    closure.run(new Status(RaftError.ESTATEMACHINE, 
                            "Apply command failed: %s", e.getMessage()));
                }
                metrics.incrementCommandErrors();
            }
        }
        
        metrics.recordBatchProcessTime(System.currentTimeMillis() - startTime);
        log.debug("Applied batch of {} commands in {}ms", commands.size(), 
                System.currentTimeMillis() - startTime);
    }

    private Object applyCommand(RaftCommand command) {
        switch (command.getType()) {
            case PUT:
                return handlePut(command);
            case DELETE:
                return handleDelete(command);
            case PUT_EPHEMERAL:
                return handlePutEphemeral(command);
            case ACQUIRE_LOCK:
                return handleAcquireLock(command);
            case RELEASE_LOCK:
                return handleReleaseLock(command);
            case HEARTBEAT:
                return handleHeartbeat(command);
            case BATCH_PUT:
                return handleBatchPut(command);
            case BATCH_DELETE:
                return handleBatchDelete(command);
            default:
                throw new IllegalArgumentException("Unknown command type: " + command.getType());
        }
    }

    private boolean handlePut(RaftCommand command) {
        String key = command.getKey();
        String value = command.getValue();
        String oldValue = kvStore.put(key, value);
        
        // Update prefix tree
        prefixTree.addKey(key);
        
        // Fire event
        fireEvent(key, value, oldValue == null ? RaftEventType.ADD : RaftEventType.UPDATE);
        metrics.incrementPutOperations();
        return true;
    }

    private boolean handleDelete(RaftCommand command) {
        String key = command.getKey();
        String oldValue = kvStore.remove(key);
        ephemeralNodes.remove(key);
        
        // Update prefix tree
        prefixTree.removeKey(key);
        
        if (oldValue != null) {
            fireEvent(key, null, RaftEventType.REMOVE);
            metrics.incrementDeleteOperations();
        }
        return oldValue != null;
    }

    private boolean handlePutEphemeral(RaftCommand command) {
        String key = command.getKey();
        String value = command.getValue();
        long ttlMs = command.getTtl();
        
        kvStore.put(key, value);
        ephemeralNodes.put(key, System.currentTimeMillis() + ttlMs);
        prefixTree.addKey(key);
        
        fireEvent(key, value, RaftEventType.ADD);
        metrics.incrementEphemeralOperations();
        return true;
    }

    private boolean handleBatchPut(RaftCommand command) {
        Map<String, String> batch = command.getBatch();
        int successCount = 0;
        
        for (Map.Entry<String, String> entry : batch.entrySet()) {
            try {
                String key = entry.getKey();
                String value = entry.getValue();
                String oldValue = kvStore.put(key, value);
                prefixTree.addKey(key);
                fireEvent(key, value, oldValue == null ? RaftEventType.ADD : RaftEventType.UPDATE);
                successCount++;
            } catch (Exception e) {
                log.warn("Failed to put key {} in batch", entry.getKey(), e);
            }
        }
        
        metrics.recordBatchOperation("PUT", batch.size(), successCount);
        return successCount == batch.size();
    }

    private boolean handleBatchDelete(RaftCommand command) {
        Map<String, String> batch = command.getBatch();
        int successCount = 0;
        
        for (String key : batch.keySet()) {
            try {
                String oldValue = kvStore.remove(key);
                ephemeralNodes.remove(key);
                prefixTree.removeKey(key);
                if (oldValue != null) {
                    fireEvent(key, null, RaftEventType.REMOVE);
                    successCount++;
                }
            } catch (Exception e) {
                log.warn("Failed to delete key {} in batch", key, e);
            }
        }
        
        metrics.recordBatchOperation("DELETE", batch.size(), successCount);
        return successCount == batch.size();
    }

    private boolean handleAcquireLock(RaftCommand command) {
        String key = command.getKey();
        String owner = command.getOwner();
        long ttlMs = command.getTtl();
        
        LockInfo existingLock = distributedLocks.get(key);
        if (existingLock != null && !existingLock.isExpired()) {
            if (!existingLock.getOwner().equals(owner)) {
                metrics.incrementLockContentions();
                return false; // Lock already held by another owner
            }
        }
        
        LockInfo lockInfo = new LockInfo(owner, System.currentTimeMillis() + ttlMs);
        distributedLocks.put(key, lockInfo);
        metrics.incrementLockAcquisitions();
        return true;
    }

    private boolean handleReleaseLock(RaftCommand command) {
        String key = command.getKey();
        String owner = command.getOwner();
        
        LockInfo existingLock = distributedLocks.get(key);
        if (existingLock != null && existingLock.getOwner().equals(owner)) {
            distributedLocks.remove(key);
            metrics.incrementLockReleases();
            return true;
        }
        return false;
    }

    private boolean handleHeartbeat(RaftCommand command) {
        // Update lease for ephemeral nodes owned by this client
        String owner = command.getOwner();
        long currentTime = System.currentTimeMillis();
        
        ephemeralNodes.entrySet().parallelStream()
                .filter(entry -> {
                    String key = entry.getKey();
                    String value = kvStore.get(key);
                    return value != null && value.contains(owner);
                })
                .forEach(entry -> {
                    entry.setValue(currentTime + 30000); // Extend lease by 30 seconds
                });
        
        metrics.incrementHeartbeats();
        return true;
    }

    // Optimized children query using prefix tree
    public Set<String> getAllKeys() {
        return kvStore.keySet();
    }

    public Collection<String> getChildren(String prefix) {
        return prefixTree.getChildren(prefix);
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }

    public boolean exists(String key) {
        return kvStore.containsKey(key);
    }

    public void addEventListener(String path, RaftRegistryEventListener listener) {
        eventListeners.computeIfAbsent(path, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    public void removeEventListener(String path, RaftRegistryEventListener listener) {
        CopyOnWriteArrayList<RaftRegistryEventListener> listeners = eventListeners.get(path);
        if (listeners != null) {
            listeners.remove(listener);
            if (listeners.isEmpty()) {
                eventListeners.remove(path);
            }
        }
    }

    private void fireEvent(String key, String value, RaftEventType eventType) {
        RaftRegistryEvent event = RaftRegistryEvent.builder()
                .eventPath(key)
                .watchedPath(key)
                .value(value)
                .eventType(eventType)
                .timestamp(System.currentTimeMillis())
                .build();

        // Fire to specific path listeners
        CopyOnWriteArrayList<RaftRegistryEventListener> specificListeners = eventListeners.get(key);
        if (specificListeners != null) {
            for (RaftRegistryEventListener listener : specificListeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    log.warn("Event listener failed for path: {}", key, e);
                }
            }
        }

        // Fire to prefix listeners
        for (Map.Entry<String, CopyOnWriteArrayList<RaftRegistryEventListener>> entry : eventListeners.entrySet()) {
            String watchedPath = entry.getKey();
            if (key.startsWith(watchedPath + "/") || key.equals(watchedPath)) {
                event.setWatchedPath(watchedPath);
                for (RaftRegistryEventListener listener : entry.getValue()) {
                    try {
                        listener.onEvent(event);
                    } catch (Exception e) {
                        log.warn("Event listener failed for watched path: {}", watchedPath, e);
                    }
                }
            }
        }
        
        metrics.incrementEventsGenerated();
    }

    private void startLeaseManager() {
        leaseExecutor.scheduleWithFixedDelay(this::cleanExpiredNodes, 
                config.getLeaseCleanupIntervalSecs(), 
                config.getLeaseCleanupIntervalSecs(), 
                TimeUnit.SECONDS);
    }

    private void startBatchProcessor() {
        // Optional: implement batch processing for performance optimization
        // This can be used for grouping multiple operations
    }

    private void cleanExpiredNodes() {
        long currentTime = System.currentTimeMillis();
        int expiredCount = 0;
        
        Iterator<Map.Entry<String, Long>> iterator = ephemeralNodes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (entry.getValue() < currentTime) {
                String key = entry.getKey();
                iterator.remove();
                kvStore.remove(key);
                prefixTree.removeKey(key);
                fireEvent(key, null, RaftEventType.REMOVE);
                expiredCount++;
            }
        }
        
        // Clean expired locks
        Iterator<Map.Entry<String, LockInfo>> lockIterator = distributedLocks.entrySet().iterator();
        while (lockIterator.hasNext()) {
            Map.Entry<String, LockInfo> entry = lockIterator.next();
            if (entry.getValue().isExpired()) {
                lockIterator.remove();
                expiredCount++;
            }
        }
        
        if (expiredCount > 0) {
            log.debug("Cleaned {} expired nodes and locks", expiredCount);
            metrics.recordExpiredNodesCleanup(expiredCount);
        }
    }

    @Override
    public void onShutdown() {
        log.info("Shutting down Raft state machine...");
        
        leaseExecutor.shutdown();
        batchProcessor.shutdown();
        
        try {
            if (!leaseExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                leaseExecutor.shutdownNow();
            }
            if (!batchProcessor.awaitTermination(5, TimeUnit.SECONDS)) {
                batchProcessor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for executors to terminate");
        }
        
        shutdownFuture.complete(null);
        log.info("Raft state machine shutdown completed");
    }

    @Override
    public void onLeaderStart(long term) {
        log.info("Became leader for term: {}", term);
        metrics.recordLeaderElection(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        log.info("Stopped being leader: {}", status);
        metrics.recordLeaderStop();
    }

    @Override
    public void onStartFollowing(long leaderTerm) {
        log.info("Started following leader for term: {}", leaderTerm);
        metrics.recordFollowerStart();
    }

    @Override
    public void onStopFollowing(long leaderTerm) {
        log.info("Stopped following leader for term: {}", leaderTerm);
        metrics.recordFollowerStop();
    }

    // Snapshot operations
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        try {
            RaftSnapshot snapshot = new RaftSnapshot(kvStore, ephemeralNodes, distributedLocks, logIndex.get());
            snapshot.save(writer);
            done.run(Status.OK());
            metrics.recordSnapshotSave();
            log.info("Snapshot saved successfully at index: {}", logIndex.get());
        } catch (Exception e) {
            log.error("Failed to save snapshot", e);
            done.run(new Status(RaftError.EIO, "Failed to save snapshot: %s", e.getMessage()));
            metrics.incrementSnapshotErrors();
        }
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            RaftSnapshot snapshot = RaftSnapshot.load(reader);
            
            // Clear current state
            kvStore.clear();
            ephemeralNodes.clear();
            distributedLocks.clear();
            prefixTree.clear();
            
            // Restore from snapshot
            kvStore.putAll(snapshot.getKvStore());
            ephemeralNodes.putAll(snapshot.getEphemeralNodes());
            distributedLocks.putAll(snapshot.getDistributedLocks());
            logIndex.set(snapshot.getLogIndex());
            
            // Rebuild prefix tree
            for (String key : kvStore.keySet()) {
                prefixTree.addKey(key);
            }
            
            metrics.recordSnapshotLoad();
            log.info("Snapshot loaded successfully, restored {} keys", kvStore.size());
            return true;
        } catch (Exception e) {
            log.error("Failed to load snapshot", e);
            metrics.incrementSnapshotErrors();
            return false;
        }
    }

    // Lock info class
    @lombok.Data
    public static class LockInfo implements Serializable {
        private final String owner;
        private final long expireTime;
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expireTime;
        }
    }

    // Simple prefix tree implementation for efficient children queries
    private static class PrefixTree {
        private final ConcurrentHashMap<String, Set<String>> prefixToChildren = new ConcurrentHashMap<>();
        
        public void addKey(String key) {
            String[] parts = key.split("/");
            StringBuilder prefix = new StringBuilder();
            
            for (int i = 0; i < parts.length - 1; i++) {
                if (i > 0) prefix.append("/");
                prefix.append(parts[i]);
                
                String prefixStr = prefix.toString();
                String child = (i == parts.length - 2) ? key : prefix + "/" + parts[i + 1];
                
                prefixToChildren.computeIfAbsent(prefixStr, k -> ConcurrentHashMap.newKeySet()).add(child);
            }
        }
        
        public void removeKey(String key) {
            String[] parts = key.split("/");
            StringBuilder prefix = new StringBuilder();
            
            for (int i = 0; i < parts.length - 1; i++) {
                if (i > 0) prefix.append("/");
                prefix.append(parts[i]);
                
                String prefixStr = prefix.toString();
                Set<String> children = prefixToChildren.get(prefixStr);
                if (children != null) {
                    children.remove(key);
                    if (children.isEmpty()) {
                        prefixToChildren.remove(prefixStr);
                    }
                }
            }
        }
        
        public Collection<String> getChildren(String prefix) {
            Set<String> children = prefixToChildren.get(prefix);
            return children != null ? new ArrayList<>(children) : Collections.emptyList();
        }
        
        public void clear() {
            prefixToChildren.clear();
        }
    }
}