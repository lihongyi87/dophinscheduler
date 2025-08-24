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

package org.apache.dolphinscheduler.raft.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raft metrics collector for monitoring and observability
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "raft", name = "enable-metrics", havingValue = "true", matchIfMissing = true)
public class RaftMetrics {

    private final MeterRegistry meterRegistry;

    // Counters
    private Counter commandsAppliedCounter;
    private Counter commandErrorsCounter;
    private Counter putOperationsCounter;
    private Counter deleteOperationsCounter;
    private Counter ephemeralOperationsCounter;
    private Counter lockAcquisitionsCounter;
    private Counter lockReleasesCounter;
    private Counter lockContentionsCounter;
    private Counter heartbeatsCounter;
    private Counter eventsGeneratedCounter;
    private Counter snapshotSaveCounter;
    private Counter snapshotLoadCounter;
    private Counter snapshotErrorsCounter;
    private Counter leaderElectionsCounter;
    private Counter leaderStopsCounter;
    private Counter followerStartsCounter;
    private Counter followerStopsCounter;

    // Timers
    private Timer commandLatencyTimer;
    private Timer batchProcessTimeTimer;
    private Timer snapshotSaveTimer;
    private Timer snapshotLoadTimer;

    // Gauges
    private AtomicLong kvStoreSize = new AtomicLong(0);
    private AtomicLong ephemeralNodesSize = new AtomicLong(0);
    private AtomicLong distributedLocksSize = new AtomicLong(0);
    private AtomicLong currentTerm = new AtomicLong(0);
    private AtomicLong logIndex = new AtomicLong(0);
    private AtomicLong expiredNodesCleanedUp = new AtomicLong(0);

    // Custom metrics
    private final AtomicLong batchOperations = new AtomicLong(0);
    private final AtomicLong batchSuccesses = new AtomicLong(0);

    @Autowired(required = false)
    public RaftMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry != null ? meterRegistry : Metrics.globalRegistry;
    }

    @PostConstruct
    public void initMetrics() {
        if (meterRegistry == null) {
            log.warn("MeterRegistry is not available, metrics will not be collected");
            return;
        }

        log.info("Initializing Raft metrics...");

        // Initialize counters
        commandsAppliedCounter = Counter.builder("raft.commands.applied")
                .description("Total number of commands applied")
                .register(meterRegistry);

        commandErrorsCounter = Counter.builder("raft.commands.errors")
                .description("Total number of command errors")
                .register(meterRegistry);

        putOperationsCounter = Counter.builder("raft.operations.put")
                .description("Total number of PUT operations")
                .register(meterRegistry);

        deleteOperationsCounter = Counter.builder("raft.operations.delete")
                .description("Total number of DELETE operations")
                .register(meterRegistry);

        ephemeralOperationsCounter = Counter.builder("raft.operations.ephemeral")
                .description("Total number of ephemeral operations")
                .register(meterRegistry);

        lockAcquisitionsCounter = Counter.builder("raft.locks.acquisitions")
                .description("Total number of lock acquisitions")
                .register(meterRegistry);

        lockReleasesCounter = Counter.builder("raft.locks.releases")
                .description("Total number of lock releases")
                .register(meterRegistry);

        lockContentionsCounter = Counter.builder("raft.locks.contentions")
                .description("Total number of lock contentions")
                .register(meterRegistry);

        heartbeatsCounter = Counter.builder("raft.heartbeats")
                .description("Total number of heartbeats processed")
                .register(meterRegistry);

        eventsGeneratedCounter = Counter.builder("raft.events.generated")
                .description("Total number of events generated")
                .register(meterRegistry);

        snapshotSaveCounter = Counter.builder("raft.snapshots.saves")
                .description("Total number of snapshot saves")
                .register(meterRegistry);

        snapshotLoadCounter = Counter.builder("raft.snapshots.loads")
                .description("Total number of snapshot loads")
                .register(meterRegistry);

        snapshotErrorsCounter = Counter.builder("raft.snapshots.errors")
                .description("Total number of snapshot errors")
                .register(meterRegistry);

        leaderElectionsCounter = Counter.builder("raft.leader.elections")
                .description("Total number of leader elections")
                .register(meterRegistry);

        leaderStopsCounter = Counter.builder("raft.leader.stops")
                .description("Total number of times stopped being leader")
                .register(meterRegistry);

        followerStartsCounter = Counter.builder("raft.follower.starts")
                .description("Total number of times started following")
                .register(meterRegistry);

        followerStopsCounter = Counter.builder("raft.follower.stops")
                .description("Total number of times stopped following")
                .register(meterRegistry);

        // Initialize timers
        commandLatencyTimer = Timer.builder("raft.command.latency")
                .description("Command execution latency")
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(10))
                .register(meterRegistry);

        batchProcessTimeTimer = Timer.builder("raft.batch.process.time")
                .description("Batch processing time")
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(5))
                .register(meterRegistry);

        snapshotSaveTimer = Timer.builder("raft.snapshot.save.time")
                .description("Snapshot save time")
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(meterRegistry);

        snapshotLoadTimer = Timer.builder("raft.snapshot.load.time")
                .description("Snapshot load time")
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(meterRegistry);

        // Initialize gauges
        Gauge.builder("raft.kvstore.size")
                .description("Current size of KV store")
                .register(meterRegistry, kvStoreSize, AtomicLong::get);

        Gauge.builder("raft.ephemeral.nodes.size")
                .description("Current number of ephemeral nodes")
                .register(meterRegistry, ephemeralNodesSize, AtomicLong::get);

        Gauge.builder("raft.locks.size")
                .description("Current number of distributed locks")
                .register(meterRegistry, distributedLocksSize, AtomicLong::get);

        Gauge.builder("raft.current.term")
                .description("Current Raft term")
                .register(meterRegistry, currentTerm, AtomicLong::get);

        Gauge.builder("raft.log.index")
                .description("Current log index")
                .register(meterRegistry, logIndex, AtomicLong::get);

        Gauge.builder("raft.expired.nodes.cleaned")
                .description("Number of expired nodes cleaned up")
                .register(meterRegistry, expiredNodesCleanedUp, AtomicLong::get);

        // Custom derived metrics
        Gauge.builder("raft.batch.success.rate")
                .description("Batch operation success rate")
                .register(meterRegistry, this, metrics -> {
                    long operations = batchOperations.get();
                    long successes = batchSuccesses.get();
                    return operations > 0 ? (double) successes / operations : 0.0;
                });

        log.info("Raft metrics initialized successfully");
    }

    // Counter methods
    public void incrementCommandsApplied() {
        if (commandsAppliedCounter != null) {
            commandsAppliedCounter.increment();
        }
    }

    public void incrementCommandErrors() {
        if (commandErrorsCounter != null) {
            commandErrorsCounter.increment();
        }
    }

    public void incrementPutOperations() {
        if (putOperationsCounter != null) {
            putOperationsCounter.increment();
        }
    }

    public void incrementDeleteOperations() {
        if (deleteOperationsCounter != null) {
            deleteOperationsCounter.increment();
        }
    }

    public void incrementEphemeralOperations() {
        if (ephemeralOperationsCounter != null) {
            ephemeralOperationsCounter.increment();
        }
    }

    public void incrementLockAcquisitions() {
        if (lockAcquisitionsCounter != null) {
            lockAcquisitionsCounter.increment();
        }
    }

    public void incrementLockReleases() {
        if (lockReleasesCounter != null) {
            lockReleasesCounter.increment();
        }
    }

    public void incrementLockContentions() {
        if (lockContentionsCounter != null) {
            lockContentionsCounter.increment();
        }
    }

    public void incrementHeartbeats() {
        if (heartbeatsCounter != null) {
            heartbeatsCounter.increment();
        }
    }

    public void incrementEventsGenerated() {
        if (eventsGeneratedCounter != null) {
            eventsGeneratedCounter.increment();
        }
    }

    public void incrementSnapshotErrors() {
        if (snapshotErrorsCounter != null) {
            snapshotErrorsCounter.increment();
        }
    }

    // Timer methods
    public void recordCommandLatency(long latencyMs) {
        if (commandLatencyTimer != null) {
            commandLatencyTimer.record(Duration.ofMillis(latencyMs));
        }
    }

    public void recordBatchProcessTime(long processTimeMs) {
        if (batchProcessTimeTimer != null) {
            batchProcessTimeTimer.record(Duration.ofMillis(processTimeMs));
        }
    }

    public void recordSnapshotSave() {
        if (snapshotSaveCounter != null) {
            snapshotSaveCounter.increment();
        }
    }

    public void recordSnapshotLoad() {
        if (snapshotLoadCounter != null) {
            snapshotLoadCounter.increment();
        }
    }

    public Timer.Sample startSnapshotSaveTimer() {
        return snapshotSaveTimer != null ? Timer.start(meterRegistry) : null;
    }

    public void stopSnapshotSaveTimer(Timer.Sample sample) {
        if (sample != null && snapshotSaveTimer != null) {
            sample.stop(snapshotSaveTimer);
        }
    }

    public Timer.Sample startSnapshotLoadTimer() {
        return snapshotLoadTimer != null ? Timer.start(meterRegistry) : null;
    }

    public void stopSnapshotLoadTimer(Timer.Sample sample) {
        if (sample != null && snapshotLoadTimer != null) {
            sample.stop(snapshotLoadTimer);
        }
    }

    // Gauge update methods
    public void updateKvStoreSize(long size) {
        kvStoreSize.set(size);
    }

    public void updateEphemeralNodesSize(long size) {
        ephemeralNodesSize.set(size);
    }

    public void updateDistributedLocksSize(long size) {
        distributedLocksSize.set(size);
    }

    public void updateCurrentTerm(long term) {
        currentTerm.set(term);
    }

    public void updateLogIndex(long index) {
        logIndex.set(index);
    }

    // Leadership tracking
    public void recordLeaderElection(long term) {
        if (leaderElectionsCounter != null) {
            leaderElectionsCounter.increment();
        }
        updateCurrentTerm(term);
    }

    public void recordLeaderStop() {
        if (leaderStopsCounter != null) {
            leaderStopsCounter.increment();
        }
    }

    public void recordFollowerStart() {
        if (followerStartsCounter != null) {
            followerStartsCounter.increment();
        }
    }

    public void recordFollowerStop() {
        if (followerStopsCounter != null) {
            followerStopsCounter.increment();
        }
    }

    // Batch operations tracking
    public void recordBatchOperation(String operationType, int total, int successful) {
        batchOperations.addAndGet(total);
        batchSuccesses.addAndGet(successful);
        
        // Also record with tags for more detailed analysis
        if (meterRegistry != null) {
            Counter.builder("raft.batch.operations")
                    .tag("type", operationType.toLowerCase())
                    .register(meterRegistry)
                    .increment(total);
            
            Counter.builder("raft.batch.successes")
                    .tag("type", operationType.toLowerCase())
                    .register(meterRegistry)
                    .increment(successful);
        }
    }

    // Cleanup tracking
    public void recordExpiredNodesCleanup(int cleanedCount) {
        expiredNodesCleanedUp.addAndGet(cleanedCount);
    }

    // Health check methods
    public boolean isHealthy() {
        // Simple health check based on error rates
        if (commandsAppliedCounter == null || commandErrorsCounter == null) {
            return true; // If metrics are disabled, assume healthy
        }
        
        double totalCommands = commandsAppliedCounter.count();
        double errorCommands = commandErrorsCounter.count();
        
        if (totalCommands == 0) {
            return true; // No commands processed yet
        }
        
        double errorRate = errorCommands / totalCommands;
        return errorRate < 0.05; // Less than 5% error rate
    }

    public MetricsSnapshot getSnapshot() {
        return MetricsSnapshot.builder()
                .commandsApplied(commandsAppliedCounter != null ? (long) commandsAppliedCounter.count() : 0)
                .commandErrors(commandErrorsCounter != null ? (long) commandErrorsCounter.count() : 0)
                .putOperations(putOperationsCounter != null ? (long) putOperationsCounter.count() : 0)
                .deleteOperations(deleteOperationsCounter != null ? (long) deleteOperationsCounter.count() : 0)
                .kvStoreSize(kvStoreSize.get())
                .ephemeralNodesSize(ephemeralNodesSize.get())
                .distributedLocksSize(distributedLocksSize.get())
                .currentTerm(currentTerm.get())
                .logIndex(logIndex.get())
                .build();
    }

    @lombok.Builder
    @lombok.Data
    public static class MetricsSnapshot {
        private final long commandsApplied;
        private final long commandErrors;
        private final long putOperations;
        private final long deleteOperations;
        private final long kvStoreSize;
        private final long ephemeralNodesSize;
        private final long distributedLocksSize;
        private final long currentTerm;
        private final long logIndex;
    }
}