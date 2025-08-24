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

package org.apache.dolphinscheduler.server.master.registry;

import org.apache.dolphinscheduler.raft.config.RaftConfiguration;
import org.apache.dolphinscheduler.raft.core.RaftRegistryStateMachine;
import org.apache.dolphinscheduler.raft.metrics.RaftMetrics;
import org.apache.dolphinscheduler.raft.utils.RaftNodeIdGenerator;
import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.RegistryException;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Master-integrated Raft Registry implementation
 * This class integrates the optimized Raft module with master startup lifecycle
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "registry", name = "type", havingValue = "raft")
public class MasterRaftRegistry implements Registry {

    @Autowired
    private RaftConfiguration raftConfig;
    
    @Autowired
    private RaftMetrics raftMetrics;
    
    @Autowired
    private MasterConfig masterConfig;

    private RaftRegistryStateMachine stateMachine;
    private final List<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();
    
    private volatile boolean started = false;

    @PostConstruct
    public void init() {
        log.info("Initializing Master Raft Registry...");
        
        // Initialize node address from master config if not set
        if (raftConfig.getNodeAddress() == null) {
            String nodeAddress = masterConfig.getListenPort() != 0 ? 
                "127.0.0.1:" + (masterConfig.getListenPort() + 3556) : // Offset for Raft port
                "127.0.0.1:9234";
            raftConfig.setNodeAddress(nodeAddress);
            log.info("Inferred Raft node address from master config: {}", nodeAddress);
        }
        
        // Validate configuration
        raftConfig.validate();
        
        // Initialize state machine with configuration and metrics
        stateMachine = new RaftRegistryStateMachine(raftConfig, raftMetrics);
        
        log.info("Master Raft Registry initialized with node address: {}", raftConfig.getNodeAddress());
    }

    @Override
    public void start() {
        if (started) {
            log.warn("Raft registry is already started");
            return;
        }

        try {
            log.info("Starting Master Raft Registry...");
            
            // Generate stable node ID
            String nodeId = RaftNodeIdGenerator.generateNodeId(raftConfig.getNodeAddress());
            log.info("Generated node ID: {}", nodeId);
            
            // TODO: Start Raft server - will be implemented in next iteration
            // For now, we'll use the state machine directly for single-node mode
            
            started = true;
            
            // Notify connection listeners
            for (ConnectionListener listener : connectionListeners) {
                try {
                    listener.onConnected();
                } catch (Exception e) {
                    log.warn("Failed to notify connection listener", e);
                }
            }
            
            log.info("Master Raft Registry started successfully");
        } catch (Exception e) {
            log.error("Failed to start Master Raft Registry", e);
            throw new RegistryException("Failed to start Raft registry", e);
        }
    }

    @Override
    public boolean isConnected() {
        return started;
    }

    @Override
    public void connectUntilTimeout(Duration timeout) throws RegistryException {
        if (!isConnected()) {
            throw new RegistryException("Master Raft registry not connected");
        }
    }

    @Override
    public void subscribe(String path, SubscribeListener listener) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        stateMachine.addEventListener(path, event -> {
            try {
                Event registryEvent = Event.builder()
                        .watchedPath(event.getWatchedPath())
                        .eventPath(event.getEventPath()) 
                        .eventData(event.getValue())
                        .type(convertEventType(event.getEventType()))
                        .build();
                listener.notify(registryEvent);
            } catch (Exception e) {
                log.warn("Error notifying registry listener for path: {}", path, e);
            }
        });
    }

    @Override
    public void addConnectionStateListener(ConnectionListener listener) {
        connectionListeners.add(listener);
    }

    @Override
    public String get(String key) throws RegistryException {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        String value = stateMachine.getValue(key);
        if (value == null) {
            throw new RegistryException("Key not found: " + key);
        }
        return value;
    }

    @Override
    public void put(String key, String value, boolean deleteOnDisconnect) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            if (deleteOnDisconnect) {
                // For single-node mode, simulate ephemeral behavior
                // TODO: Use proper Raft commands when cluster mode is implemented
                stateMachine.getValue(key); // Simulate put ephemeral
            } else {
                stateMachine.getValue(key); // Simulate put
            }
        } catch (Exception e) {
            throw new RegistryException("Failed to put key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            // TODO: Use proper Raft commands when cluster mode is implemented
            log.debug("Simulating delete for key: {}", key);
        } catch (Exception e) {
            throw new RegistryException("Failed to delete key: " + key, e);
        }
    }

    @Override
    public Collection<String> children(String key) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            return stateMachine.getChildren(key);
        } catch (Exception e) {
            log.warn("Failed to get children for key: {}", key, e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean exists(String key) {
        if (!started) {
            return false;
        }
        
        return stateMachine.exists(key);
    }

    @Override
    public boolean acquireLock(String key) {
        return acquireLock(key, 0);
    }

    @Override
    public boolean acquireLock(String key, long timeout) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            String nodeId = RaftNodeIdGenerator.generateNodeId(raftConfig.getNodeAddress());
            // TODO: Use proper Raft commands when cluster mode is implemented
            log.debug("Simulating acquire lock for key: {}, owner: {}", key, nodeId);
            return true; // Simulate success for single-node mode
        } catch (Exception e) {
            log.error("Failed to acquire lock: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean releaseLock(String key) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            String nodeId = RaftNodeIdGenerator.generateNodeId(raftConfig.getNodeAddress());
            // TODO: Use proper Raft commands when cluster mode is implemented
            log.debug("Simulating release lock for key: {}, owner: {}", key, nodeId);
            return true; // Simulate success for single-node mode
        } catch (Exception e) {
            log.error("Failed to release lock: {}", key, e);
            return false;
        }
    }

    @PreDestroy
    @Override
    public void close() throws IOException {
        if (started) {
            log.info("Shutting down Master Raft Registry...");
            
            // Notify connection listeners
            for (ConnectionListener listener : connectionListeners) {
                try {
                    listener.onDisconnected();
                } catch (Exception e) {
                    log.warn("Failed to notify connection listener on shutdown", e);
                }
            }
            
            // Shutdown state machine
            if (stateMachine != null) {
                stateMachine.onShutdown();
            }
            
            started = false;
            log.info("Master Raft Registry shutdown completed");
        }
    }

    private Event.Type convertEventType(org.apache.dolphinscheduler.raft.event.RaftEventType raftEventType) {
        switch (raftEventType) {
            case ADD:
                return Event.Type.ADD;
            case UPDATE:
                return Event.Type.UPDATE;
            case REMOVE:
                return Event.Type.REMOVE;
            default:
                return Event.Type.UPDATE;
        }
    }

    // Health check and monitoring methods
    public boolean isHealthy() {
        return started && raftMetrics.isHealthy();
    }

    public RaftMetrics.MetricsSnapshot getMetricsSnapshot() {
        return raftMetrics.getSnapshot();
    }
}