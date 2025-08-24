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

package org.apache.dolphinscheduler.raft.utils;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Optimized node ID generator for Raft cluster
 */
@Slf4j
public class RaftNodeIdGenerator {

    private static volatile String cachedNodeId;

    /**
     * Generate stable node ID based on MAC address and process info
     * Format: {mac}-{pid}-{port}
     */
    public static String generateNodeId(String nodeAddress) {
        if (cachedNodeId != null) {
            return cachedNodeId;
        }

        synchronized (RaftNodeIdGenerator.class) {
            if (cachedNodeId != null) {
                return cachedNodeId;
            }

            try {
                String macAddress = getMacAddress();
                String processId = getProcessId();
                String port = extractPortFromAddress(nodeAddress);
                
                cachedNodeId = String.format("%s-%s-%s", macAddress, processId, port);
                log.info("Generated stable node ID: {}", cachedNodeId);
                return cachedNodeId;
            } catch (Exception e) {
                // Fallback to less stable but unique ID
                String fallbackId = "fallback-" + System.currentTimeMillis() + "-" + 
                                   ThreadLocalRandom.current().nextInt(10000);
                log.warn("Failed to generate stable node ID, using fallback: {}", fallbackId, e);
                cachedNodeId = fallbackId;
                return cachedNodeId;
            }
        }
    }

    /**
     * Get MAC address of the first non-loopback network interface
     */
    private static String getMacAddress() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            
            // Skip loopback and virtual interfaces
            if (networkInterface.isLoopback() || networkInterface.isVirtual() || 
                !networkInterface.isUp()) {
                continue;
            }
            
            byte[] mac = networkInterface.getHardwareAddress();
            if (mac != null && mac.length > 0) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < mac.length; i++) {
                    sb.append(String.format("%02X", mac[i]));
                    if (i < mac.length - 1) {
                        sb.append(":");
                    }
                }
                return sb.toString();
            }
        }
        
        // Fallback: use local host address hash
        return String.valueOf(InetAddress.getLocalHost().getHostAddress().hashCode());
    }

    /**
     * Get current process ID
     */
    private static String getProcessId() {
        try {
            String jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            return jvmName.split("@")[0];
        } catch (Exception e) {
            // Fallback to thread ID
            return String.valueOf(Thread.currentThread().getId());
        }
    }

    /**
     * Extract port from node address (e.g., "127.0.0.1:9234" -> "9234")
     */
    private static String extractPortFromAddress(String nodeAddress) {
        if (nodeAddress != null && nodeAddress.contains(":")) {
            return nodeAddress.substring(nodeAddress.lastIndexOf(":") + 1);
        }
        return "9234"; // default port
    }

    /**
     * Reset cached node ID (mainly for testing)
     */
    public static void resetCache() {
        cachedNodeId = null;
    }
}