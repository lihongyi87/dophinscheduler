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

package org.apache.dolphinscheduler.server.worker.config;

import org.apache.dolphinscheduler.meter.metrics.BaseServerLoadProtectionConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Worker服务器负载保护配置类
 *
 * 这个类继承自基础的服务器负载保护配置，专门用于配置Worker节点的负载保护参数。
 * 它复用了父类的所有配置项，包括：
 *
 * 基础保护配置项（继承自BaseServerLoadProtectionConfig）：
 * - enabled: 是否启用负载保护功能（默认：true）
 * - maxSystemCpuUsagePercentageThresholds: 系统CPU使用率阈值（默认：0.7，即70%）
 * - maxJvmCpuUsagePercentageThresholds: JVM CPU使用率阈值（默认：0.7，即70%）
 * - maxSystemMemoryUsagePercentageThresholds: 系统内存使用率阈值（默认：0.7，即70%）
 * - maxDiskUsagePercentageThresholds: 磁盘使用率阈值（默认：0.7，即70%）
 *
 * 使用场景：
 * - 防止Worker节点因资源不足导致任务执行失败
 * - 保证系统稳定性，避免因过载导致的服务崩溃
 * - 在集群环境中实现负载均衡，让资源充足的节点承担更多任务
 *
 * 配置示例（在application.yaml中）：
 * worker:
 *   server-load-protection:
 *     enabled: true
 *     max-system-cpu-usage-percentage-thresholds: 0.8
 *     max-jvm-cpu-usage-percentage-thresholds: 0.8
 *     max-system-memory-usage-percentage-thresholds: 0.8
 *     max-disk-usage-percentage-thresholds: 0.9
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class WorkerServerLoadProtectionConfig extends BaseServerLoadProtectionConfig {
    // 当前类没有额外的配置项，完全继承父类的配置
    // 这种设计方式为将来可能的Worker特有配置预留了扩展空间
}
