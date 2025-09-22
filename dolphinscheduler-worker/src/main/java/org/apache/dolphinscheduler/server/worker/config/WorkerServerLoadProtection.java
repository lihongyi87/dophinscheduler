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

import org.apache.dolphinscheduler.meter.metrics.BaseServerLoadProtection;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskExecutorContainerProvider;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Worker服务器负载保护组件
 *
 * 这个组件继承自基础的服务器负载保护，专门为Worker节点提供负载保护机制。
 * 它会监控系统资源使用情况，当资源紧张时阻止接收新的任务，避免服务器过载。
 *
 * 保护策略包括：
 * 1. 基础系统指标保护（CPU、内存、磁盘）
 * 2. Worker特有的任务执行容器负载保护
 *
 * 工作原理：
 * - 定期检查系统资源使用率
 * - 检查任务执行容器的使用情况
 * - 当任何一项指标超过阈值时，标记为过载状态
 * - 过载时拒绝接收新的任务分配
 */
@Slf4j
@Component
public class WorkerServerLoadProtection extends BaseServerLoadProtection {

    /**
     * 物理任务执行容器提供者
     * 用于获取当前任务执行容器的状态信息
     */
    @Autowired
    private PhysicalTaskExecutorContainerProvider physicalTaskExecutorContainerDelegator;

    /**
     * 构造函数
     *
     * @param workerConfig Worker配置，包含负载保护的配置参数
     */
    public WorkerServerLoadProtection(WorkerConfig workerConfig) {
        // 调用父类构造函数，传入Worker的负载保护配置
        // 父类会初始化基础的负载保护阈值（CPU、内存、磁盘使用率等）
        super(workerConfig.getServerLoadProtection());
    }

    /**
     * 判断Worker节点是否过载
     *
     * 这个方法会综合检查多个指标来判断Worker是否过载：
     * 1. 首先检查负载保护是否启用
     * 2. 然后检查基础系统指标（CPU、内存、磁盘）
     * 3. 最后检查Worker特有的任务执行容器使用率
     *
     * @param systemMetrics 系统指标信息，包含CPU、内存、磁盘等使用率
     * @return true: 系统过载，应该拒绝新任务; false: 系统正常，可以接收新任务
     */
    @Override
    public boolean isOverload(SystemMetrics systemMetrics) {
        // 第一层检查：如果负载保护功能未启用，直接返回false
        // 这样可以在不需要负载保护时避免不必要的计算开销
        if (!baseServerLoadProtectionConfig.isEnabled()) {
            return false;
        }

        // 第二层检查：调用父类方法检查基础系统指标
        // 包括系统CPU使用率、JVM CPU使用率、系统内存使用率、磁盘使用率
        // 如果任何一项超过配置的阈值，直接返回true表示过载
        if (super.isOverload(systemMetrics)) {
            return true;
        }

        // 第三层检查：Worker特有的任务执行容器槽位使用率
        // slotUsage()返回值为0-1之间的浮点数，表示槽位使用百分比
        // 当返回1时，表示所有执行线程都被占用，无法接收新任务
        if (physicalTaskExecutorContainerDelegator.getExecutorContainer().slotUsage() == 1) {
            // 记录过载信息到日志，帮助运维人员了解过载原因
            log.info("OverLoad: the TaskExecutorContainer slot usage is 1");
            return true;
        }

        // 所有检查都通过，返回false表示系统正常，可以接收新任务
        return false;
    }
}
