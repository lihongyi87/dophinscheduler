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

package org.apache.dolphinscheduler.server.master.config;

import org.apache.dolphinscheduler.meter.metrics.BaseServerLoadProtection;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * Master服务器负载保护实现类
 * 
 * 这个类负责监控Master节点的资源使用情况，并在资源过载时阻止新任务的接收。
 * 它继承了通用的基类负载保护机制，并增加了Master特有的保护阶层。
 * 
 * 保护机制包括：
 * 1. 基础资源保护：CPU、内存、磁盘使用率
 * 2. 并发工作流实例数量限制
 * 
 * 简单理解：就像一个智能的“门卫”，在系统过载时拒绝新的访问者。
 */
@Slf4j
@Component
public class MasterServerLoadProtection extends BaseServerLoadProtection {

    /**
     * 工作流仓库 - 用于获取当前正在运行的工作流实例数量
     */
    private final IWorkflowRepository workflowRepository;

    /**
     * Master服务器负载保护配置 - 包含各种阈值设置
     */
    private final MasterServerLoadProtectionConfig masterServerLoadProtectionConfig;

    /**
     * Master服务器负载保护构造函数
     * 
     * @param workflowRepository 工作流仓库，用于获取当前运行的工作流数量
     * @param masterConfig Master配置对象，包含负载保护配置
     */
    public MasterServerLoadProtection(IWorkflowRepository workflowRepository,
                                      MasterConfig masterConfig) {
        // 调用父类构造函数，初始化基础资源保护机制
        super(masterConfig.getServerLoadProtection());
        // 保存Master特有的负载保护配置
        this.masterServerLoadProtectionConfig = masterConfig.getServerLoadProtection();
        // 保存工作流仓库引用
        this.workflowRepository = workflowRepository;
    }

    /**
     * 检查Master服务器是否过载
     * 
     * 这个方法会从多个维度检查系统是否处于过载状态：
     * 1. 基础资源检查：CPU、内存、磁盘使用率
     * 2. 业务级别检查：并发工作流实例数量
     * 
     * 简单理解：就像体检中心的检查，从多个指标判断身体健康状况。
     * 
     * @param systemMetrics 系统资源指标对象
     * @return true 如果系统过载，false 如果系统正常
     */
    @Override
    public boolean isOverload(SystemMetrics systemMetrics) {
        // 先检查是否启用了负载保护机制
        if (!masterServerLoadProtectionConfig.isEnabled()) {
            return false;  // 如果未启用，直接返回false
        }

        // 先调用父类的检查方法，检查基础资源（CPU、内存、磁盘）
        if (super.isOverload(systemMetrics)) {
            return true;   // 如果基础资源已经过载，直接返回true
        }

        // 检查工作流实例数量是否超限
        int currentWorkflowInstanceCount = workflowRepository.getAll().size();
        if (currentWorkflowInstanceCount >= masterServerLoadProtectionConfig.getMaxConcurrentWorkflowInstances()) {
            log.info(
                    "OverLoad: 当前工作流实例数量: {} 超过了最大并发数量限制 {}",
                    currentWorkflowInstanceCount, masterServerLoadProtectionConfig.getMaxConcurrentWorkflowInstances());
            return true;
        }
        return false;
    }
}
