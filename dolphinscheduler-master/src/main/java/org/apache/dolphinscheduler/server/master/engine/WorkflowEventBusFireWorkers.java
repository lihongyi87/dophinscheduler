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

package org.apache.dolphinscheduler.server.master.engine;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

/**
 * 工作流事件总线处理工作者集合
 * 
 * 管理多个工作流事件处理工作者的集合类，实现事件处理的并行化和负载均衡。
 * 类比：就像一个呼叫中心的座席管理系统，管理多个客服人员并行处理客户来电。
 * 
 * 核心职责：
 * 1. 工作者管理：创建和管理多个事件处理工作者实例
 * 2. 线程池管理：使用定时线程池为每个工作者分配独立的执行线程
 * 3. 事件处理器注册：为所有工作者注册相同的事件处理器集合
 * 4. 负载分配：提供工作者选择机制，实现事件的负载均衡分配
 * 5. 生命周期管理：统一管理所有工作者和线程池的启动关闭
 * 
 * 设计特点：
 * - 并行处理：多个工作者并行处理不同工作流的事件，提高吞吐量
 * - 负载均衡：通过工作者槽位分配实现事件的均匀分布
 * - 定时触发：使用固定间隔的定时调度确保事件及时处理
 * - 资源隔离：每个工作者独立处理，避免相互影响
 * - 可配置性：工作者数量可通过配置文件调整
 * 
 * 工作机制：
 * - 启动阶段：根据配置创建指定数量的工作者和对应的调度线程
 * - 运行阶段：每个工作者按固定间隔触发事件处理逻辑
 * - 分配阶段：根据工作流ID的哈希值选择对应的工作者处理
 * - 关闭阶段：优雅关闭所有线程和资源
 * 
 * 性能优化：
 * - 并发处理：多线程并行处理提高事件处理效率
 * - 内存管理：工作者数组复用，避免频繁对象创建
 * - 调度优化：使用ScheduledExecutorService提供精确的定时调度
 * 
 * 类比理解：
 * 就像一个大型快递分拣中心的工人管理系统：
 * - 管理多个分拣工人（工作者）
 * - 每个工人负责处理特定区域的包裹（工作流事件）
 * - 定期触发工人进行分拣工作（定时事件处理）
 * - 根据包裹地址分配给对应工人（负载均衡）
 * - 统一管理工人的上下班（生命周期管理）
 */
@Slf4j
@Component
public class WorkflowEventBusFireWorkers implements AutoCloseable {

    /**
     * 生命周期事件处理器列表
     * 
     * Spring自动注入的所有生命周期事件处理器实现类。
     * 这些处理器会被注册到每个工作者中，用于处理不同类型的生命周期事件。
     * 类比：呼叫中心的业务处理手册，每个客服都需要掌握的标准服务流程。
     */
    @Autowired
    private List<ILifecycleEventHandler> eventHandlers;

    /**
     * Master配置信息
     * 
     * 包含Master节点的各种配置参数，其中workflowEventBusFireThreadCount
     * 决定了创建多少个工作者线程来处理工作流事件。
     * 类比：呼叫中心的配置手册，规定了需要配置多少个座席。
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * 默认事件触发间隔
     * 
     * 每个工作者触发事件处理的时间间隔，单位为毫秒。
     * 设置为100ms是在响应速度和CPU占用之间的平衡选择。
     * 类比：客服查看新来电的时间间隔，太频繁浪费资源，太慢影响响应。
     */
    private static final long DEFAULT_FIRE_INTERVAL = 100;

    /**
     * 工作流事件处理工作者数组
     * 
     * 存储所有创建的工作者实例，数组大小由配置文件中的线程数量决定。
     * 每个工作者负责处理分配给它的工作流事件，实现并行处理。
     * 类比：呼叫中心的座席数组，每个座席独立处理分配给它的客户电话。
     */
    private WorkflowEventBusFireWorker[] workflowEventBusFireWorkers;

    /**
     * 定时执行器服务
     * 
     * 为每个工作者提供定时调度服务，确保工作者能够按固定间隔执行事件处理逻辑。
     * 使用ScheduledExecutorService提供精确的定时调度能力。
     * 类比：呼叫中心的排班系统，确保每个客服都能按时工作。
     */
    private ScheduledExecutorService workflowEventBusFireThreadPool;

    /**
     * 启动工作流事件处理工作者集合
     * 
     * 初始化并启动所有的事件处理工作者，为每个工作者分配独立的执行线程，
     * 开始定时处理工作流事件的循环执行。
     * 
     * 启动流程：
     * 1. 获取配置：从Master配置中获取工作者线程数量
     * 2. 创建线程池：创建定时调度线程池，线程数等于工作者数量
     * 3. 初始化工作者数组：创建指定数量的工作者实例数组
     * 4. 创建工作者：为每个槽位创建工作者实例并注册事件处理器
     * 5. 启动定时调度：为每个工作者安排定时执行任务
     * 
     * 线程命名规则：
     * - 使用"ds-workflow-eventbus-worker-%d"模式命名线程
     * - 所有线程都是守护线程，不会阻止JVM正常退出
     * 
     * 调度策略：
     * - 使用scheduleWithFixedDelay确保固定间隔执行
     * - 初始延迟和执行间隔都是DEFAULT_FIRE_INTERVAL(100ms)
     * - TODO: 未来可能改为基于等待-通知机制的动态间隔调度
     * 
     * 类比：就像启动呼叫中心系统，为每个座席分配电话线路，
     * 启动定时任务让客服定期查看是否有新来电需要处理。
     */
    public void start() {
        // ==========第一步：获取工作者数量配置==========
        // 从Master配置中读取工作流事件总线的工作者线程数量
        // 这个数量决定了系统的并行处理能力，通常设置为CPU核数的2-4倍
        final int workflowEventBusFireThreadCount = masterConfig.getWorkflowEventBusFireThreadCount();

        // ==========第二步：创建定时调度线程池==========
        // 创建专门用于工作流事件处理的定时线程池
        // 线程池大小等于工作者数量，每个工作者对应一个调度线程
        // 使用守护线程工厂，确保不会阻止JVM正常退出
        workflowEventBusFireThreadPool = Executors.newScheduledThreadPool(
                workflowEventBusFireThreadCount, // 线程池大小
                ThreadUtils.newDaemonThreadFactory("ds-workflow-eventbus-worker-%d")); // 守护线程工厂

        // ==========第三步：初始化工作者数组==========
        // 创建工作者数组，数组大小等于配置的线程数量
        // 每个数组元素将存储一个独立的事件处理工作者
        workflowEventBusFireWorkers = new WorkflowEventBusFireWorker[workflowEventBusFireThreadCount];

        // ==========第四步：循环创建和配置工作者==========
        for (int i = 0; i < workflowEventBusFireThreadCount; i++) {
            // ----------创建单个工作者实例----------
            // 为当前槽位创建新的事件处理工作者
            final WorkflowEventBusFireWorker workflowEventBusFireWorker = new WorkflowEventBusFireWorker();

            // ----------注册事件处理器----------
            // 将所有类型的事件处理器注册到当前工作者
            // 这确保每个工作者都能处理所有类型的生命周期事件
            // 包括：任务启动、运行、成功、失败、工作流完成等事件
            eventHandlers.forEach(workflowEventBusFireWorker::registerEventHandler);

            // ----------将工作者存入数组----------
            // 将配置好的工作者放入对应的数组槽位
            // 后续可以通过数组索引快速定位特定的工作者
            workflowEventBusFireWorkers[i] = workflowEventBusFireWorker;

            // ----------安排定时调度任务----------
            // 为当前工作者安排定时执行的事件处理任务
            // 使用scheduleWithFixedDelay确保固定间隔执行
            workflowEventBusFireThreadPool.scheduleWithFixedDelay(
                    workflowEventBusFireWorker::fireAllRegisteredEvent, // 要执行的方法
                    DEFAULT_FIRE_INTERVAL, // 初始延迟时间（100ms）
                    // TODO: 未来优化点 - 不为所有工作者使用固定间隔
                    // 每个工作者应该使用等待-通知机制控制触发间隔
                    // 这样可以在有事件时立即处理，无事件时减少CPU消耗
                    DEFAULT_FIRE_INTERVAL, // 执行间隔时间（100ms）
                    TimeUnit.MILLISECONDS); // 时间单位
        }

        // ==========启动完成==========
        // 记录启动成功日志，包含工作者数量信息
        // 此时所有工作者都已创建并开始定时执行事件处理任务
        log.info("WorkflowEventBusFireWorkers started, worker size: {}", workflowEventBusFireThreadCount);
    }

    /**
     * 获取指定槽位的工作者
     * 
     * 根据工作者槽位号获取对应的事件处理工作者实例。
     * 这是负载均衡的关键方法，通过槽位分配将工作流事件分配给特定的工作者。
     * 
     * @param workerSlot 工作者槽位号，通常通过工作流ID哈希计算得出
     * @return 对应槽位的工作者实例
     * 
     * 类比：根据座席号找到对应的客服人员来处理特定的客户电话。
     */
    public WorkflowEventBusFireWorker getWorker(Integer workerSlot) {
        return workflowEventBusFireWorkers[workerSlot];
    }

    /**
     * 获取所有工作者数组（测试用）
     * 
     * 返回工作者数组的引用，主要用于单元测试中验证工作者的创建和配置。
     * 在生产环境中不建议直接访问工作者数组。
     * 
     * @return 工作者数组
     */
    @VisibleForTesting
    public WorkflowEventBusFireWorker[] getWorkers() {
        return workflowEventBusFireWorkers;
    }

    /**
     * 获取工作者数量
     * 
     * 返回当前配置的工作者数量，这个数量决定了系统的并行处理能力。
     * 数量来源于Master配置文件中的workflowEventBusFireThreadCount参数。
     * 
     * @return 工作者数量
     * 
     * 类比：查询呼叫中心当前配置了多少个座席。
     */
    public int getWorkerSize() {
        return masterConfig.getWorkflowEventBusFireThreadCount();
    }

    /**
     * 关闭工作流事件处理工作者集合
     * 
     * 优雅关闭所有工作者和相关资源，包括关闭线程池和清理资源。
     * 这是系统关闭时必须执行的清理操作，确保资源正确释放。
     * 
     * 关闭流程：
     * 1. 检查线程池：如果线程池不为null，执行关闭操作
     * 2. 关闭线程池：调用shutdown()方法停止接收新任务并关闭现有任务
     * 3. 记录日志：记录关闭完成的日志信息
     * 
     * 注意事项：
     * - shutdown()会等待已提交的任务完成，是一种优雅关闭
     * - 不会立即强制中断正在执行的任务
     * - 确保系统关闭时不会丢失正在处理的事件
     * 
     * @throws Exception 关闭过程中可能抛出的异常
     * 
     * 类比：呼叫中心下班时，等待所有客服处理完当前通话后统一下线。
     */
    @Override
    public void close() throws Exception {
        // ==========检查线程池是否存在==========
        // 防止在未初始化或已关闭的情况下调用shutdown方法
        // 这是一种防御性编程的做法，确保关闭操作的安全性
        if (workflowEventBusFireThreadPool != null) {
            // ==========执行线程池优雅关闭==========
            // 调用shutdown()方法优雅关闭线程池：
            // 1. 停止接收新的调度任务
            // 2. 等待已提交的任务完成执行
            // 3. 释放线程池相关资源
            // 注意：这不会强制中断正在执行的任务，确保事件处理完整性
            workflowEventBusFireThreadPool.shutdown();
        }

        // ==========记录关闭完成日志==========
        // 记录工作者集合关闭完成的日志信息
        // 便于运维人员了解系统组件的关闭状态
        log.info("WorkflowEventBusFireWorkers closed");
    }
}
