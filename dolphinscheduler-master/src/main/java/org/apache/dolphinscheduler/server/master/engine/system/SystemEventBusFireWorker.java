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

package org.apache.dolphinscheduler.server.master.engine.system;

import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.server.master.engine.system.event.AbstractSystemEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.ISystemEventHandler;
import org.apache.dolphinscheduler.server.master.failover.FailoverCoordinator;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 系统事件总线触发工作线程
 *
 * 这是负责从SystemEventBus中消费事件并分发到相应处理器的核心工作线程。
 * 它是整个系统事件处理机制的执行引擎，确保系统事件能够被及时、正确地处理。
 *
 * 类比：企业中的专职邮件分拣员，负责从邮件中心取出邮件，
 *      根据邮件类型和地址将其分发给相应的部门进行处理。
 *
 * 核心职责：
 * 1. 持续监听SystemEventBus中的待处理事件
 * 2. 根据事件类型找到匹配的处理器
 * 3. 将事件分发给处理器执行具体逻辑
 * 4. 处理异常情况，确保系统稳定运行
 *
 * 工作机制：
 * - 采用守护线程模式，在后台持续运行
 * - 使用阻塞队列机制，高效处理事件
 * - 支持优雅停机，响应系统生命周期管理
 * - 具备错误重试能力，提高系统可靠性
 *
 * 线程安全：
 * - 作为单线程消费者，避免了并发处理的复杂性
 * - 通过服务生命周期管理确保线程安全启停
 *
 * 容错处理：
 * - 当事件处理失败时，会将事件重新放回队列
 * - 包含重试延迟机制，避免快速失败循环
 */
@Slf4j
@Component
@SuppressWarnings({"unchecked", "rawtypes"})
public class SystemEventBusFireWorker extends BaseDaemonThread implements AutoCloseable {

    /**
     * 系统事件总线
     *
     * 用于从队列中获取待处理的系统事件。
     * 这是事件消费的数据源。
     */
    @Autowired
    private SystemEventBus systemEventBus;

    /**
     * 故障转移协调器
     *
     * 负责执行具体的故障转移逻辑。
     * 虽然在这个类中没有直接使用，但通过依赖注入确保其可用性。
     */
    @Autowired
    private FailoverCoordinator failoverCoordinator;

    /**
     * 系统事件处理器列表
     *
     * 包含所有注册的系统事件处理器，通过Spring自动装配获取。
     * 每个处理器负责处理特定类型的系统事件。
     *
     * 类比：企业中各个部门的专员列表，每个专员负责处理特定类型的事务。
     */
    @Autowired
    private List<ISystemEventHandler> systemEventHandlers;

    /**
     * 工作线程运行标志
     *
     * 控制工作线程的运行状态：
     * - true: 线程继续运行，处理事件
     * - false: 线程停止运行，退出事件处理循环
     *
     * 注意：使用static确保在多实例环境下的一致性
     */
    private static boolean flag = false;

    /**
     * 构造系统事件总线触发工作线程
     *
     * 初始化守护线程，设置线程名称以便于调试和监控。
     *
     * 线程命名规范：使用类名作为线程名，便于日志追踪和问题定位。
     */
    public SystemEventBusFireWorker() {
        super("SystemEventBusFireWorker");
    }

    /**
     * 启动系统事件处理工作线程
     *
     * 初始化并启动事件处理线程，开始监听和处理系统事件。
     *
     * 执行步骤：
     * 1. 设置运行标志为true，允许线程进入处理循环
     * 2. 调用父类的start方法启动线程
     * 3. 记录启动日志，便于追踪系统状态
     *
     * 类比：打开邮件分拣中心，工作人员开始上班，准备处理邮件。
     */
    @Override
    public void start() {
        flag = true;
        super.start();
        log.info("SystemEventBusFireWorker started");
    }

    /**
     * 事件处理主循环
     *
     * 这是工作线程的核心方法，负责持续从事件总线中获取事件并处理。
     *
     * 工作流程：
     * 1. 持续检查运行标志，确保线程应该继续运行
     * 2. 从事件总线中子塞式获取下一个事件
     * 3. 检查服务器生命周期状态，响应停机指令
     * 4. 调用fireSystemEvent处理事件
     * 5. 如果处理失败，将事件重新放回队列等待重试
     *
     * 异常处理：
     * - 线程中断：保持中断标志并退出循环
     * - 事件处理失败：重新放回事件，延迟10秒后重试
     *
     * 类比：邮件分拣员的工作日常，不停地从邮箱中取出邮件，
     *      按照类型分发给相应部门，如果处理失败就重新放回邮箱。
     */
    @Override
    public void run() {
        while (flag) {
            final AbstractSystemEvent systemEvent;
            try {
                // 从事件总线中子塞式获取下一个事件
                systemEvent = systemEventBus.take();
            } catch (InterruptedException interruptedException) {
                // 线程被中断，保持中断状态并退出
                Thread.currentThread().interrupt();
                log.warn("SystemEventBusFireWorker has been interrupted", interruptedException);
                break;
            }
            // 检查服务器是否已经停止，响应优雅停机
            if (ServerLifeCycleManager.isStopped()) {
                log.info("SystemEventBusFireWorker has been stopped");
                break;
            }
            try {
                // 处理系统事件
                fireSystemEvent(systemEvent);
            } catch (Exception ex) {
                // 处理失败，将事件重新放回事件总线等待重试
                systemEventBus.publish(systemEvent);
                log.error("Fire SystemEvent: {} failed", systemEvent, ex);
                // 延迟10秒后重试，避免快速失败循环
                ThreadUtils.sleep(10_000);
            }
        }
    }

    /**
     * 触发系统事件处理
     *
     * 根据事件类型找到匹配的处理器，并执行具体的事件处理逻辑。
     * 这是事件分发和处理的核心方法。
     *
     * 处理步骤：
     * 1. 启动性能计时器，监控处理耗时
     * 2. 从所有注册的处理器中筛选出匹配的处理器
     * 3. 检查是否找到匹配的处理器，如果没有则记录错误
     * 4. 对所有匹配的处理器执行事件处理
     * 5. 停止计时器并记录处理耗时
     *
     * 类比：收到一封邮件后，根据邮件类型和内容找到对应的处理部门，
     *      如果找不到就记录错误，否则就交给相应部门处理。
     *
     * 性能监控：
     * - 使用StopWatch记录每个事件的处理耗时
     * - 日志记录便于性能分析和问题排查
     *
     * 容错设计：
     * - 如果没有匹配的处理器，不会抛出异常，而是记录错误日志
     * - 支持多个处理器处理同一事件（虽然目前每种事件只有一个处理器）
     *
     * @param systemEvent 要处理的系统事件，不能为null
     */
    private void fireSystemEvent(final AbstractSystemEvent systemEvent) {
        // 启动性能计时器，用于监控事件处理耗时
        final StopWatch stopWatch = StopWatch.createStarted();

        // 从所有注册的处理器中筛选出能够处理当前事件类型的处理器
        final List<ISystemEventHandler> matchedSystemEventHandlers = systemEventHandlers
                .stream()
                .filter(systemEventHandler -> systemEventHandler.matchState() == systemEvent.getEventType())
                .collect(Collectors.toList());

        // 检查是否找到匹配的处理器
        if (CollectionUtils.isEmpty(matchedSystemEventHandlers)) {
            log.error("No matched SystemEventHandler for SystemEvent: {}", systemEvent);
            return;
        }

        // 对所有匹配的处理器执行事件处理
        matchedSystemEventHandlers.forEach(systemEventHandler -> systemEventHandler.handle(systemEvent));

        // 停止计时器并记录处理耗时
        stopWatch.stop();
        log.info("Fire SystemEvent: {} cost: {} ms", systemEvent, stopWatch.getTime());
    }

    /**
     * 关闭系统事件处理工作线程
     *
     * 实现AutoCloseable接口，支持try-with-resources语法和优雅停机。
     * 通过设置运行标志为false，通知主循环退出。
     *
     * 停机流程：
     * 1. 设置flag为false，使主循环在下一次检查时退出
     * 2. 记录关闭日志，便于追踪系统状态
     *
     * 类比：对邮件分拣员发出下班通知，工作人员完成当前手头工作后
     *      就会停止处理新的邮件并离开工作岗位。
     *
     * 注意：
     * - 这是优雅停机，不会强制中断正在处理的事件
     * - 工作线程会在处理完当前事件后自然退出
     */
    @Override
    public void close() {
        flag = false;
        log.info("SystemEventBusFireWorker closed");
    }
}
