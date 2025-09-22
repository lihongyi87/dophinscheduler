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

package org.apache.dolphinscheduler.server.master.metrics;

import java.util.function.Supplier;

import lombok.experimental.UtilityClass;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;

/**
 * Master服务器监控指标工具类
 * <p>
 * 该类负责收集和管理Master服务器相关的各种监控指标，包括：
 * - Master服务器负载监控（过载计数）
 * - 工作流命令消费监控
 * - 心跳监控
 * - 系统资源监控（内存、CPU使用率）
 * - 异常监控（未缓存异常数量）
 * <p>
 * 使用Micrometer框架进行指标收集，支持与Prometheus等监控系统集成
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
@UtilityClass
public class MasterServerMetrics {

    /**
     * Master服务器过载计数器
     * <p>
     * 用于测量Master服务器过载的次数，当服务器负载过高时会增加此计数
     * 指标名称: ds.master.overload.count
     * 用途: 监控服务器负载状况、触发告警或扩容策略
     */
    private final Counter masterOverloadCounter =
            Counter.builder("ds.master.overload.count")
                    .description("Master server overload count")
                    .register(Metrics.globalRegistry);

    /**
     * Master消费工作流命令计数器
     * <p>
     * 用于测量Master服务器消费的工作流命令数量
     * 指标名称: ds.master.consume.command.count
     * 用途: 监控命令处理吞吐量、分析系统处理能力
     */
    private final Counter masterConsumeCommandCounter =
            Counter.builder("ds.master.consume.command.count")
                    .description("Master server consume command count")
                    .register(Metrics.globalRegistry);

    /**
     * Master心跳计数器
     * <p>
     * 统计Master服务器心跳次数，用于监控服务器活跃状态
     * 指标名称: ds.master.heartbeat.count
     * 用途: 监控服务器连接状态、检测网络问题
     */
    private final Counter masterHeartBeatCounter =
            Counter.builder("ds.master.heartbeat.count")
                    .description("master heartbeat count")
                    .register(Metrics.globalRegistry);

    /**
     * 注册Master内存可用量监控
     * <p>
     * 注册一个Gauge来实时监控Master服务器的可用内存量
     * 指标名称: ds.master.memory.available
     * 用途: 监控内存使用情况、预防内存不足、优化内存分配
     *
     * @param supplier 提供当前可用内存量的函数
     */
    public void registerMasterMemoryAvailableGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.master.memory.available", supplier)
                .description("Master memory available")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Master CPU使用率监控
     * <p>
     * 注册一个Gauge来实时监控Master服务器的CPU使用率
     * 指标名称: ds.master.cpu.usage
     * 用途: 监控CPU负载、性能调优、资源规划
     *
     * @param supplier 提供当前CPU使用率的函数
     */
    public void registerMasterCpuUsageGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.master.cpu.usage", supplier)
                .description("master cpu usage")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Master内存使用量监控
     * <p>
     * 注册一个Gauge来实时监控Master服务器的内存使用量
     * 指标名称: ds.master.memory.usage
     * 用途: 监控内存消耗、检测内存泄漏、优化内存使用
     *
     * @param supplier 提供当前内存使用量的函数
     */
    public void registerMasterMemoryUsageGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.master.memory.usage", supplier)
                .description("Master memory usage")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册未缓存异常数量监控
     * <p>
     * 注册一个Gauge来监控未被缓存的异常数量
     * 指标名称: ds.master.uncached.exception
     * 用途: 监控异常处理情况、分析系统稳定性
     *
     * @param supplier 提供当前未缓存异常数量的函数
     */
    public static void registerUncachedException(final Supplier<Number> supplier) {
        Gauge.builder("ds.master.uncached.exception", supplier)
                .description("number of uncached exception")
                .register(Metrics.globalRegistry);
    }

    /**
     * 增加Master过载计数
     * <p>
     * 当Master服务器检测到过载情况时调用此方法增加过载计数
     * 用于监控和告警系统判断服务器负载状况
     */
    public void incMasterOverload() {
        masterOverloadCounter.increment();
    }

    /**
     * 增加Master消费命令计数
     * <p>
     * 当Master服务器消费工作流命令时调用此方法增加消费计数
     *
     * @param commandCount 消费的命令数量
     */
    public void incMasterConsumeCommand(int commandCount) {
        masterConsumeCommandCounter.increment(commandCount);
    }

    /**
     * 增加Master心跳计数
     * <p>
     * 当Master服务器发送心跳时调用此方法增加心跳计数
     * 用于监控服务器活跃状态和网络连接情况
     */
    public void incMasterHeartbeatCount() {
        masterHeartBeatCounter.increment();
    }
}
