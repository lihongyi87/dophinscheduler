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

package org.apache.dolphinscheduler.server.worker.metrics;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import lombok.experimental.UtilityClass;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/**
 * Worker服务器指标监控工具类
 *
 * 负责收集和统计Worker节点的运行指标，包括负载状态、资源下载情况、
 * 心跳监控、任务队列状态等核心运维指标。
 * 通过Micrometer框架将指标数据暴露给Prometheus等监控系统。
 *
 * 主要监控指标类型：
 * 1. 计数器(Counter)：累积计数类指标，如心跳次数、下载成功/失败次数
 * 2. 计时器(Timer)：耗时统计指标，如资源下载耗时分布
 * 3. 分布统计(DistributionSummary)：数值分布指标，如下载文件大小分布
 * 4. 仪表盘(Gauge)：实时数值指标，如CPU使用率、内存使用率、队列大小等
 */
@UtilityClass
public class WorkerServerMetrics {

    /**
     * Worker节点过载计数器
     * 记录Worker节点因负载过高而拒绝接收新任务的次数
     * 用于监控集群负载分布和识别性能瓶颈
     */
    private final Counter workerOverloadCounter =
            Counter.builder("ds.worker.overload.count")
                    .description("overloaded workers count")
                    .register(Metrics.globalRegistry);

    /**
     * Worker任务提交队列满载计数器
     * 记录Worker节点任务提交队列达到最大容量的次数
     * 帮助识别任务积压问题和队列容量配置是否合理
     */
    private final Counter workerFullSubmitQueueCounter =
            Counter.builder("ds.worker.full.submit.queue.count")
                    .description("full worker submit queues count")
                    .register(Metrics.globalRegistry);

    /**
     * Worker资源下载成功计数器
     * 记录Worker节点成功下载任务所需资源文件的次数
     * 用于监控资源管理模块的运行状况
     */
    private final Counter workerResourceDownloadSuccessCounter =
            Counter.builder("ds.worker.resource.download.count")
                    .tag("status", "success")
                    .description("worker resource download success count")
                    .register(Metrics.globalRegistry);

    /**
     * Worker资源下载失败计数器
     * 记录Worker节点下载任务资源文件失败的次数
     * 帮助识别资源存储、网络连接等问题
     */
    private final Counter workerResourceDownloadFailCounter =
            Counter.builder("ds.worker.resource.download.count")
                    .tag("status", "fail")
                    .description("worker resource download failure count")
                    .register(Metrics.globalRegistry);

    /**
     * Worker心跳计数器
     * 记录Worker节点向注册中心发送心跳的总次数
     * 用于监控Worker节点的存活状态和注册中心连接情况
     */
    private final Counter workerHeartBeatCounter =
            Counter.builder("ds.worker.heartbeat.count")
                    .description("worker heartbeat count")
                    .register(Metrics.globalRegistry);

    /**
     * Worker资源下载耗时计时器
     * 统计Worker节点下载资源文件的耗时分布
     * 包含P50、P75、P95、P99百分位数统计，用于性能分析和SLA监控
     */
    private final Timer workerResourceDownloadDurationTimer =
            Timer.builder("ds.worker.resource.download.duration")
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .description("time cost of resource download on workers")
                    .register(Metrics.globalRegistry);

    /**
     * Worker资源下载文件大小分布统计
     * 统计Worker节点下载的资源文件大小分布情况
     * 单位：字节(bytes)，包含P50、P75、P95、P99百分位数统计
     * 帮助分析资源文件大小对下载性能的影响
     */
    private final DistributionSummary workerResourceDownloadSizeDistribution =
            DistributionSummary.builder("ds.worker.resource.download.size")
                    .baseUnit("bytes")
                    .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .description("size of downloaded resource files on worker")
                    .register(Metrics.globalRegistry);

    /**
     * 增加Worker过载计数
     * 当Worker节点因负载过高拒绝接收新任务时调用
     */
    public void incWorkerOverloadCount() {
        workerOverloadCounter.increment();
    }

    /**
     * 增加Worker任务提交队列满载计数
     * 当Worker节点的任务提交队列达到最大容量时调用
     */
    public void incWorkerSubmitQueueIsFullCount() {
        workerFullSubmitQueueCounter.increment();
    }

    /**
     * 增加Worker资源下载成功计数
     * 每次成功下载任务所需资源文件时调用
     */
    public void incWorkerResourceDownloadSuccessCount() {
        workerResourceDownloadSuccessCounter.increment();
    }

    /**
     * 增加Worker资源下载失败计数
     * 每次下载任务资源文件失败时调用
     */
    public void incWorkerResourceDownloadFailureCount() {
        workerResourceDownloadFailCounter.increment();
    }

    /**
     * 增加Worker心跳计数
     * 每次Worker节点向注册中心发送心跳时调用
     */
    public void incWorkerHeartbeatCount() {
        workerHeartBeatCounter.increment();
    }

    /**
     * 记录Worker资源下载耗时
     *
     * @param milliseconds 下载耗时，单位：毫秒
     */
    public void recordWorkerResourceDownloadTime(final long milliseconds) {
        workerResourceDownloadDurationTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * 记录Worker资源下载文件大小
     *
     * @param size 下载文件大小，单位：字节
     */
    public void recordWorkerResourceDownloadSize(final long size) {
        workerResourceDownloadSizeDistribution.record(size);
    }

    /**
     * 注册Worker任务总数仪表盘指标
     * 实时监控Worker节点当前处理的任务总数
     *
     * @param supplier 提供任务总数的数据源
     */
    public void registerWorkerTaskTotalGauge(final Supplier<Number> supplier) {
        Gauge.builder("ds.worker.task", supplier)
                .description("total number of tasks on worker")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Worker执行队列大小仪表盘指标
     * 实时监控Worker节点任务执行队列中等待执行的任务数量
     *
     * @param supplier 提供执行队列大小的数据源
     */
    public void registerWorkerExecuteQueueSizeGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.worker.execute.queue.size", supplier)
                .description("worker execute queue size")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Worker活跃执行线程数仪表盘指标
     * 实时监控Worker节点当前正在执行任务的线程数量
     *
     * @param supplier 提供活跃执行线程数的数据源
     */
    public void registerWorkerActiveExecuteThreadGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.worker.active.execute.thread", supplier)
                .description("number of active task execute threads on worker")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Worker可用内存仪表盘指标
     * 实时监控Worker节点的可用内存大小
     *
     * @param supplier 提供可用内存大小的数据源，单位通常为字节或MB
     */
    public void registerWorkerMemoryAvailableGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.worker.memory.available", supplier)
                .description("worker memory available")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Worker CPU使用率仪表盘指标
     * 实时监控Worker节点的CPU使用率
     *
     * @param supplier 提供CPU使用率的数据源，通常为0-100的百分比值
     */
    public void registerWorkerCpuUsageGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.worker.cpu.usage", supplier)
                .description("worker cpu usage")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册Worker内存使用率仪表盘指标
     * 实时监控Worker节点的内存使用率
     *
     * @param supplier 提供内存使用率的数据源，通常为0-100的百分比值
     */
    public void registerWorkerMemoryUsageGauge(Supplier<Number> supplier) {
        Gauge.builder("ds.worker.memory.usage", supplier)
                .description("worker memory usage")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册未缓存异常数量仪表盘指标
     * 实时监控Worker节点中未被缓存处理的异常数量
     * 帮助识别系统中的异常处理问题
     *
     * @param supplier 提供未缓存异常数量的数据源
     */
    public static void registerUncachedException(final Supplier<Number> supplier) {
        Gauge.builder("ds.worker.uncached.exception", supplier)
                .description("number of uncached exception")
                .register(Metrics.globalRegistry);
    }

}
