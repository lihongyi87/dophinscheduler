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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import lombok.experimental.UtilityClass;

import com.google.common.collect.ImmutableSet;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;

/**
 * 任务监控指标工具类
 * <p>
 * 该类负责收集和管理任务相关的各种监控指标，包括：
 * - 任务实例状态统计计数器
 * - 任务分发相关指标（成功、失败、错误）
 * - 准备中任务数量监控
 * <p>
 * 使用Micrometer框架进行指标收集，支持与Prometheus等监控系统集成
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
@UtilityClass
public class TaskMetrics {

    /**
     * 任务实例计数器映射
     * <p>
     * 存储每个状态对应的计数器实例，便于快速访问和操作
     */
    private final Map<String, Counter> taskInstanceCounters = new HashMap<>();

    /**
     * 任务实例状态集合
     * <p>
     * 定义所有可能的任务实例状态：
     * - submit: 已提交状态
     * - timeout: 超时状态
     * - finish: 完成状态
     * - failover: 故障转移状态
     * - retry: 重试状态
     * - dispatch: 已分发状态
     * - success: 成功状态
     * - kill: 已杀死状态
     * - fail: 失败状态
     * - stop: 停止状态
     */
    private final Set<String> taskInstanceStates = ImmutableSet.of(
            "submit", "timeout", "finish", "failover", "retry", "dispatch", "success", "kill", "fail", "stop");

    /**
     * 静态初始化块
     * <p>
     * 为每个任务实例状态预先创建计数器并注册到全局注册表中
     * 提供按状态统计任务实例数量的能力
     */
    static {
        for (final String state : taskInstanceStates) {
            taskInstanceCounters.put(
                    state,
                    Counter.builder("ds.task.instance.count")
                            .tags("state", state)
                            .description(String.format("Workflow instance %s total count", state))
                            .register(Metrics.globalRegistry));
        }

    }

    /**
     * 任务分发计数器
     * <p>
     * 统计成功分发给Worker节点的任务总数
     * 指标名称: ds.task.dispatch.count
     * 用途: 监控任务分发成功率、系统吞吐量
     */
    private final Counter taskDispatchCounter =
            Counter.builder("ds.task.dispatch.count")
                    .description("Task dispatch count")
                    .register(Metrics.globalRegistry);

    /**
     * 任务分发失败计数器
     * <p>
     * 统计任务分发失败的总数，包括重试的失败次数
     * 指标名称: ds.task.dispatch.failure.count
     * 用途: 监控任务分发失败率、系统健康状况
     */
    private final Counter taskDispatchFailCounter =
            Counter.builder("ds.task.dispatch.failure.count")
                    .description("Task dispatch failures count, retried ones included")
                    .register(Metrics.globalRegistry);

    /**
     * 任务分发错误计数器
     * <p>
     * 统计任务分发过程中发生的错误次数
     * 指标名称: ds.task.dispatch.error.count
     * 用途: 监控分发组件异常情况、系统稳定性
     */
    private final Counter taskDispatchErrorCounter =
            Counter.builder("ds.task.dispatch.error.count")
                    .description("Number of errors during task dispatch")
                    .register(Metrics.globalRegistry);

    /**
     * 注册准备中任务数量监控
     * <p>
     * 注册一个Gauge来实时监控当前准备中的任务数量
     * 指标名称: ds.task.prepared
     * 用途: 监控任务队列状况、系统处理能力
     *
     * @param consumer 提供当前准备中任务数量的函数
     */
    public synchronized void registerTaskPrepared(Supplier<Number> consumer) {
        Gauge.builder("ds.task.prepared", consumer)
                .description("Task prepared count")
                .register(Metrics.globalRegistry);
    }

    /**
     * 增加任务分发失败计数
     * <p>
     * 当任务分发失败时调用此方法增加失败计数
     *
     * @param failedCount 失败的任务数量
     */
    public void incTaskDispatchFailed(int failedCount) {
        taskDispatchFailCounter.increment(failedCount);
    }

    /**
     * 增加任务分发错误计数
     * <p>
     * 当任务分发过程中发生异常时调用此方法
     */
    public void incTaskDispatchError() {
        taskDispatchErrorCounter.increment();
    }

    /**
     * 增加任务分发计数
     * <p>
     * 当成功分发任务时调用此方法增加分发计数
     */
    public void incTaskDispatch() {
        taskDispatchCounter.increment();
    }

    /**
     * 根据状态增加任务实例计数
     * <p>
     * 根据任务实例的状态增加相应的计数器
     *
     * @param state 任务实例状态
     */
    public void incTaskInstanceByState(final String state) {
        if (taskInstanceCounters.get(state) == null) {
            return;
        }
        taskInstanceCounters.get(state).increment();
    }

}
