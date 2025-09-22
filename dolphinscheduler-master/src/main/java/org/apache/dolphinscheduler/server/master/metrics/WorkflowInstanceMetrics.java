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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableSet;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/**
 * 工作流实例监控指标工具类
 * <p>
 * 该类负责收集和管理工作流实例相关的各种监控指标，包括：
 * - 工作流实例状态统计计数器
 * - 命令查询耗时监控
 * - 工作流实例生成耗时监控
 * - 运行中工作流实例数量监控
 * - 需要重新提交的工作流实例数量监控
 * <p>
 * 使用Micrometer框架进行指标收集，支持与Prometheus等监控系统集成
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
@UtilityClass
@Slf4j
public class WorkflowInstanceMetrics {

    /**
     * 工作流实例状态集合
     * <p>
     * 定义所有可能的工作流实例状态：
     * - submit: 已提交状态
     * - timeout: 超时状态
     * - finish: 完成状态
     * - failover: 故障转移状态
     * - success: 成功状态
     * - fail: 失败状态
     * - stop: 停止状态
     */
    private final Set<String> workflowInstanceStates = ImmutableSet.of(
            "submit", "timeout", "finish", "failover", "success", "fail", "stop");

    /**
     * 静态初始化块
     * <p>
     * 为每个工作流实例状态预先注册计数器，避免运行时创建的开销
     * 使用dummy标签作为占位符，实际使用时会被具体的工作流定义代码替换
     */
    static {
        for (final String state : workflowInstanceStates) {
            Counter.builder("ds.workflow.instance.count")
                    .tags("state", state, "workflow.definition.code", "dummy")
                    .description(String.format("workflow instance total count by state and definition code"))
                    .register(Metrics.globalRegistry);
        }

    }

    /**
     * 命令查询耗时计时器
     * <p>
     * 监控从数据库查询工作流命令的耗时情况，用于分析命令调度的性能瓶颈
     * 指标名称: ds.workflow.command.query.duration
     * 用途: 性能监控、数据库查询优化参考
     */
    private final Timer commandQueryTimer =
            Timer.builder("ds.workflow.command.query.duration")
                    .description("Command query duration")
                    .register(Metrics.globalRegistry);

    /**
     * 工作流实例生成耗时计时器
     * <p>
     * 监控从命令生成工作流实例的耗时情况，包括实例创建、初始化等操作
     * 指标名称: ds.workflow.instance.generate.duration
     * 用途: 性能监控、实例生成流程优化参考
     */
    private final Timer workflowInstanceGenerateTimer =
            Timer.builder("ds.workflow.instance.generate.duration")
                    .description("workflow instance generated duration")
                    .register(Metrics.globalRegistry);

    /**
     * 记录命令查询耗时
     * <p>
     * 将命令查询的耗时记录到计时器中，用于分析数据库查询性能
     *
     * @param milliseconds 查询耗时（毫秒）
     */
    public void recordCommandQueryTime(long milliseconds) {
        commandQueryTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * 记录工作流实例生成耗时
     * <p>
     * 将工作流实例生成的耗时记录到计时器中，用于分析实例创建性能
     *
     * @param milliseconds 生成耗时（毫秒）
     */
    public void recordWorkflowInstanceGenerateTime(long milliseconds) {
        workflowInstanceGenerateTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * 注册运行中工作流实例数量监控
     * <p>
     * 注册一个Gauge来实时监控当前运行中的工作流实例数量
     * 指标名称: ds.workflow.instance.running
     * 用途: 系统负载监控、资源使用情况分析
     *
     * @param function 提供当前运行中实例数量的函数
     */
    public synchronized void registerWorkflowInstanceRunningGauge(Supplier<Number> function) {
        Gauge.builder("ds.workflow.instance.running", function)
                .description("The current running workflow instance count")
                .register(Metrics.globalRegistry);
    }

    /**
     * 注册需要重新提交的工作流实例数量监控
     * <p>
     * 注册一个Gauge来实时监控当前需要重新提交的工作流实例数量
     * 指标名称: ds.workflow.instance.resubmit
     * 用途: 故障监控、系统健康状况分析
     *
     * @param function 提供当前需要重新提交实例数量的函数
     */
    public synchronized void registerWorkflowInstanceResubmitGauge(Supplier<Number> function) {
        Gauge.builder("ds.workflow.instance.resubmit", function)
                .description("The current workflow instance need to resubmit count")
                .register(Metrics.globalRegistry);
    }

    /**
     * 根据状态和工作流定义代码增加工作流实例计数
     * <p>
     * 根据工作流实例的状态和对应的工作流定义代码增加相应的计数器
     * 指标名称: ds.workflow.instance.count
     * 标签: state（状态）, workflow.definition.code（工作流定义代码）
     * 用途: 统计不同工作流定义在各个状态下的实例数量
     *
     * @param state 工作流实例状态（submit/timeout/finish/failover/success/fail/stop）
     * @param workflowDefinitionCode 工作流定义代码
     */
    public void incWorkflowInstanceByStateAndWorkflowDefinitionCode(final String state,
                                                                    final String workflowDefinitionCode) {
        // 当标签需要从本地上下文确定时，只能在方法体内构造或查找Meter
        // 查找成本只是单次哈希查找，对于大多数用例来说是可以接受的
        Metrics.globalRegistry.counter(
                "ds.workflow.instance.count",
                "state", state,
                "workflow.definition.code", workflowDefinitionCode)
                .increment();
    }

    /**
     * 清理指定工作流定义代码的所有实例计数指标
     * <p>
     * 当工作流定义被删除时，清理相关的所有状态计数器，避免指标累积过多
     * 遍历所有可能的状态，移除对应的计数器
     *
     * @param workflowDefinitionCode 要清理的工作流定义代码
     */
    public void cleanUpWorkflowInstanceCountMetricsByDefinitionCode(final Long workflowDefinitionCode) {
        for (final String state : workflowInstanceStates) {
            final Counter counter = Metrics.globalRegistry.counter(
                    "ds.workflow.instance.count",
                    "state", state,
                    "workflow.definition.code", String.valueOf(workflowDefinitionCode));
            Metrics.globalRegistry.remove(counter);
        }
    }

}
