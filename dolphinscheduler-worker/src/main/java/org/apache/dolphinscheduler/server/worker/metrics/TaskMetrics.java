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

import org.apache.dolphinscheduler.plugin.task.api.TaskChannelFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import lombok.experimental.UtilityClass;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

/**
 * 任务指标监控工具类
 *
 * 负责收集和统计Worker节点上各种类型任务的执行情况指标，
 * 通过Micrometer框架将指标数据暴露给监控系统。
 * 主要监控任务类型维度的执行计数，便于分析不同任务类型的使用情况和性能表现。
 */
@UtilityClass
public class TaskMetrics {

    /**
     * 按任务类型分组的任务执行计数器映射
     * Key: 任务类型名称，Value: 对应的Counter指标
     * 用于统计各种已知任务类型的执行次数
     */
    private final Map<String, Counter> taskTypeExecutionCounter = new HashMap<>();

    /**
     * 未知任务类型执行计数器
     * 用于统计系统中未识别或不支持的任务类型的执行次数
     * 帮助识别配置错误或缺失的任务类型插件
     */
    private final Counter taskUnknownTypeExecutionCounter =
            Counter.builder("ds.task.execution.count.by.type")
                    .tag("task_type", "unknown")
                    .description("task execution counter by type")
                    .register(Metrics.globalRegistry);

    /**
     * 静态初始化块：动态注册所有可用的任务类型计数器
     * 通过ServiceLoader机制加载所有TaskChannelFactory实现，
     * 为每种任务类型创建独立的执行计数器
     */
    static {
        for (TaskChannelFactory taskChannelFactory : ServiceLoader.load(TaskChannelFactory.class)) {
            taskTypeExecutionCounter.put(
                    taskChannelFactory.getName(),
                    Counter.builder("ds.task.execution.count.by.type")
                            .tag("task_type", taskChannelFactory.getName())
                            .description("task execution counter by type")
                            .register(Metrics.globalRegistry));
        }
    }

    /**
     * 增加指定任务类型的执行计数
     *
     * @param taskType 任务类型名称（如：SHELL、SQL、PYTHON等）
     *                如果任务类型未知或未注册，则统计到unknown类型计数器中
     */
    public void incrTaskTypeExecuteCount(String taskType) {
        taskTypeExecutionCounter.getOrDefault(taskType, taskUnknownTypeExecutionCounter).increment();
    }

}
