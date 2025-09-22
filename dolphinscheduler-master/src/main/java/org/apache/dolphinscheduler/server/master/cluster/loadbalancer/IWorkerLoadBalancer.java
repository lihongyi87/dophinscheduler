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

package org.apache.dolphinscheduler.server.master.cluster.loadbalancer;

import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;

import java.util.Optional;

import lombok.NonNull;

/**
 * Worker负载均衡器接口
 *
 * 这是DolphinScheduler任务调度的核心接口，负责从Worker集群中选择合适的Worker节点执行任务。
 * 不同的负载均衡算法可以实现这个接口，提供不同的负载分配策略。
 * 类比：工厂的任务分配员，根据不同的策略将生产任务分配给合适的工人。
 *
 * 主要职责：
 * 1. Worker选择：根据负载均衡算法从指定工作组中选择Worker
 * 2. 负载分散：确保任务能够合理分布到不同的Worker节点
 * 3. 可用性保证：只选择状态正常、有处理能力的Worker
 * 4. 性能优化：根据Worker的负载情况和处理能力进行智能选择
 *
 * 支持的负载均衡策略：
 * - 随机选择：随机从可用Worker中选择
 * - 轮询选择：依次轮流选择Worker
 * - 加权轮询：根据Worker权重进行轮询
 * - 动态加权：根据Worker实时负载动态调整权重
 */
public interface IWorkerLoadBalancer {

    /**
     * 从指定工作组中选择一个Worker地址
     *
     * 这是负载均衡器的核心方法，根据具体的负载均衡算法从给定工作组中
     * 选择一个合适的Worker节点来执行任务。
     *
     * 选择条件：
     * 1. Worker必须属于指定的工作组
     * 2. Worker状态必须是正常（NORMAL）
     * 3. Worker必须有处理任务的能力
     * 4. 根据具体算法进行最优选择
     *
     * 类比：从指定技能组的工人中选择一个来承担新的生产任务。
     *
     * @param workerGroup 工作组名称，不能为null
     * @return 选中的Worker地址的Optional对象，如果没有可用Worker则返回空
     */
    Optional<String> select(@NonNull String workerGroup);

    /**
     * 获取负载均衡器类型
     *
     * 返回当前负载均衡器的类型标识，用于：
     * 1. 配置验证：确认当前使用的负载均衡策略
     * 2. 监控统计：记录不同策略的使用情况
     * 3. 动态切换：支持运行时切换负载均衡策略
     *
     * @return 负载均衡器类型枚举
     */
    WorkerLoadBalancerType getType();

}
