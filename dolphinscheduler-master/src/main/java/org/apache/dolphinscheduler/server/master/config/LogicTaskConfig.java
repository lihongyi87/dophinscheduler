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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 逻辑任务配置类
 *
 * 逻辑任务是在Master节点直接执行的任务类型，不需要分发到Worker节点。
 * 这些任务通常执行速度较快，主要用于工作流控制和决策。
 *
 * 主要包括以下任务类型：
 * - 条件分支任务：根据条件判断执行不同的分支
 * - Switch任务：根据参数值选择不同的执行路径
 * - 子工作流任务：调用其他工作流实例
 * - 依赖检查任务：检查依赖条件是否满足
 *
 * 使用场景：
 * - 复杂工作流的流程控制
 * - 基于条件的动态调度
 * - 工作流嵌套调用
 *
 * @author DolphinScheduler Team
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LogicTaskConfig {

    /**
     * 逻辑任务执行器线程池大小
     *
     * 这个线程池专门用于执行在Master节点上运行的逻辑任务。
     *
     * 线程数量设计考虑：
     * - 逻辑任务通常为CPU密集型操作
     * - 执行时间相对较短，不会长时间阻塞
     * - 需要快速响应，避免影响工作流调度性能
     *
     * 默认值计算公式：CPU核数 * 2 + 1
     * - 这是针对CPU密集型任务的经典配置
     * - 适合短时间、高并发的任务处理
     *
     * 配置建议：
     * - 小型环境（4核）：建议8-16个线程
     * - 中型环境（8核）：建议16-32个线程
     * - 大型环境（16核+）：建议32-64个线程
     *
     * 注意事项：
     * - 线程数过少会导致逻辑任务排队等待
     * - 线程数过多会增加上下文切换开销
     * - 需要根据实际的逻辑任务执行频率调整
     */
    @Builder.Default
    private int taskExecutorThreadCount = Runtime.getRuntime().availableProcessors() * 2 + 1;
}
