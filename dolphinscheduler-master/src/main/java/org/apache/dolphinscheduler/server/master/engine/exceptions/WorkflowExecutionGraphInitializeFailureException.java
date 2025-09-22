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

package org.apache.dolphinscheduler.server.master.engine.exceptions;

import org.apache.dolphinscheduler.dao.entity.TaskInstance;

/**
 * 工作流执行图初始化失败异常
 *
 * 当工作流执行图在初始化过程中遇到无法恢复的错误时抛出的异常。
 * 工作流执行图是工作流运行时的核心数据结构，包含任务之间的依赖关系和执行状态。
 *
 * 触发场景：
 * 1. 任务状态不一致：数据库中的任务状态与期望状态不匹配
 * 2. DAG结构错误：工作流定义的DAG存在环形依赖或无效节点
 * 3. 任务定义缺失：引用的任务定义在数据库中不存在
 * 4. 参数验证失败：任务参数格式错误或必需参数缺失
 * 5. 版本不兼容：工作流定义版本与当前系统不兼容
 *
 * 影响范围：
 * - 整个工作流实例无法启动
 * - 相关的命令会被移动到错误命令表
 * - 触发监控告警通知运维人员
 *
 * 处理策略：
 * - 详细记录初始化失败的原因
 * - 保留原始命令和工作流定义用于问题排查
 * - 通知相关人员进行人工介入处理
 *
 * 类比：就像工厂生产线启动前的设备检查，
 * 如果发现关键设备故障就不能开始生产。
 */
public class WorkflowExecutionGraphInitializeFailureException extends RuntimeException {

    /**
     * 构造工作流执行图初始化失败异常
     *
     * @param message 异常详细信息，说明初始化失败的具体原因
     */
    public WorkflowExecutionGraphInitializeFailureException(String message) {
        // ==========调用父类构造器==========
        // 设置异常消息，用于后续的异常处理和日志记录
        super(message);
    }

    /**
     * 创建任务状态无效的初始化失败异常
     *
     * 当发现任务实例的状态不符合初始化要求时，使用此静态方法创建异常。
     * 这是一个便利方法，用于快速创建特定类型的初始化失败异常。
     *
     * @param taskInstance 状态无效的任务实例
     * @return 包含详细错误信息的初始化失败异常
     */
    public static WorkflowExecutionGraphInitializeFailureException bootstrapTaskStateNotValid(TaskInstance taskInstance) {
        // ==========构造详细的错误消息==========
        // 包含任务名称和状态信息，便于问题定位和调试
        return new WorkflowExecutionGraphInitializeFailureException(
                "The task: " + taskInstance.getName() + " state: " + taskInstance.getState() + " is not valid");
    }
}
