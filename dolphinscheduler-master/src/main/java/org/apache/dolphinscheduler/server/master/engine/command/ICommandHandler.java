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

package org.apache.dolphinscheduler.server.master.engine.command;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.server.master.engine.command.handler.ReRunWorkflowCommandHandler;
import org.apache.dolphinscheduler.server.master.engine.command.handler.RecoverFailureTaskCommandHandler;
import org.apache.dolphinscheduler.server.master.engine.command.handler.RunWorkflowCommandHandler;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.WorkflowExecutionRunnable;

/**
 * 命令处理器接口
 *
 * 用于处理特定类型的命令，将命令转换为可执行的工作流运行实例。
 * 每个处理器负责处理一种特定类型的命令，实现命令的多态处理。
 *
 * 支持的命令类型包括：
 * - START_PROCESS：启动工作流
 * - REPEAT_RUNNING：重新运行工作流
 * - RECOVER_WAITTING_THREAD：恢复等待的任务
 * - START_FAILURE_TASK_PROCESS：恢复失败任务
 * - COMPLEMENT_DATA：补数运行
 * - SCHEDULER：定时调度运行
 * - START_CURRENT_TASK_PROCESS：启动当前任务
 *
 * 类比：就像不同类型文件的处理器，PDF处理器处理PDF文件，
 * Word处理器处理Word文件，每种处理器专门处理对应类型的文件。
 *
 * @see RunWorkflowCommandHandler 运行工作流命令处理器
 * @see ReRunWorkflowCommandHandler 重新运行工作流命令处理器
 * @see RecoverFailureTaskCommandHandler 恢复失败任务命令处理器
 */
public interface ICommandHandler {

    /**
     * 处理命令并返回工作流执行运行实例
     *
     * 将命令解析并转换为可执行的工作流运行实例，包括：
     * 1. 验证命令的有效性
     * 2. 创建或获取工作流定义
     * 3. 构建工作流执行上下文
     * 4. 初始化任务实例
     * 5. 创建工作流执行运行实例
     *
     * @param command 需要处理的命令对象，包含执行参数和上下文信息
     * @return 工作流执行运行实例，可以直接提交给执行引擎运行
     */
    WorkflowExecutionRunnable handleCommand(final Command command);

    /**
     * 获取当前处理器支持的命令类型
     *
     * 用于命令路由，系统根据命令的类型选择合适的处理器。
     * 每个处理器只能处理一种特定类型的命令。
     *
     * @return 当前处理器支持的命令类型枚举值
     */
    CommandType commandType();

}
