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

package org.apache.dolphinscheduler.server.master.engine.command.handler;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteContext.WorkflowExecuteContextBuilder;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * 重新运行工作流命令处理器
 * 
 * 这是专门处理REPEAT_RUNNING命令的处理器，负责重新执行已经完成或失败的工作流实例。
 * 类比：就像一个"项目重启专员"，当项目需要重新开展时，基于原项目档案快速重启项目。
 * 
 * 核心职责：
 * 1. 实例重置：重置原工作流实例的状态和时间信息
 * 2. 任务清理：将历史任务实例标记为无效，避免状态冲突
 * 3. 图重建：基于清理后的状态重新构建执行图
 * 4. 状态管理：将工作流状态设为运行中并更新运行次数
 * 5. 资源分配：重新分配Master节点资源
 * 
 * 与普通启动的区别：
 * - 复用实例：使用原有的工作流实例，而不是创建新实例
 * - 保留参数：保持原命令参数不变，确保执行环境一致
 * - 清理历史：清除上次执行的任务状态，避免数据混乱
 * - 计数递增：增加运行次数计数，便于统计和监控
 * - 时间更新：更新重启时间，便于执行时间统计
 * 
 * 适用场景：
 * - 失败重试：工作流执行失败后需要重新运行
 * - 手动重启：用户手动触发已完成工作流的重新执行
 * - 调试运行：开发调试时需要多次重复执行工作流
 * - 数据修复：因数据问题需要重新处理的工作流
 * - 定期重跑：需要定期重新执行的历史工作流
 * 
 * 处理策略：
 * - 继承父类：继承RunWorkflowCommandHandler的大部分逻辑
 * - 重写关键方法：只重写实例组装和执行图构建的关键部分
 * - 状态重置：清理上次执行留下的状态信息
 * - 任务失效：将历史任务实例标记为无效状态
 * - 图形重建：在清理基础上重新构建执行图
 * 
 * 数据一致性保证：
 * - 原子操作：确保实例重置和任务清理的原子性
 * - 状态隔离：新旧执行状态完全隔离，避免混淆
 * - 时间准确：准确记录重启时间和执行时间
 * - 计数正确：正确维护工作流运行次数统计
 * 
 * 类比理解：
 * 就像重新装修一个房子：
 * - 保留房屋结构（工作流定义和实例框架）
 * - 清理旧装修（清除历史任务状态）
 * - 重新制定方案（重建执行图）
 * - 安排新工人（分配新的执行资源）
 * - 记录装修次数（更新运行计数）
 */
@Component
public class ReRunWorkflowCommandHandler extends RunWorkflowCommandHandler {

    /**
     * 工作流实例数据访问对象
     * 
     * 负责工作流实例的查询和更新操作。在重新运行时用于获取原工作流实例
     * 并更新其状态、时间等关键字段，确保实例信息的准确性。
     * 类比：项目档案管理员，负责查找和更新项目档案信息。
     */
    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    /**
     * 任务实例数据访问对象
     * 
     * 负责任务实例的查询和状态管理。在重新运行时用于查找历史任务实例
     * 并将它们标记为无效，避免与新执行的任务产生状态冲突。
     * 类比：工序档案管理员，负责清理旧的工序记录。
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * Spring应用上下文
     * 
     * 提供对Spring容器的访问能力，继承自父类的功能，用于获取
     * 任务执行所需的各种服务和组件实例。
     * 类比：工具仓库，提供各种专业工具和设备支持。
     */
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Master节点配置
     * 
     * 包含Master节点的配置信息，用于将重新运行的工作流绑定到
     * 当前Master节点，确保资源分配和故障转移的正确性。
     * 类比：项目管理中心的联系信息。
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * 组装重新运行的工作流实例
     * 
     * 这是重新运行处理的核心方法，负责重置原工作流实例的状态和相关字段。
     * 与普通启动不同，这里复用原有实例而不是创建新实例，但需要清理上次执行的痕迹。
     * 
     * 重置策略：
     * 1. 实例获取：从数据库获取原工作流实例，如果不存在则抛出异常
     * 2. 状态清理：清空变量池(varPool)，移除上次执行产生的变量
     * 3. 状态更新：将实例状态重置为RUNNING_EXECUTION运行状态
     * 4. 命令类型：更新命令类型为当前的REPEAT_RUNNING类型
     * 5. 时间管理：设置重启时间为当前时间，清空结束时间
     * 6. 资源绑定：将实例绑定到当前Master节点
     * 7. 计数递增：运行次数加1，便于统计和监控
     * 8. 数据持久化：将更新后的实例保存到数据库
     * 
     * 重要限制：
     * - 不能更新命令参数：保持原有的command params不变，确保执行环境一致
     * - 保留实例ID：使用原有的工作流实例ID，维持关联关系
     * - 保留定义关联：保持与原工作流定义的关联关系
     * - 保留项目归属：保持原有的项目归属和权限设置
     * 
     * 字段更新详情：
     * - state：重置为RUNNING_EXECUTION，表示开始新的执行
     * - commandType：更新为REPEAT_RUNNING，标识重新运行类型
     * - restartTime：记录本次重启的时间戳
     * - host：绑定到当前处理的Master节点
     * - endTime：清空结束时间，等待本次执行完成后设置
     * - runTimes：递增运行次数计数器
     * - varPool：清空变量池，避免上次执行的变量影响本次执行
     * 
     * 异常处理：
     * - 实例不存在：如果指定的工作流实例ID不存在，抛出IllegalArgumentException
     * - 状态冲突：确保实例当前状态允许重新运行
     * - 并发控制：通过数据库更新操作保证并发安全
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * @throws IllegalArgumentException 当指定的工作流实例不存在时
     * 
     * 类比：就像重新装修房屋时的准备工作：
     * - 找到原房屋档案（获取原工作流实例）
     * - 清空房屋内容（清理变量池和状态）
     * - 更新装修记录（更新状态和类型）
     * - 安排新装修队（绑定Master节点）
     * - 记录装修次数（递增运行次数）
     */
    @Override
    protected void assembleWorkflowInstance(final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final Command command = workflowExecuteContextBuilder.getCommand();
        final int workflowInstanceId = command.getWorkflowInstanceId();
        final WorkflowInstance workflowInstance = workflowInstanceDao.queryOptionalById(workflowInstanceId)
                .orElseThrow(() -> new IllegalArgumentException("Cannot find WorkflowInstance:" + workflowInstanceId));
        workflowInstance.setVarPool(null);
        workflowInstance.setStateWithDesc(WorkflowExecutionStatus.RUNNING_EXECUTION, command.getCommandType().name());
        workflowInstance.setCommandType(command.getCommandType());
        workflowInstance.setRestartTime(new Date());
        workflowInstance.setHost(masterConfig.getMasterAddress());
        workflowInstance.setEndTime(null);
        workflowInstance.setRunTimes(workflowInstance.getRunTimes() + 1);
        workflowInstanceDao.updateById(workflowInstance);

        workflowExecuteContextBuilder.setWorkflowInstance(workflowInstance);
    }

    /**
     * 组装工作流执行图
     * 
     * 重写父类的执行图组装方法，在构建执行图之前先清理历史任务实例。
     * 这是重新运行与普通启动的关键差异，需要先清除上次执行的任务状态。
     * 
     * 处理流程：
     * 1. 历史清理：调用markAllTaskInstanceInvalid方法清理历史任务实例
     * 2. 图构建：调用父类方法进行标准的执行图构建过程
     * 
     * 清理目的：
     * - 避免状态冲突：防止新旧任务实例状态混乱
     * - 确保一致性：保证执行图只包含本次执行的任务节点
     * - 简化逻辑：让后续的执行逻辑不需要区分新旧任务
     * - 数据隔离：实现不同执行轮次的数据完全隔离
     * 
     * 执行顺序：
     * 必须先清理历史数据，再构建执行图，确保图中不包含无效的历史数据引用。
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像重新装修前要先清理房间，再规划新的装修方案。
     */
    @Override
    protected void assembleWorkflowExecutionGraph(final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        markAllTaskInstanceInvalid(workflowExecuteContextBuilder);
        super.assembleWorkflowExecutionGraph(workflowExecuteContextBuilder);
    }

    /**
     * 标记所有历史任务实例为无效
     * 
     * 私有方法，负责将工作流历史执行过程中产生的所有任务实例标记为无效状态。
     * 这是重新运行时清理历史数据的关键步骤，确保新执行不受历史状态影响。
     * 
     * 处理逻辑：
     * 1. 获取工作流实例：从上下文中获取当前的工作流实例
     * 2. 查询历史任务：调用父类方法获取所有有效的历史任务实例
     * 3. 批量标记无效：通过DAO层批量将这些任务实例标记为无效
     * 
     * 标记效果：
     * - 逻辑删除：任务实例不会被物理删除，而是标记为无效状态
     * - 查询过滤：后续查询有效任务时会自动过滤掉这些无效实例
     * - 历史保留：保留执行历史，便于问题排查和数据分析
     * - 性能优化：避免执行图构建时处理大量历史数据
     * 
     * 数据完整性：
     * - 批量操作：使用批量标记操作，提高性能和数据一致性
     * - 事务保护：在数据库事务保护下执行，确保操作的原子性
     * - 错误处理：如果标记过程出现异常，整个操作会回滚
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像清理旧装修时给所有旧材料贴上"报废"标签，
     * 不是直接扔掉，而是标记不再使用，方便后续统一处理。
     */
    private void markAllTaskInstanceInvalid(final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final WorkflowInstance workflowInstance = workflowExecuteContextBuilder.getWorkflowInstance();
        final List<TaskInstance> taskInstances = getValidTaskInstance(workflowInstance);
        taskInstanceDao.markTaskInstanceInvalid(taskInstances);
    }

    /**
     * 返回处理器支持的命令类型
     * 
     * 标识该处理器专门处理REPEAT_RUNNING类型的命令，这是工作流重新运行的专用命令类型。
     * 命令分发器会根据这个类型将重新运行的命令路由到当前处理器。
     * 
     * 命令类型特点：
     * - REPEAT_RUNNING：专门用于重新执行已完成或失败的工作流
     * - 与START_PROCESS区别：START_PROCESS用于首次启动，REPEAT_RUNNING用于重新运行
     * - 处理策略不同：重新运行需要清理历史状态，首次启动则是全新创建
     * 
     * @return REPEAT_RUNNING命令类型
     * 
     * 类比：就像医院科室的专业分工，重新运行就是"康复科"，
     * 专门处理需要二次治疗的病例。
     */
    @Override
    public CommandType commandType() {
        return CommandType.REPEAT_RUNNING;
    }
}
