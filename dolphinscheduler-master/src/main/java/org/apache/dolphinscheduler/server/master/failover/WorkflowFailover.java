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

package org.apache.dolphinscheduler.server.master.failover;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.extract.master.command.WorkflowFailoverCommandParam;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 工作流故障转移处理器
 *
 * 专门处理工作流级别的故障转移操作，是Master节点故障转移的核心执行组件。
 * 当Master节点故障时，该组件负责将运行在故障Master上的工作流转移到其他健康的Master节点。
 *
 * <p>工作流故障转移机制：
 * <ul>
 *   <li>状态变更：将工作流状态从运行中变更为FAILOVER</li>
 *   <li>命令生成：创建恢复命令，用于在新的Master上重新启动工作流</li>
 *   <li>事务保证：确保状态更新和命令创建的原子性</li>
 * </ul>
 *
 * <p>与任务故障转移的区别：
 * <ul>
 *   <li>粒度更大：处理整个工作流而不是单个任务</li>
 *   <li>影响更广：可能影响工作流的整体执行流程</li>
 *   <li>恢复更复杂：需要重新构建工作流的执行上下文</li>
 * </ul>
 *
 * <p>故障转移策略：
 * <ul>
 *   <li>容错恢复：使用RECOVER_TOLERANCE_FAULT_PROCESS命令类型</li>
 *   <li>状态保持：保留原始工作流状态信息用于恢复</li>
 *   <li>定义绑定：保持与原工作流定义的版本关系</li>
 * </ul>
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
@Slf4j
@Component
public class WorkflowFailover {

    /**
     * 工作流实例数据访问对象，用于操作工作流实例的数据库记录
     */
    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    /**
     * 命令数据访问对象，用于创建工作流恢复命令
     */
    @Autowired
    private CommandDao commandDao;

    /**
     * 执行工作流故障转移
     *
     * 这是工作流故障转移的核心方法，通过数据库事务确保状态更新和命令创建的原子性。
     * 该方法将工作流标记为FAILOVER状态，并创建恢复命令，以便其他Master节点接管执行。
     *
     * <p>故障转移的两个关键步骤：
     * <ol>
     *   <li>状态更新：将工作流状态从原状态变更为FAILOVER</li>
     *   <li>命令创建：生成RECOVER_TOLERANCE_FAULT_PROCESS恢复命令</li>
     * </ol>
     *
     * <p>事务保证的重要性：
     * <ul>
     *   <li>原子性：确保状态更新和命令创建要么都成功，要么都失败</li>
     *   <li>一致性：避免工作流处于中间状态（已标记FAILOVER但没有恢复命令）</li>
     *   <li>可靠性：确保工作流能够被其他Master正确恢复</li>
     * </ul>
     *
     * <p>恢复命令的作用：
     * <ul>
     *   <li>触发恢复：其他Master通过命令队列发现需要恢复的工作流</li>
     *   <li>状态保持：保留工作流的原始执行状态信息</li>
     *   <li>版本绑定：确保使用正确的工作流定义版本</li>
     * </ul>
     *
     * @param workflowInstance 需要故障转移的工作流实例，包含完整的工作流上下文信息
     */
    @Transactional
    public void failoverWorkflow(final WorkflowInstance workflowInstance) {
        // 第一步：将工作流状态更新为FAILOVER
        // 这标志着工作流需要被其他Master节点接管
        workflowInstanceDao.updateWorkflowInstanceState(
                workflowInstance.getId(),
                workflowInstance.getState(),
                WorkflowExecutionStatus.FAILOVER);

        // 构建工作流故障转移命令参数
        // 保存工作流的原始执行状态，用于恢复时参考
        final WorkflowFailoverCommandParam failoverWorkflowCommandParam = WorkflowFailoverCommandParam.builder()
                .workflowExecutionStatus(workflowInstance.getState())
                .build();

        // 第二步：创建工作流恢复命令
        // 使用RECOVER_TOLERANCE_FAULT_PROCESS命令类型进行容错恢复
        final Command failoverCommand = Command.builder()
                .commandParam(JSONUtils.toJsonString(failoverWorkflowCommandParam))
                .commandType(CommandType.RECOVER_TOLERANCE_FAULT_PROCESS)
                .workflowDefinitionCode(workflowInstance.getWorkflowDefinitionCode())
                .workflowDefinitionVersion(workflowInstance.getWorkflowDefinitionVersion())
                .workflowInstanceId(workflowInstance.getId())
                .build();

        // 将恢复命令插入命令队列
        // 其他Master节点会从命令队列中获取并执行该命令
        commandDao.insert(failoverCommand);

        log.info("工作流故障转移成功: [id={}, name={}, state={}]",
                workflowInstance.getId(),
                workflowInstance.getName(),
                workflowInstance.getState().name());
    }

}
