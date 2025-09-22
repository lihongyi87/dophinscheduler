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

package org.apache.dolphinscheduler.server.master.engine.workflow.runnable;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.server.master.engine.command.ICommandHandler;
import org.apache.dolphinscheduler.server.master.engine.exceptions.CommandDuplicateHandleException;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 工作流执行运行器工厂类
 * <p>
 * 该工厂类负责根据命令对象创建工作流执行运行器实例。它是DolphinScheduler Master引擎中
 * 工作流运行器的统一创建入口，通过命令类型匹配相应的命令处理器来构建运行器。
 * <p>
 * 主要功能包括：
 * <ul>
 *   <li>命令的事务性处理，确保命令只被处理一次</li>
 *   <li>根据命令类型选择合适的命令处理器</li>
 *   <li>创建对应的工作流执行运行器实例</li>
 *   <li>防止命令重复处理的安全机制</li>
 * </ul>
 * <p>
 * 该工厂使用事务机制来保证在Master集群重平衡等场景下，
 * 同一个命令不会被多个Master节点重复处理。
 *
 * @author DolphinScheduler Team
 */
@Slf4j
@Component
public class WorkflowExecutionRunnableFactory {

    /**
     * 命令处理器列表
     * <p>
     * Spring会自动注入所有实现了ICommandHandler接口的处理器，
     * 用于根据不同的命令类型执行相应的处理逻辑
     */
    @Autowired
    private List<ICommandHandler> commandHandlers;

    /**
     * 工作流实例数据访问对象
     * <p>
     * 用于对工作流实例进行数据库操作，包括查询、创建、更新等
     */
    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    /**
     * 命令数据访问对象
     * <p>
     * 用于对命令进行数据库操作，包括查询、删除等
     */
    @Autowired
    private CommandDao commandDao;

    /**
     * 根据命令创建工作流执行运行器
     * <p>
     * 该方法使用事务来确保命令只被处理一次。在Master集群重平衡等场景下，
     * 不同Master节点的slot可能会发生变化，通过事务机制可以避免同一个命令
     * 被多个Master节点重复处理。
     * <p>
     * 处理流程：
     * <ol>
     *   <li>在事务中删除命令，如果删除失败则抛出重复处理异常</li>
     *   <li>根据命令类型找到对应的命令处理器</li>
     *   <li>通过命令处理器创建工作流执行运行器</li>
     * </ol>
     *
     * @param command 要处理的命令对象，包含命令类型和相关参数
     * @return 创建的工作流执行运行器实例
     * @throws CommandDuplicateHandleException 如果命令已被处理（命令在数据库中不存在）
     * @throws IllegalArgumentException 如果找不到对应命令类型的处理器
     */
    @Transactional
    public IWorkflowExecutionRunnable createWorkflowExecuteRunnable(Command command) {
        // ========== 第一步：删除命令，确保幂等性 ==========
        // 使用删除操作作为分布式锁的机制
        // 在多Master环境下，只有成功删除命令的Master才能继续处理
        // 这避免了同一个命令被多个Master同时处理的问题
        deleteCommandOrThrow(command);

        // ========== 第二步：创建工作流执行器 ==========
        // 命令删除成功后，说明当前Master获得了处理权
        // 继续执行工作流运行器的创建逻辑
        return doCreateWorkflowExecutionRunnable(command);
    }

    /**
     * 执行工作流执行运行器的创建逻辑
     * <p>
     * 每个工作流执行运行器代表一个工作流实例，根据命令类型的不同，
     * 该方法可能会在数据库中创建新的工作流实例。
     * <p>
     * 创建过程：
     * <ol>
     *   <li>从命令中提取命令类型</li>
     *   <li>在注册的命令处理器中查找匹配的处理器</li>
     *   <li>使用找到的处理器处理命令并创建运行器</li>
     * </ol>
     *
     * @param command 要处理的命令对象
     * @return 创建的工作流执行运行器实例
     * @throws IllegalArgumentException 如果找不到对应命令类型的处理器
     */
    private IWorkflowExecutionRunnable doCreateWorkflowExecutionRunnable(Command command) {
        // ========== 第一步：获取命令类型 ==========
        // 命令类型决定了工作流的执行方式
        // 如：START（首次执行）、RECOVER（恢复执行）、REPEAT（重复执行）等
        final CommandType commandType = command.getCommandType();

        // ========== 第二步：查找命令处理器 ==========
        // 使用策略模式，每种命令类型对应一个处理器
        // 处理器负责根据命令创建相应的工作流实例
        final ICommandHandler commandHandler = commandHandlers
                .stream()
                .filter(c -> c.commandType() == commandType) // 筛选匹配命令类型的处理器
                .findFirst()
                // 如果找不到处理器，说明系统配置有问题或收到了未知的命令类型
                .orElseThrow(() -> new IllegalArgumentException(
                        "Cannot find ICommandHandler for commandType: " + commandType));

        // ========== 第三步：处理命令并创建运行器 ==========
        // 委托给具体的命令处理器执行：
        // 1. 创建或恢复工作流实例
        // 2. 解析DAG图
        // 3. 初始化任务列表
        // 4. 构建工作流执行运行器
        return commandHandler.handleCommand(command);
    }

    /**
     * 删除数据库中的命令，如果命令不存在则抛出重复处理异常
     * <p>
     * 这是防止命令重复处理的关键机制。通过尝试删除命令来判断命令是否
     * 已经被其他Master节点处理过。如果删除失败，说明命令已经不存在，
     * 可能已经被处理过了。
     *
     * @param command 要删除的命令对象
     * @throws CommandDuplicateHandleException 如果命令删除失败（命令不存在）
     */
    private void deleteCommandOrThrow(Command command) {
        // ========== 执行命令删除操作 ==========
        // deleteById方法返回true表示删除成功，false表示记录不存在
        // 这是一个原子操作，在事务中执行，保证并发安全
        boolean deleteResult = commandDao.deleteById(command.getId());

        // ========== 检查删除结果 ==========
        if (!deleteResult) {
            // 删除失败的可能原因：
            // 1. 命令已经被其他Master节点删除并处理
            // 2. 命令ID不存在（数据不一致）
            // 3. 数据库操作失败（会抛出异常，不会走到这里）

            // 抛出重复处理异常，上层会捕获并记录日志
            // 这不是错误，而是正常的集群协调机制
            throw new CommandDuplicateHandleException(command);
        }

        // 删除成功，当前Master获得了该命令的处理权
    }
}
