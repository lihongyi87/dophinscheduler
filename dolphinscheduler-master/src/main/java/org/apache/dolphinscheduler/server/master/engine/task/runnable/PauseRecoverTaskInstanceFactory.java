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

package org.apache.dolphinscheduler.server.master.engine.task.runnable;

import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 暂停恢复任务实例工厂
 *
 * 专门用于处理暂停任务恢复的工厂类，负责将暂停状态的任务恢复到可执行状态。
 * 继承抽象任务实例工厂，实现了暂停恢复任务的特定处理逻辑。
 *
 * 核心职责：
 * - 恢复暂停状态的任务实例
 * - 更新任务状态为可执行状态
 * - 保持任务的所有配置和上下文信息
 * - 维护任务实例的完整性和连续性
 *
 * 设计特点：
 * - 工厂模式：封装暂停恢复的处理逻辑
 * - 建造者模式：通过Builder简化对象操作
 * - 状态转换：专注于状态更新，不创建新实例
 * - 事务管理：确保状态更新的原子性
 * - 轻量级操作：只更新状态，保留所有原有信息
 *
 * 恢复策略：
 * 1. 接收暂停状态的任务实例
 * 2. 将状态更新为SUBMITTED_SUCCESS
 * 3. 持久化状态变更
 * 4. 返回更新后的原任务实例
 *
 * 与其他恢复的区别：
 * - 失败恢复：创建新实例，清理运行时状态
 * - 故障转移：创建新实例，释放资源，清理环境
 * - 重试：创建新实例，增加重试次数
 * - 暂停恢复：直接更新原实例状态，保留所有信息
 *
 * 状态管理：
 * - 输入状态：通常是PAUSE或相关暂停状态
 * - 输出状态：SUBMITTED_SUCCESS（准备重新执行）
 * - 保留信息：保持所有配置、上下文、时间等信息不变
 * - 实例复用：直接使用原任务实例，不创建新实例
 *
 * 使用场景：
 * - 工作流暂停后的恢复操作
 * - 手动暂停任务的重新启动
 * - 系统维护后的任务恢复
 * - 资源可用后的暂停任务恢复
 *
 * 事务特性：
 * - 使用@Transactional确保状态更新的一致性
 * - 简单的状态更新操作，事务开销最小
 * - 失败时自动回滚，保持状态完整性
 *
 * 优势特点：
 * - 轻量级：不需要复制或创建新对象
 * - 高效：只涉及单一状态字段的更新
 * - 连续性：保持任务执行的连续性和完整性
 * - 简洁性：操作简单直接，逻辑清晰
 *
 * 类比理解：
 * 就像设备的"唤醒"操作：
 * - 设备进入休眠状态后保持所有配置
 * - 唤醒时只需要改变电源状态
 * - 所有程序、数据、设置都保持不变
 * - 可以从上次停止的地方继续工作
 */
@Component
public class PauseRecoverTaskInstanceFactory
        extends
            AbstractTaskInstanceFactory<PauseRecoverTaskInstanceFactory.PauseRecoverTaskInstanceBuilder> {

    /**
     * 任务实例数据访问对象
     * 用于更新暂停任务实例的状态
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 创建暂停恢复任务实例建造者
     * @return 配置好的建造者实例
     */
    @Override
    public PauseRecoverTaskInstanceFactory.PauseRecoverTaskInstanceBuilder builder() {
        return new PauseRecoverTaskInstanceBuilder(this);
    }

    /**
     * 恢复暂停的任务实例
     *
     * 将暂停状态的任务实例恢复到可执行状态。与其他工厂不同，
     * 此方法不创建新的任务实例，而是直接更新原实例的状态。
     *
     * 恢复策略：
     * - 状态更新：将任务状态设置为SUBMITTED_SUCCESS
     * - 实例复用：直接使用原任务实例，保留所有信息
     * - 连续性：保持任务执行的连续性和完整性
     * - 轻量级：最小化的操作，只更新必要的状态
     *
     * 状态转换：
     * - 从：PAUSE或其他暂停相关状态
     * - 到：SUBMITTED_SUCCESS（已成功提交，准备执行）
     *
     * 保留信息：
     * - 任务配置：所有任务定义和参数保持不变
     * - 执行上下文：工作流信息、变量池等保持不变
     * - 时间信息：创建时间、提交时间等保持不变
     * - 资源配置：Worker组、环境等配置保持不变
     * - 执行历史：之前的执行记录和状态变更保持不变
     *
     * 与其他恢复方式的对比：
     * - 重试恢复：创建新实例，增加重试计数
     * - 失败恢复：创建新实例，清理运行时状态
     * - 故障转移：创建新实例，释放资源
     * - 暂停恢复：更新原实例，保留所有信息
     *
     * 适用场景：
     * - 任务因为工作流暂停而停止
     * - 用户手动暂停后需要恢复
     * - 资源不足暂停后资源恢复
     * - 维护窗口结束后的任务恢复
     *
     * 事务保证：
     * - 状态更新操作在事务中执行
     * - 确保状态变更的原子性
     * - 失败时自动回滚，保持数据一致性
     *
     * @param builder 包含暂停任务实例的建造者对象
     * @return 状态更新后的原任务实例
     */
    @Transactional
    @Override
    public TaskInstance createTaskInstance(PauseRecoverTaskInstanceBuilder builder) {
        final TaskInstance needRecoverTaskInstance = builder.needRecoverTaskInstance;
        needRecoverTaskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        taskInstanceDao.updateById(needRecoverTaskInstance);
        return needRecoverTaskInstance;
    }

    /**
     * 暂停恢复任务实例建造者
     *
     * 专门用于构建暂停恢复操作的建造者类，实现了ITaskInstanceBuilder接口。
     * 提供流式API来设置暂停恢复所需的参数。
     *
     * 建造者特点：
     * - 简化参数：只需要暂停的任务实例作为输入
     * - 流式接口：支持方法链式调用
     * - 类型安全：编译时确保参数类型正确
     * - 轻量级：专注于状态恢复，不涉及复杂对象创建
     *
     * 必需参数：
     * - 暂停任务实例：需要恢复的暂停状态任务
     *
     * 使用模式：
     * ```java
     * TaskInstance resumedInstance = factory.builder()
     *     .withTaskInstance(pausedTaskInstance)
     *     .build();
     * ```
     *
     * 恢复场景：
     * - 工作流从暂停状态恢复
     * - 用户手动恢复暂停的任务
     * - 资源可用后的任务恢复
     * - 维护完成后的批量任务恢复
     */
    public static class PauseRecoverTaskInstanceBuilder implements ITaskInstanceFactory.ITaskInstanceBuilder {

        /**
         * 暂停恢复任务实例工厂引用
         * 用于调用工厂的恢复方法
         */
        private final PauseRecoverTaskInstanceFactory pauseRecoverTaskInstanceFactory;

        /**
         * 需要恢复的暂停任务实例
         * 提供恢复操作所需的任务对象
         */
        private TaskInstance needRecoverTaskInstance;

        /**
         * 构造暂停恢复任务实例建造者
         * @param pauseRecoverTaskInstanceFactory 工厂实例引用
         */
        public PauseRecoverTaskInstanceBuilder(PauseRecoverTaskInstanceFactory pauseRecoverTaskInstanceFactory) {
            this.pauseRecoverTaskInstanceFactory = pauseRecoverTaskInstanceFactory;
        }

        /**
         * 设置需要恢复的暂停任务实例
         * @param needRecoverTaskInstance 暂停任务实例对象
         * @return 建造者自身，支持链式调用
         */
        public PauseRecoverTaskInstanceBuilder withTaskInstance(TaskInstance needRecoverTaskInstance) {
            this.needRecoverTaskInstance = needRecoverTaskInstance;
            return this;
        }

        /**
         * 执行暂停恢复操作
         * 调用工厂的恢复方法，更新任务状态并返回恢复后的实例
         * @return 恢复后的任务实例（与输入实例是同一个对象）
         */
        @Override
        public TaskInstance build() {
            return pauseRecoverTaskInstanceFactory.createTaskInstance(this);
        }
    }
}
