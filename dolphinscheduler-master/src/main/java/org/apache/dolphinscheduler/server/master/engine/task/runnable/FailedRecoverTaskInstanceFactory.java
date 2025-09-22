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

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 失败恢复任务实例工厂
 *
 * 专门用于创建失败恢复任务实例的工厂类，负责处理失败任务的恢复执行场景。
 * 继承抽象任务实例工厂，实现了失败恢复任务的特定创建逻辑。
 *
 * 核心职责：
 * - 基于失败任务创建新的恢复任务实例
 * - 清理原任务的运行时状态和环境信息
 * - 保留任务的基础配置和业务逻辑
 * - 管理原任务和新恢复任务的状态转换
 *
 * 设计特点：
 * - 工厂模式：封装失败恢复任务实例的创建逻辑
 * - 建造者模式：通过Builder简化对象构造
 * - 模板方法：复用父类的克隆方法
 * - 事务管理：确保状态转换的原子性
 * - 状态重置：清理运行时信息，保留配置信息
 *
 * 恢复策略：
 * 1. 克隆失败任务的完整配置
 * 2. 清理运行时状态（主机、路径、变量池等）
 * 3. 重置为提交成功状态准备重新执行
 * 4. 更新提交时间为当前时间
 * 5. 禁用原失败任务实例
 * 6. 持久化新的恢复任务实例
 *
 * 状态管理：
 * - 新恢复实例：设置为SUBMITTED_SUCCESS，准备重新执行
 * - 原失败实例：设置标志为NO，停用避免重复处理
 * - 运行时清理：清除主机、路径、变量池等环境相关信息
 * - 时间更新：设置新的提交时间，标记恢复开始
 *
 * 与重试的区别：
 * - 重试：通常是自动化的，在失败后立即或延迟执行
 * - 恢复：通常是手动触发的，用于长时间失败后的修复
 * - 重试：保持重试计数和间隔逻辑
 * - 恢复：重新开始，清理所有运行时状态
 *
 * 使用场景：
 * - 长时间失败任务的手动恢复
 * - 系统故障修复后的任务重新执行
 * - 配置修正后的失败任务恢复
 * - 环境问题解决后的任务重启
 *
 * 事务特性：
 * - 使用@Transactional确保数据一致性
 * - 原任务禁用和新任务创建在同一事务中
 * - 失败时自动回滚，保持数据完整性
 *
 * 类比理解：
 * 就像设备维修后的"重新启动"过程：
 * - 保留设备的基本配置和功能定义
 * - 清除故障时产生的错误状态和临时数据
 * - 重置运行环境和工作状态
 * - 标记旧的故障记录为已处理
 * - 重新投入正常的工作流程
 */
@Component
public class FailedRecoverTaskInstanceFactory
        extends
            AbstractTaskInstanceFactory<FailedRecoverTaskInstanceFactory.FailedRecoverTaskInstanceBuilder> {

    /**
     * 任务实例数据访问对象
     * 用于持久化新恢复实例和更新原失败实例
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 创建失败恢复任务实例建造者
     * @return 配置好的建造者实例
     */
    @Override
    public FailedRecoverTaskInstanceFactory.FailedRecoverTaskInstanceBuilder builder() {
        return new FailedRecoverTaskInstanceBuilder(this);
    }

    /**
     * 创建失败恢复任务实例
     *
     * 基于失败的任务实例创建新的恢复实例，清理运行时状态，
     * 并管理原任务和新任务的状态转换。这是失败恢复场景的核心处理逻辑。
     *
     * 创建策略：
     * - 任务克隆：完整复制失败任务的配置信息
     * - 状态清理：清除与执行环境相关的运行时状态
     * - 状态重置：设置为提交成功状态，准备重新执行
     * - 时间更新：设置新的提交时间，标记恢复开始
     * - 状态管理：禁用原失败任务，启用新恢复任务
     *
     * 恢复实例属性设置：
     * - ID：重置为null，插入时生成新ID
     * - 执行状态：SUBMITTED_SUCCESS（已成功提交）
     * - 执行主机：重置为null（重新分配）
     * - 变量池：重置为null（重新计算）
     * - 提交时间：设置为当前时间
     * - 日志路径：重置为null（重新生成）
     * - 执行路径：重置为null（重新创建）
     *
     * 原失败任务处理：
     * - 设置标志为NO，停用原任务实例
     * - 避免原失败任务继续被处理
     * - 保留失败任务的历史记录供分析
     *
     * 恢复场景特点：
     * - 通常是手动触发的操作
     * - 需要清理所有运行时状态
     * - 重新开始完整的执行流程
     * - 不保留失败时的临时数据
     *
     * 事务保证：
     * - 新任务创建和原任务更新在同一事务中
     * - 确保状态转换的原子性和一致性
     * - 失败时自动回滚，防止数据不一致
     *
     * @param builder 包含失败任务实例的建造者对象
     * @return 创建的失败恢复任务实例
     */
    @Transactional
    @Override
    public TaskInstance createTaskInstance(FailedRecoverTaskInstanceBuilder builder) {
        final TaskInstance needRecoverTaskInstance = builder.needRecoverTaskInstance;
        final TaskInstance taskInstance = cloneTaskInstance(needRecoverTaskInstance);
        taskInstance.setId(null);
        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        taskInstance.setHost(null);
        taskInstance.setVarPool(null);
        taskInstance.setSubmitTime(new Date());
        taskInstance.setLogPath(null);
        taskInstance.setExecutePath(null);
        taskInstanceDao.insert(taskInstance);

        needRecoverTaskInstance.setFlag(Flag.NO);
        taskInstanceDao.updateById(needRecoverTaskInstance);
        return taskInstance;
    }

    /**
     * 失败恢复任务实例建造者
     *
     * 专门用于构建失败恢复任务实例的建造者类，实现了ITaskInstanceBuilder接口。
     * 提供流式API来设置失败恢复任务创建所需的参数。
     *
     * 建造者特点：
     * - 简化参数：恢复只需要失败任务实例作为输入
     * - 流式接口：支持方法链式调用
     * - 类型安全：编译时确保参数类型正确
     * - 延迟构建：参数设置完成后统一构建对象
     *
     * 必需参数：
     * - 失败任务实例：提供恢复所需的完整任务配置
     *
     * 使用模式：
     * ```java
     * TaskInstance recoverInstance = factory.builder()
     *     .withTaskInstance(failedTaskInstance)
     *     .build();
     * ```
     *
     * 恢复场景：
     * - 长时间失败任务的手动恢复
     * - 环境修复后的任务重新执行
     * - 配置更正后的失败任务恢复
     * - 系统维护完成后的任务重启
     */
    public static class FailedRecoverTaskInstanceBuilder implements ITaskInstanceFactory.ITaskInstanceBuilder {

        /**
         * 失败恢复任务实例工厂引用
         * 用于调用工厂的创建方法
         */
        private final FailedRecoverTaskInstanceFactory failedRecoverTaskInstanceFactory;

        /**
         * 需要恢复的失败任务实例
         * 提供恢复任务所需的完整配置信息
         */
        private TaskInstance needRecoverTaskInstance;

        /**
         * 构造失败恢复任务实例建造者
         * @param failedRecoverTaskInstanceFactory 工厂实例引用
         */
        public FailedRecoverTaskInstanceBuilder(FailedRecoverTaskInstanceFactory failedRecoverTaskInstanceFactory) {
            this.failedRecoverTaskInstanceFactory = failedRecoverTaskInstanceFactory;
        }

        /**
         * 设置需要恢复的失败任务实例
         * @param needRecoverTaskInstance 失败任务实例对象
         * @return 建造者自身，支持链式调用
         */
        public FailedRecoverTaskInstanceBuilder withTaskInstance(TaskInstance needRecoverTaskInstance) {
            this.needRecoverTaskInstance = needRecoverTaskInstance;
            return this;
        }

        /**
         * 构建失败恢复任务实例
         * 调用工厂的创建方法，生成配置完整的恢复任务实例
         * @return 创建的失败恢复任务实例
         */
        @Override
        public TaskInstance build() {
            return failedRecoverTaskInstanceFactory.createTaskInstance(this);
        }
    }
}
