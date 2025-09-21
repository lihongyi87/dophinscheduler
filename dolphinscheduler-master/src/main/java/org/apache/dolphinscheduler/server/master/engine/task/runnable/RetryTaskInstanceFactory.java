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
import org.apache.dolphinscheduler.server.master.engine.task.runnable.RetryTaskInstanceFactory.RetryTaskInstanceBuilder;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 重试任务实例工厂
 *
 * 专门用于创建重试任务实例的工厂类，负责处理任务执行失败后的重试场景。
 * 继承抽象任务实例工厂，实现了重试任务的特定创建逻辑。
 *
 * 核心职责：
 * - 基于失败任务创建新的重试任务实例
 * - 继承原任务的配置和上下文信息
 * - 更新重试次数和相关状态
 * - 管理原任务实例和新重试实例的状态
 *
 * 设计特点：
 * - 工厂模式：封装重试任务实例的创建逻辑
 * - 建造者模式：通过Builder简化对象构造
 * - 模板方法：复用父类的克隆方法
 * - 事务管理：确保原任务禁用和新任务创建的原子性
 *
 * 重试策略：
 * 1. 克隆原任务实例的完整配置
 * 2. 重置运行时状态字段（ID、时间、主机等）
 * 3. 递增重试次数
 * 4. 设置为提交成功状态准备执行
 * 5. 禁用原任务实例避免重复执行
 * 6. 持久化新的重试实例
 *
 * 状态管理：
 * - 新重试实例：设置为SUBMITTED_SUCCESS，准备重新执行
 * - 原任务实例：设置标志为NO，停用避免重复执行
 * - 重试次数：在原有基础上加1
 * - 提交时间：更新为当前时间
 *
 * 使用场景：
 * - 任务执行失败时的自动重试
 * - 手动重试失败的任务
 * - 超时任务的重新执行
 * - 异常任务的恢复处理
 *
 * 事务特性：
 * - 使用@Transactional确保数据一致性
 * - 原任务禁用和新任务创建在同一事务中
 * - 失败时自动回滚，保持数据完整性
 *
 * 类比理解：
 * 就像产品生产线上的"返工处理"工艺：
 * - 保留原产品的规格和要求
 * - 重置生产状态为待加工
 * - 记录返工次数和时间
 * - 标记原产品为废品状态
 * - 投入新的生产循环
 */
@Component
public class RetryTaskInstanceFactory extends AbstractTaskInstanceFactory<RetryTaskInstanceBuilder> {

    /**
     * 任务实例数据访问对象
     * 用于持久化新重试实例和更新原任务实例
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 创建重试任务实例建造者
     * @return 配置好的建造者实例
     */
    @Override
    public RetryTaskInstanceBuilder builder() {
        return new RetryTaskInstanceBuilder(this);
    }

    /**
     * 创建重试任务实例
     *
     * 基于失败的任务实例创建新的重试实例，保留原任务的配置信息，
     * 重置运行时状态，并管理原任务和新任务的状态转换。
     *
     * 创建策略：
     * - 任务克隆：完整复制原任务的配置和元数据
     * - 状态重置：清除运行时产生的状态信息
     * - 计数递增：重试次数在原基础上加1
     * - 时间更新：设置新的提交时间
     * - 状态管理：禁用原任务，启用新重试任务
     *
     * 重试实例属性设置：
     * - ID：重置为null，插入时生成新ID
     * - 执行状态：SUBMITTED_SUCCESS（已成功提交）
     * - 进程ID：重置为0
     * - 执行主机：重置为null（重新分配）
     * - 执行路径：重置为null（重新创建）
     * - 日志路径：重置为null（重新生成）
     * - 开始时间：重置为null（重新执行时设置）
     * - 结束时间：重置为null（执行完成时设置）
     * - 提交时间：设置为当前时间
     * - 重试次数：原次数 + 1
     *
     * 原任务处理：
     * - 设置标志为NO，停用原任务实例
     * - 避免原任务继续被调度执行
     * - 保留原任务的历史记录供追踪
     *
     * 事务保证：
     * - 新任务创建和原任务更新在同一事务中
     * - 确保状态转换的原子性和一致性
     * - 失败时自动回滚，防止数据不一致
     *
     * @param builder 包含原任务实例的建造者对象
     * @return 创建的重试任务实例
     */
    @Transactional
    @Override
    public TaskInstance createTaskInstance(RetryTaskInstanceBuilder builder) {
        final TaskInstance needRetryTaskInstance = builder.taskInstance;
        final TaskInstance taskInstance = cloneTaskInstance(needRetryTaskInstance);
        taskInstance.setId(null);
        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        taskInstance.setPid(0);
        taskInstance.setHost(null);
        taskInstance.setExecutePath(null);
        taskInstance.setLogPath(null);
        taskInstance.setStartTime(null);
        taskInstance.setEndTime(null);
        taskInstance.setSubmitTime(new Date());
        taskInstance.setRetryTimes(taskInstance.getRetryTimes() + 1);
        taskInstanceDao.insert(taskInstance);

        needRetryTaskInstance.setFlag(Flag.NO);
        taskInstanceDao.updateById(needRetryTaskInstance);
        return taskInstance;
    }

    /**
     * 重试任务实例建造者
     *
     * 专门用于构建重试任务实例的建造者类，实现了ITaskInstanceBuilder接口。
     * 提供流式API来设置重试任务创建所需的参数。
     *
     * 建造者特点：
     * - 简化参数：重试只需要原任务实例作为输入
     * - 流式接口：支持方法链式调用
     * - 类型安全：编译时确保参数类型正确
     * - 延迟构建：参数设置完成后统一构建对象
     *
     * 必需参数：
     * - 原任务实例：提供重试所需的完整任务配置
     *
     * 使用模式：
     * ```java
     * TaskInstance retryInstance = factory.builder()
     *     .withTaskInstance(failedTaskInstance)
     *     .build();
     * ```
     *
     * 重试场景：
     * - 任务执行失败需要重试
     * - 任务超时需要重新执行
     * - 手动触发的任务重试
     * - 系统故障恢复后的任务重试
     */
    public static class RetryTaskInstanceBuilder implements ITaskInstanceFactory.ITaskInstanceBuilder {

        /**
         * 重试任务实例工厂引用
         * 用于调用工厂的创建方法
         */
        private final RetryTaskInstanceFactory retryTaskInstanceFactory;

        /**
         * 需要重试的原任务实例
         * 提供重试任务所需的完整配置信息
         */
        private TaskInstance taskInstance;

        /**
         * 构造重试任务实例建造者
         * @param retryTaskInstanceFactory 工厂实例引用
         */
        public RetryTaskInstanceBuilder(RetryTaskInstanceFactory retryTaskInstanceFactory) {
            this.retryTaskInstanceFactory = retryTaskInstanceFactory;
        }

        /**
         * 设置需要重试的任务实例
         * @param taskInstance 原任务实例对象
         * @return 建造者自身，支持链式调用
         */
        public RetryTaskInstanceBuilder withTaskInstance(TaskInstance taskInstance) {
            this.taskInstance = taskInstance;
            return this;
        }

        /**
         * 构建重试任务实例
         * 调用工厂的创建方法，生成配置完整的重试任务实例
         * @return 创建的重试任务实例
         */
        @Override
        public TaskInstance build() {
            return retryTaskInstanceFactory.createTaskInstance(this);
        }

    }
}
