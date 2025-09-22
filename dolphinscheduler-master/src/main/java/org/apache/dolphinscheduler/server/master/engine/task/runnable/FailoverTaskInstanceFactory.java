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
import org.apache.dolphinscheduler.server.master.engine.ITaskGroupCoordinator;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.FailoverTaskInstanceFactory.FailoverTaskInstanceBuilder;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 故障转移任务实例工厂
 *
 * 专门用于创建故障转移任务实例的工厂类，负责处理节点故障时的任务转移场景。
 * 继承抽象任务实例工厂，实现了故障转移任务的特定创建逻辑。
 *
 * 核心职责：
 * - 基于故障任务创建新的转移任务实例
 * - 清理原任务的运行时状态和资源占用
 * - 释放任务组资源槽位
 * - 管理原任务和新转移任务的状态转换
 *
 * 设计特点：
 * - 工厂模式：封装故障转移任务实例的创建逻辑
 * - 建造者模式：通过Builder简化对象构造
 * - 模板方法：复用父类的克隆方法
 * - 事务管理：确保资源释放和状态转换的原子性
 * - 资源协调：集成任务组协调器管理资源槽位
 *
 * 故障转移策略：
 * 1. 克隆故障任务的配置信息
 * 2. 清理运行时状态（主机、路径、变量池等）
 * 3. 重置为提交成功状态准备重新执行
 * 4. 释放任务组资源槽位
 * 5. 标记原任务为容错需要状态
 * 6. 持久化新的转移任务实例
 *
 * 状态管理：
 * - 新转移实例：设置为SUBMITTED_SUCCESS，准备重新分配
 * - 原故障实例：设置状态为NEED_FAULT_TOLERANCE，标记为容错处理
 * - 资源清理：清除主机、路径、变量池等运行时信息
 * - 提交时间：更新为当前时间
 *
 * 资源协调：
 * - 检查原任务是否占用任务组槽位
 * - 如果占用则通过协调器释放槽位
 * - 确保资源能够被其他任务重新利用
 *
 * 使用场景：
 * - Worker节点故障时的任务转移
 * - 网络分区导致的任务孤立
 * - 系统重启后的任务恢复
 * - 负载均衡触发的任务迁移
 *
 * 事务特性：
 * - 使用@Transactional确保数据一致性
 * - 资源释放、状态更新、新任务创建在同一事务中
 * - 失败时自动回滚，防止资源泄露
 *
 * 类比理解：
 * 就像应急响应中的"人员转移"操作：
 * - 保留原有的工作任务和要求
 * - 清除与故障环境相关的信息
 * - 释放被占用的资源和设备
 * - 标记原工作地点为待修复状态
 * - 重新安排到可用的工作环境
 */
@Component
public class FailoverTaskInstanceFactory extends AbstractTaskInstanceFactory<FailoverTaskInstanceBuilder> {

    /**
     * 任务实例数据访问对象
     * 用于持久化新转移实例和更新原故障实例
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 任务组协调器
     * 用于管理任务组资源槽位，释放故障任务占用的资源
     */
    @Autowired
    private ITaskGroupCoordinator taskGroupCoordinator;

    /**
     * 创建故障转移任务实例建造者
     * @return 配置好的建造者实例
     */
    @Override
    public FailoverTaskInstanceFactory.FailoverTaskInstanceBuilder builder() {
        return new FailoverTaskInstanceBuilder(this);
    }

    /**
     * 创建故障转移任务实例
     *
     * 基于故障任务实例创建新的转移实例，清理运行时状态，释放资源，
     * 并管理原任务和新任务的状态转换。这是故障转移场景的核心处理逻辑。
     *
     * 创建策略：
     * - 任务克隆：完整复制故障任务的配置信息
     * - 状态清理：清除与故障环境相关的运行时状态
     * - 资源释放：通过任务组协调器释放占用的槽位
     * - 状态转换：设置原任务为容错状态，新任务为提交成功
     * - 时间更新：设置新的提交时间
     *
     * 转移实例属性设置：
     * - ID：重置为null，插入时生成新ID
     * - 执行状态：SUBMITTED_SUCCESS（已成功提交）
     * - 执行主机：重置为null（重新分配）
     * - 变量池：重置为null（重新计算）
     * - 提交时间：设置为当前时间
     * - 日志路径：重置为null（重新生成）
     * - 执行路径：重置为null（重新创建）
     *
     * 原故障任务处理：
     * - 设置标志为NO，停用原任务实例
     * - 状态设置为NEED_FAULT_TOLERANCE，标记为容错处理
     * - 保留故障任务记录供分析和追踪
     *
     * 资源管理：
     * - 检查原任务是否占用任务组槽位
     * - 如果占用则通过协调器释放槽位资源
     * - 确保资源能够被新任务或其他任务使用
     *
     * 事务保证：
     * - 资源释放、新任务创建、原任务更新在同一事务中
     * - 确保状态转换和资源管理的原子性
     * - 失败时自动回滚，防止资源泄露和状态不一致
     *
     * @param builder 包含故障任务实例的建造者对象
     * @return 创建的故障转移任务实例
     */
    @Transactional
    @Override
    public TaskInstance createTaskInstance(FailoverTaskInstanceBuilder builder) {
        final TaskInstance needFailoverTaskInstance = builder.needFailoverTaskInstance;
        final TaskInstance taskInstance = cloneTaskInstance(needFailoverTaskInstance);
        taskInstance.setId(null);
        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        taskInstance.setHost(null);
        taskInstance.setVarPool(null);
        taskInstance.setSubmitTime(new Date());
        taskInstance.setLogPath(null);
        taskInstance.setExecutePath(null);
        taskInstanceDao.insert(taskInstance);

        if (taskGroupCoordinator.needToReleaseTaskGroupSlot(needFailoverTaskInstance)) {
            taskGroupCoordinator.releaseTaskGroupSlot(needFailoverTaskInstance);
        }

        needFailoverTaskInstance.setFlag(Flag.NO);
        needFailoverTaskInstance.setState(TaskExecutionStatus.NEED_FAULT_TOLERANCE);
        taskInstanceDao.updateById(needFailoverTaskInstance);
        return taskInstance;
    }

    /**
     * 故障转移任务实例建造者
     *
     * 专门用于构建故障转移任务实例的建造者类，实现了ITaskInstanceBuilder接口。
     * 提供流式API来设置故障转移任务创建所需的参数。
     *
     * 建造者特点：
     * - 简化参数：故障转移只需要故障任务实例作为输入
     * - 流式接口：支持方法链式调用
     * - 类型安全：编译时确保参数类型正确
     * - 延迟构建：参数设置完成后统一构建对象
     *
     * 必需参数：
     * - 故障任务实例：提供转移所需的完整任务配置
     *
     * 使用模式：
     * ```java
     * TaskInstance failoverInstance = factory.builder()
     *     .withTaskInstance(faultTaskInstance)
     *     .build();
     * ```
     *
     * 故障转移场景：
     * - Worker节点宕机或网络故障
     * - 任务执行超时或异常中断
     * - 系统维护需要转移任务
     * - 负载均衡触发的主动转移
     */
    public static class FailoverTaskInstanceBuilder implements ITaskInstanceFactory.ITaskInstanceBuilder {

        /**
         * 故障转移任务实例工厂引用
         * 用于调用工厂的创建方法
         */
        private final FailoverTaskInstanceFactory failoverTaskInstanceFactory;

        /**
         * 需要故障转移的任务实例
         * 提供转移任务所需的完整配置信息
         */
        private TaskInstance needFailoverTaskInstance;

        /**
         * 构造故障转移任务实例建造者
         * @param failoverTaskInstanceFactory 工厂实例引用
         */
        public FailoverTaskInstanceBuilder(FailoverTaskInstanceFactory failoverTaskInstanceFactory) {
            this.failoverTaskInstanceFactory = failoverTaskInstanceFactory;
        }

        /**
         * 设置需要故障转移的任务实例
         * @param needFailoverTaskInstance 故障任务实例对象
         * @return 建造者自身，支持链式调用
         */
        public FailoverTaskInstanceBuilder withTaskInstance(TaskInstance needFailoverTaskInstance) {
            this.needFailoverTaskInstance = needFailoverTaskInstance;
            return this;
        }

        /**
         * 构建故障转移任务实例
         * 调用工厂的创建方法，生成配置完整的转移任务实例
         * @return 创建的故障转移任务实例
         */
        @Override
        public TaskInstance build() {
            return failoverTaskInstanceFactory.createTaskInstance(this);
        }
    }
}
