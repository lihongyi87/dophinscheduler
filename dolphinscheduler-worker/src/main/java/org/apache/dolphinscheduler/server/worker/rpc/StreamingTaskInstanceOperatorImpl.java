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

package org.apache.dolphinscheduler.server.worker.rpc;

import org.apache.dolphinscheduler.extract.worker.IStreamingTaskInstanceOperator;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceTriggerSavepointRequest;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceTriggerSavepointResponse;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.stream.StreamTask;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskExecutor;
import org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskExecutorRepository;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 流式任务实例操作实现类
 *
 * <p>该类是Worker节点中专门处理流式任务相关操作的RPC服务实现。
 * 实现了IStreamingTaskInstanceOperator接口，提供流式任务的专用操作功能。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>触发流式任务的保存点（Savepoint）操作</li>
 *   <li>管理流式任务的检查点机制</li>
 *   <li>处理流式任务的状态持久化</li>
 *   <li>支持流式任务的故障恢复</li>
 * </ul>
 *
 * <p>流式任务通常运行时间较长，需要特殊的状态管理和容错机制。
 * 该类提供了流式任务运行过程中的关键操作支持。</p>
 *
 * @see IStreamingTaskInstanceOperator
 * @see StreamTask
 * @see PhysicalTaskExecutorRepository
 */
@Slf4j
@Component
public class StreamingTaskInstanceOperatorImpl implements IStreamingTaskInstanceOperator {

    @Autowired
    private PhysicalTaskExecutorRepository physicalTaskExecutorRepository;

    /**
     * 触发流式任务保存点操作
     *
     * <p>该方法用于接收和处理流式任务保存点触发请求。保存点是流式任务的一种
     * 状态持久化机制，用于在任务运行过程中创建一致性检查点。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收保存点触发请求</li>
     *   <li>设置日志上下文中的任务实例ID</li>
     *   <li>从任务执行器仓库中查找对应的任务执行器</li>
     *   <li>验证任务执行器和任务实例的有效性</li>
     *   <li>确认任务为流式任务类型</li>
     *   <li>调用流式任务的保存点方法</li>
     *   <li>返回操作结果</li>
     *   <li>清理日志上下文</li>
     * </ol>
     *
     * <p>保存点功能主要用于：</p>
     * <ul>
     *   <li>定期保存任务执行状态</li>
     *   <li>支持任务从保存点恢复</li>
     *   <li>提供数据一致性保证</li>
     *   <li>支持任务升级和迁移</li>
     * </ul>
     *
     * @param taskInstanceTriggerSavepointRequest 触发保存点请求，包含任务实例ID等信息
     * @return TaskInstanceTriggerSavepointResponse 保存点触发响应，包含操作结果
     */
    @Override
    public TaskInstanceTriggerSavepointResponse triggerSavepoint(TaskInstanceTriggerSavepointRequest taskInstanceTriggerSavepointRequest) {
        // 记录接收到的流式任务保存点触发请求
        log.info("Receive triggerSavepoint request: {}", taskInstanceTriggerSavepointRequest);

        try {
            // 从请求中提取任务实例ID，用于后续的任务查找和操作
            int taskInstanceId = taskInstanceTriggerSavepointRequest.getTaskInstanceId();
            // 在日志上下文中设置任务实例ID，便于日志追踪和问题定位
            LogUtils.setTaskInstanceIdMDC(taskInstanceId);
            // 从物理任务执行器仓库中查找对应的任务执行器
            final Optional<ITaskExecutor> taskExecutorOptional = physicalTaskExecutorRepository.get(taskInstanceId);
            // 检查任务执行器是否存在，如果不存在则返回错误
            if (!taskExecutorOptional.isPresent()) {
                log.error("Cannot find WorkerTaskExecutor for taskInstance: {}", taskInstanceId);
                return TaskInstanceTriggerSavepointResponse.fail("Cannot find TaskExecutionContext");
            }
            // 将通用的任务执行器转换为物理任务执行器
            final PhysicalTaskExecutor taskExecutor = (PhysicalTaskExecutor) taskExecutorOptional.get();
            // 获取具体的任务实例对象
            AbstractTask task = taskExecutor.getPhysicalTask();
            // 检查任务实例是否存在，如果为空则返回错误
            if (task == null) {
                log.error("Cannot find StreamTask for taskInstance:{}", taskInstanceId);
                return TaskInstanceTriggerSavepointResponse.fail("Cannot find StreamTask");
            }
            // 检查任务是否为流式任务类型，只有流式任务才支持保存点操作
            if (!(task instanceof StreamTask)) {
                log.warn("The taskInstance: {} is not StreamTask", taskInstanceId);
                return TaskInstanceTriggerSavepointResponse.fail("The taskInstance is not StreamTask");
            }
            try {
                // 调用流式任务的保存点方法，创建一致性检查点
                // 保存点是流式任务的状态持久化机制，用于故障恢复
                ((StreamTask) task).savePoint();
            } catch (Exception e) {
                // 捕获保存点操作中的异常，记录错误日志并返回失败响应
                log.error("StreamTask: {} call savePoint error", taskInstanceId, e);
                return TaskInstanceTriggerSavepointResponse.fail("StreamTask call savePoint error: " + e.getMessage());
            }
            // 保存点操作成功，返回成功响应
            return TaskInstanceTriggerSavepointResponse.success();
        } finally {
            // 清理日志上下文中的任务实例ID，防止内存泄漏和日志混乱
            LogUtils.removeTaskInstanceIdMDC();
        }
    }
}
