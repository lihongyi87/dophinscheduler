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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.WorkflowLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.WorkflowExecutionRunnable;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 工作流最终化生命周期事件
 *
 * 工作流生命周期的最后阶段，用于从 Master 中移除 {@link WorkflowExecutionRunnable}。
 * 这个事件会清理工作流实例在内存中的相关资源。
 *
 * 事件特点：
 * 1. 是所有工作流的最终事件，无论成功还是失败
 * 2. 负责清理内存中的工作流相关数据
 * 3. 释放系统资源，避免内存泄漏
 * 4. 确保工作流从 Master 的管理中完全移除
 *
 * 清理内容包括：
 * - 从工作流执行器中移除工作流实例
 * - 清理任务实例的内存缓存
 * - 释放线程池和其他计算资源
 * - 清理事件监听器和回调函数
 *
 * 使用场景：
 * - 工作流成功完成后的资源清理
 * - 工作流失败后的资源清理
 * - 工作流被停止后的资源清理
 * - 系统关闭时的批量清理
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowFinalizeLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 需要进行最终化处理的工作流实例。
     */
    private IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流最终化事件
     *
     * 使用静态工厂方法创建事件实例，并进行参数校验。
     *
     * @param workflowExecutionRunnable 要进行最终化的工作流执行对象，不能为null
     * @return 工作流最终化事件实例
     * @throws NullPointerException 如果参数为null
     */
    public static WorkflowFinalizeLifecycleEvent of(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // 参数校验，确保工作流对象不为空
        checkNotNull(workflowExecutionRunnable, "workflowExecutionRunnable is null");
        return new WorkflowFinalizeLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流最终化事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.FINALIZE;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示要进行最终化的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowFinalizeLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getName() +
                '}';
    }
}
