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

package org.apache.dolphinscheduler.server.master.engine;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.engine.exceptions.WorkflowEventFireException;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.runner.IWorkflowExecuteContext;
import org.apache.dolphinscheduler.server.master.utils.ExceptionUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * 工作流事件总线触发工作者
 *
 * 这是工作流事件处理的核心执行器，负责从工作流事件总线中获取事件并触发处理。
 * 每个工作者负责管理一部分工作流实例，确保同一工作流的事件按顺序处理。
 *
 * 核心职责：
 * 1. 管理分配给自己的工作流实例
 * 2. 从工作流事件总线中获取事件
 * 3. 将事件分发给合适的处理器
 * 4. 处理异常情况，如数据库连接失败
 * 5. 维护事件处理的统计信息
 *
 * 设计特点：
 * - 事件隔离：不同工作流的事件相互独立
 * - 顺序保证：同一工作流的事件按顺序处理
 * - 容错处理：支持数据库连接失败时的重试
 * - 统计跟踪：记录事件处理的成功/失败率
 *
 * 类比：就像一个专职快递员，负责特定区域的包裹配送，
 * 确保每个客户的包裹按顺序送达。
 */
@Slf4j
@SuppressWarnings({"rawtypes", "unchecked"})
public class WorkflowEventBusFireWorker {

    /**
     * 已注册的工作流执行实例映射
     * key: 工作流实例ID
     * value: 工作流执行实例
     * 使用ConcurrentHashMap保证线程安全
     */
    private final Map<Integer, IWorkflowExecutionRunnable> registeredWorkflowExecuteRunnableMap =
            new ConcurrentHashMap<>();

    /**
     * 事件处理器映射
     * key: 事件类型
     * value: 对应的事件处理器
     * 每种事件类型对应一个特定的处理器
     */
    private final Map<ILifecycleEventType, ILifecycleEventHandler> eventHandlerMap = new ConcurrentHashMap<>();

    /**
     * 注册事件处理器
     *
     * 将事件处理器注册到工作者中，每种事件类型对应一个处理器。
     * 注册后，当相应类型的事件被触发时，会自动路由到该处理器。
     *
     * @param eventHandler 事件处理器，不能为null
     * @throws IllegalArgumentException 如果处理器或其事件类型为null
     */
    public void registerEventHandler(ILifecycleEventHandler eventHandler) {
        // 验证处理器不为null
        checkArgument(eventHandler != null, "event handler cannot be null");
        // 验证处理器的事件类型不为null
        checkArgument(eventHandler.matchEventType() != null, "event type cannot be null");
        // 将处理器注册到映射表中
        eventHandlerMap.put(eventHandler.matchEventType(), eventHandler);
    }

    /**
     * 注册工作流事件总线
     *
     * 将工作流执行实例注册到该工作者，之后该工作流的所有事件都由这个工作者处理。
     * 同一工作流实例不能重复注册。
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @throws IllegalStateException 如果工作流实例已经注册
     */
    public void registerWorkflowEventBus(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // 获取工作流执行上下文
        final IWorkflowExecuteContext workflowExecuteContext = workflowExecutionRunnable.getWorkflowExecuteContext();
        // 获取工作流实例
        final WorkflowInstance workflowInstance = workflowExecuteContext.getWorkflowInstance();
        // 获取工作流实例ID
        final Integer workflowInstanceId = workflowInstance.getId();
        // 获取工作流实例名称
        final String workflowInstanceName = workflowInstance.getName();
        // 检查工作流是否已经注册，避免重复注册
        checkState(!registeredWorkflowExecuteRunnableMap.containsKey(workflowInstanceId),
                "WorkflowExecuteRunnable(%s/%s already registered at WorkflowEventBusFireWorker", workflowInstanceId,
                workflowInstanceName);
        // 将工作流实例加入注册表
        registeredWorkflowExecuteRunnableMap.put(workflowInstanceId, workflowExecutionRunnable);
    }

    /**
     * 注销工作流事件总线
     *
     * 从工作者中移除工作流执行实例，通常在工作流结束时调用。
     * 注销后，该工作流的事件不再被处理。
     *
     * @param workflowExecutionRunnable 要注销的工作流执行实例
     */
    public void unRegisterWorkflowEventBus(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // 获取工作流执行上下文
        final IWorkflowExecuteContext workflowExecuteContext = workflowExecutionRunnable.getWorkflowExecuteContext();
        // 获取工作流实例
        final WorkflowInstance workflowInstance = workflowExecuteContext.getWorkflowInstance();
        // 获取工作流实例ID
        final Integer workflowInstanceId = workflowInstance.getId();
        // 从注册表中移除工作流实例
        registeredWorkflowExecuteRunnableMap.remove(workflowInstanceId, workflowExecutionRunnable);
    }

    /**
     * 触发所有已注册工作流的事件
     *
     * 遍历所有注册的工作流，处理它们事件队列中的事件。
     * 这是工作者的主要工作方法，由外部定期调用。
     *
     * 处理流程：
     * 1. 获取所有有事件待处理的工作流
     * 2. 依次处理每个工作流的事件
     * 3. 设置MDC日志上下文，方便跟踪
     * 4. 处理异常情况，确保一个工作流的异常不影响其他
     */
    public void fireAllRegisteredEvent() {
        // 获取所有有事件待处理的工作流
        final List<IWorkflowExecutionRunnable> workflowExecutionRunnables = getWaitingFireWorkflowExecutionRunnables();
        if (CollectionUtils.isEmpty(workflowExecutionRunnables)) {
            // 没有待处理的事件，直接返回
            return;
        }
        // 遍历每个工作流，处理其事件
        for (final IWorkflowExecutionRunnable workflowExecutionRunnable : workflowExecutionRunnables) {
            final Integer workflowInstanceId = workflowExecutionRunnable.getId();
            final String workflowInstanceName = workflowExecutionRunnable.getName();
            try {
                // 设置日志MDC上下文，方便跟踪特定工作流的日志
                LogUtils.setWorkflowInstanceIdMDC(workflowInstanceId);
                // 处理单个工作流的事件
                doFireSingleWorkflowEventBus(workflowExecutionRunnable);
            } catch (Exception ex) {
                // 记录错误日志，但不中断其他工作流的处理
                log.error("Fire event failed for WorkflowExecuteRunnable: {}", workflowInstanceName, ex);
            } finally {
                // 清理MDC上下文
                LogUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    public int getRegisteredWorkflowExecuteRunnableSize() {
        return registeredWorkflowExecuteRunnableMap.size();
    }

    /**
     * 获取等待触发事件的工作流实例列表
     *
     * 筛选出事件队列不为空的工作流实例，这些实例有事件需要处理。
     *
     * @return 有事件待处理的工作流实例列表
     */
    private List<IWorkflowExecutionRunnable> getWaitingFireWorkflowExecutionRunnables() {
        if (MapUtils.isEmpty(registeredWorkflowExecuteRunnableMap)) {
            // 没有注册的工作流，返回空列表
            return Collections.emptyList();
        }
        // 筛选出事件队列不为空的工作流
        return registeredWorkflowExecuteRunnableMap.values()
                .stream()
                .filter(workflowExecuteRunnable -> !workflowExecuteRunnable.getWorkflowEventBus().isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * 处理单个工作流的事件总线
     *
     * 从工作流的事件队列中依次取出事件并处理，直到队列为空。
     * 特殊处理数据库连接失败的情况，支持重试。
     *
     * @param workflowExecutionRunnable 要处理事件的工作流实例
     * @throws WorkflowEventFireException 如果事件处理失败
     */
    private void doFireSingleWorkflowEventBus(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // 获取工作流的事件总线
        final WorkflowEventBus workflowEventBus = workflowExecutionRunnable.getWorkflowEventBus();
        // 持续处理事件，直到队列为空
        while (!workflowEventBus.isEmpty()) {
            // 从队列中获取一个事件
            Optional<AbstractLifecycleEvent> eventOptional = workflowEventBus.poll();
            if (!eventOptional.isPresent()) {
                // 没有可处理的事件，退出
                return;
            }
            final AbstractLifecycleEvent lifecycleEvent = eventOptional.get();
            try {
                // 由于在FinalizeEventHandler中会打印事件计数
                // 所以在事件触发前先增加计数，以确保计数准确
                // 如果事件处理失败，再减少成功计数
                workflowEventBus.getWorkflowEventBusSummary().increaseFireSuccessEventCount();
                // 触发单个事件
                doFireSingleEvent(workflowExecutionRunnable, lifecycleEvent);
            } catch (Exception ex) {
                // 如果是数据库连接失败，不移除事件
                // 这样可以在数据库恢复后再次处理该事件
                if (ExceptionUtils.isDatabaseConnectedFailedException(ex)) {
                    // 将事件重新放回队列
                    workflowEventBus.publish(lifecycleEvent);
                    // 等待5秒后重试
                    ThreadUtils.sleep(5_000);
                    return;
                }
                // 更新统计信息
                workflowEventBus.getWorkflowEventBusSummary().decreaseFireSuccessEventCount();
                workflowEventBus.getWorkflowEventBusSummary().increaseFireFailedEventCount();
                // 抛出事件处理异常
                throw new WorkflowEventFireException(lifecycleEvent, ex);
            }
        }
    }

    /**
     * 触发单个事件
     *
     * 根据事件类型找到对应的处理器，并调用处理器处理事件。
     * 如果找不到对应的处理器，抛出异常。
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param event 要处理的事件
     * @throws RuntimeException 如果找不到对应的事件处理器
     */
    private void doFireSingleEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                   final AbstractLifecycleEvent event) {
        // 根据事件类型获取对应的处理器
        final ILifecycleEventHandler lifecycleEventHandler = eventHandlerMap.get(event.getEventType());
        if (lifecycleEventHandler == null) {
            // 找不到处理器，抛出异常
            throw new RuntimeException("No EventHandler found for event: " + event.getEventType());
        }
        // 调用处理器处理事件
        lifecycleEventHandler.handle(workflowExecutionRunnable, event);
    }

}
