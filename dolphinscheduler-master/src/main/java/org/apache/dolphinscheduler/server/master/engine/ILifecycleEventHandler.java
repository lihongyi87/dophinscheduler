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

import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

/**
 * 生命周期事件处理器接口
 * 用于处理特定类型的生命周期事件，所有生命周期事件处理器都需要实现此接口
 *
 * @param <T> 继承自AbstractLifecycleEvent的具体事件类型
 */
public interface ILifecycleEventHandler<T extends AbstractLifecycleEvent> {

    /**
     * 处理生命周期事件
     * 具体的事件处理逻辑由实现类定义
     *
     * @param workflowExecutionRunnable 工作流执行运行时对象，提供工作流运行上下文
     * @param event 要处理的生命周期事件对象
     */
    void handle(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                final T event);

    /**
     * 匹配事件类型
     * 返回当前处理器能够处理的事件类型，用于事件路由
     *
     * @return 匹配的生命周期事件类型
     */
    ILifecycleEventType matchEventType();

}
