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

package org.apache.dolphinscheduler.server.master.engine.workflow.listener;

import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

/**
 * 工作流生命周期监听器接口
 * <p>
 * 定义工作流生命周期事件的监听器接口，用于处理工作流在不同生命周期阶段的事件通知。
 * 监听器可以根据具体的事件类型来执行相应的业务逻辑，如工作流成功、失败、超时等。
 * </p>
 *
 * @author DolphinScheduler
 */
public interface IWorkflowLifecycleListener {

    /**
     * 通知工作流生命周期事件
     * <p>
     * 当工作流的生命周期状态发生变化时，会调用此方法来通知监听器。
     * 监听器可以根据事件类型和工作流实例信息来执行相应的处理逻辑。
     * </p>
     *
     * @param workflowExecutionRunnable 工作流执行运行时对象，包含工作流实例和执行上下文信息
     * @param workflowLifecycleLifecycleEvent 工作流生命周期事件对象，包含事件类型和相关数据
     */
    void notifyWorkflowLifecycleEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                      final AbstractWorkflowLifecycleLifecycleEvent workflowLifecycleLifecycleEvent);

    /**
     * 判断是否匹配当前事件
     * <p>
     * 监听器通过此方法判断是否需要处理当前的工作流生命周期事件。
     * 只有匹配的事件才会被当前监听器处理。
     * </p>
     *
     * @param event 工作流生命周期事件
     * @return true-匹配，需要处理该事件；false-不匹配，跳过该事件
     */
    boolean match(AbstractWorkflowLifecycleLifecycleEvent event);

}
