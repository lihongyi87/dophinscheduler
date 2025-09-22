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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle;

import org.apache.dolphinscheduler.server.master.engine.AbstractLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

/**
 * 工作流生命周期事件抽象基类
 *
 * 这是所有工作流生命周期事件的基础类，提供了事件的通用功能和结构。
 * 所有具体的工作流事件（如启动、暂停、停止等）都必须继承这个类。
 *
 * 主要功能：
 * 1. 继承通用生命周期事件的基础能力（如延迟执行）
 * 2. 要求子类提供工作流执行对象的访问方法
 * 3. 为工作流事件提供统一的抽象接口
 *
 * 简单理解：这就像是所有工作流事件的"模板"，定义了事件必须具备的基本要素。
 *
 * @author DolphinScheduler
 */
public abstract class AbstractWorkflowLifecycleLifecycleEvent extends AbstractLifecycleEvent {

    /**
     * 默认构造函数
     *
     * 创建一个立即执行的工作流生命周期事件。
     * 延迟时间设置为0，表示事件会被立即处理。
     */
    public AbstractWorkflowLifecycleLifecycleEvent() {
        super(0L);
    }

    /**
     * 带延迟时间的构造函数
     *
     * 创建一个延迟执行的工作流生命周期事件。
     * 可以用于需要延迟处理的场景，比如重试机制或定时触发。
     *
     * @param delayTime 延迟执行时间（毫秒）
     */
    public AbstractWorkflowLifecycleLifecycleEvent(long delayTime) {
        super(delayTime);
    }

    /**
     * 获取工作流执行对象
     *
     * 这是一个抽象方法，要求所有子类都必须实现。
     * 用于获取与此事件相关的工作流执行对象，该对象包含了
     * 工作流的所有执行信息和上下文。
     *
     * @return 工作流执行对象，包含工作流的完整执行信息
     */
    public abstract IWorkflowExecutionRunnable getWorkflowExecutionRunnable();

}
