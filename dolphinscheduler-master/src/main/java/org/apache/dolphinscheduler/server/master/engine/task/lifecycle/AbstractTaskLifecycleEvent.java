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

package org.apache.dolphinscheduler.server.master.engine.task.lifecycle;

import org.apache.dolphinscheduler.server.master.engine.AbstractLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

/**
 * 任务生命周期事件抽象基类
 *
 * 所有任务生命周期事件的抽象基类，定义了任务事件的基本结构和接口。
 * 继承自{@link AbstractLifecycleEvent}，扩展了任务特定的功能。
 *
 * 设计理念：
 * - 事件驱动：基于事件驱动模式，解耦事件发布者和处理者
 * - 延迟执行：支持延迟执行机制，可以设置事件的延迟处理时间
 * - 任务关联：每个事件都关联一个具体的任务执行实例
 * - 类型安全：通过抽象基类确保类型安全性
 *
 * 核心特性：
 * 1. 延迟机制：支持延迟触发事件，用于实现定时任务、重试等场景
 * 2. 任务绑定：每个事件都绑定一个任务执行实例，确保事件处理的上下文正确性
 * 3. 继承体系：为所有任务生命周期事件提供统一的基础结构
 *
 * 使用场景：
 * - 任务状态变化通知
 * - 任务控制操作触发
 * - 任务异常处理事件
 * - 任务生命周期管理
 *
 * 类比理解：
 * 就像医院的病例记录卡，每张卡片记录一个特定的医疗事件（检查、治疗、用药等），
 * 都包含基本信息（患者、时间、事件类型）和具体内容。
 *
 * @see AbstractLifecycleEvent 生命周期事件基类
 * @see ITaskExecutionRunnable 任务执行实例接口
 * @see TaskLifecycleEventType 任务生命周期事件类型
 */
public abstract class AbstractTaskLifecycleEvent extends AbstractLifecycleEvent {

    /**
     * 默认构造方法
     *
     * 创建一个不延迟执行的任务生命周期事件。
     * 事件将在发布后立即被处理。
     */
    public AbstractTaskLifecycleEvent() {
        super();
    }

    /**
     * 延迟构造方法
     *
     * 创建一个延迟执行的任务生命周期事件。
     * 事件将在指定的延迟时间后被处理。
     *
     * @param delayTime 延迟时间（毫秒），表示事件发布后多久开始处理
     *                  - 0：立即处理
     *                  - 正数：延迟指定毫秒数后处理
     *                  - 负数：立即处理（等同于0）
     *
     * 使用场景：
     * - 任务重试：失败后延迟一段时间再重试
     * - 定时触发：在特定时间点触发事件
     * - 限流控制：避免事件处理过于频繁
     */
    public AbstractTaskLifecycleEvent(long delayTime) {
        super(delayTime);
    }

    /**
     * 获取关联的任务执行实例
     *
     * 返回与此生命周期事件关联的任务执行实例。
     * 每个任务生命周期事件都必须关联一个具体的任务执行实例，
     * 以确保事件处理时能够访问正确的任务上下文。
     *
     * @return 任务执行实例，包含任务的执行上下文和状态信息
     *
     * 实现要求：
     * - 返回值不能为null
     * - 返回的实例必须是有效的任务执行上下文
     * - 实例应该包含完整的任务状态和执行信息
     *
     * 类比理解：
     * 就像每张病例记录卡都必须标明是哪个患者的，
     * 每个任务事件都必须明确关联的是哪个任务实例。
     */
    public abstract ITaskExecutionRunnable getTaskExecutionRunnable();

}
