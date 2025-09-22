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

import org.apache.dolphinscheduler.eventbus.AbstractDelayEvent;

/**
 * 抽象生命周期事件类
 * 继承自AbstractDelayEvent，为所有生命周期事件提供基础功能
 */
public abstract class AbstractLifecycleEvent extends AbstractDelayEvent {

    /**
     * 默认构造函数
     * 创建一个无延迟的生命周期事件
     */
    public AbstractLifecycleEvent() {
        // ==========调用父类默认构造函数==========
        super(); // 初始化延迟事件的基础属性
    }

    /**
     * 带延迟时间的构造函数
     * 创建一个指定延迟时间的生命周期事件
     *
     * @param delayTime 延迟执行时间（毫秒）
     */
    public AbstractLifecycleEvent(long delayTime) {
        // ==========调用父类带延迟时间的构造函数==========
        super(delayTime); // 设置事件的延迟执行时间
    }

    /**
     * 获取事件类型
     * 抽象方法，子类必须实现以返回具体的事件类型
     *
     * @return 生命周期事件类型
     */
    public abstract ILifecycleEventType getEventType();
}
