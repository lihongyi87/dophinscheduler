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

package org.apache.dolphinscheduler.dao.entity.event;

import org.apache.dolphinscheduler.common.enums.ListenerEventType;

/**
 * 监听器事件抽象接口
 *
 * 定义了所有监听器事件必须实现的基本方法，为系统中的各种事件提供统一的接口规范。
 * 这是事件驱动架构中的核心接口，所有需要被监听和处理的事件都应该实现此接口。
 *
 * 设计模式：
 * - 接口定义模式：定义事件的标准契约
 * - 事件驱动模式：作为事件系统的基础接口
 * - 类型安全：通过枚举类型确保事件类型的正确性
 *
 * 核心职责：
 * - 定义事件类型获取方法
 * - 定义事件标题获取方法
 * - 为所有监听器事件提供统一接口
 *
 * 实现类包括：
 * - ProcessStartListenerEvent：工作流开始事件
 * - ProcessEndListenerEvent：工作流结束事件
 * - ProcessFailListenerEvent：工作流失败事件
 * - TaskStartListenerEvent：任务开始事件
 * - TaskEndListenerEvent：任务结束事件
 * - TaskFailListenerEvent：任务失败事件
 * - ProcessDefinitionCreatedListenerEvent：工作流定义创建事件
 * - ProcessDefinitionUpdatedListenerEvent：工作流定义更新事件
 * - ProcessDefinitionDeletedListenerEvent：工作流定义删除事件
 * - ServerDownListenerEvent：服务器宕机事件
 *
 * 类比：就像新闻社的新闻格式标准，所有新闻都必须有类型（政治、经济、体育等）
 *      和标题，这样订阅者才能根据类型选择感兴趣的新闻。
 */
public interface AbstractListenerEvent {

    /**
     * 获取事件类型
     *
     * 返回事件的具体类型，用于事件分发和处理。
     * 不同的事件类型会被路由到不同的处理器进行处理。
     *
     * 使用场景：
     * - 事件分发：根据类型将事件发送给相应的监听器
     * - 事件过滤：监听器可以只处理特定类型的事件
     * - 事件统计：统计各种类型事件的发生频率
     *
     * @return 事件类型枚举值，不能为null
     */
    ListenerEventType getEventType();

    /**
     * 获取事件标题
     *
     * 返回事件的描述性标题，通常用于日志记录、通知消息等场景。
     * 标题应该简洁明了，能够让人快速理解事件的核心内容。
     *
     * 格式建议：
     * - 工作流事件："工作流[{name}]开始执行"
     * - 任务事件："任务[{name}]执行成功"
     * - 定义事件："工作流定义[{name}]已创建"
     * - 服务事件："服务器[{host}]连接断开"
     *
     * @return 事件标题字符串，不能为null或空字符串
     */
    String getTitle();
}
