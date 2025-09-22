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

package org.apache.dolphinscheduler.server.master.engine.exceptions;

import org.apache.dolphinscheduler.server.master.engine.AbstractLifecycleEvent;

/**
 * 工作流事件触发异常
 *
 * 当工作流事件在触发过程中发生错误时抛出的异常。
 * 这是一个包装异常，用于封装事件处理过程中的各种底层异常。
 *
 * 触发场景：
 * 1. 事件处理器执行异常：处理器内部逻辑错误
 * 2. 数据库操作失败：状态更新、实例创建等数据库操作异常
 * 3. 网络通信错误：与Worker节点通信失败
 * 4. 资源不足：内存、连接池等资源耗尽
 * 5. 配置错误：事件处理器配置不正确
 *
 * 包含信息：
 * - 触发失败的具体事件对象
 * - 底层异常的详细堆栈信息
 * - 事件类型和相关参数
 *
 * 处理策略：
 * - 记录详细的错误日志
 * - 更新相关统计指标
 * - 根据异常类型决定是否重试
 * - 通知相关的监控系统
 *
 * 类比：就像工厂的事件通知系统发生故障，
 * 某个重要通知无法传达给相关部门。
 */
public class WorkflowEventFireException extends RuntimeException {

    /**
     * 构造工作流事件触发异常
     *
     * @param event 触发失败的生命周期事件对象
     * @param cause 引起异常的底层原因
     */
    public WorkflowEventFireException(AbstractLifecycleEvent event, Throwable cause) {
        // ==========构造详细的异常消息==========
        // 包含事件信息，便于快速定位问题
        super("Failed to fire event: " + event, cause);
    }
}
