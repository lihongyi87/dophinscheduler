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

package org.apache.dolphinscheduler.server.master.exception;

/**
 * 逻辑任务初始化异常
 * <p>
 * 当Master节点在初始化逻辑任务（如条件任务、依赖任务、子工作流任务等）时发生错误时抛出此异常。
 * 逻辑任务是那些不需要在Worker节点执行，而是在Master节点直接处理的特殊任务类型。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>逻辑任务插件加载失败</li>
 *   <li>任务参数配置错误或缺失</li>
 *   <li>依赖关系解析失败</li>
 *   <li>条件表达式解析错误</li>
 *   <li>子工作流定义不存在或无权限访问</li>
 *   <li>逻辑任务所需的资源不可用</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录详细的初始化错误信息</li>
 *   <li>将任务状态标记为失败</li>
 *   <li>停止相关工作流的执行</li>
 *   <li>发送告警通知给相关用户</li>
 *   <li>不进行自动重试，需要人工检查配置</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class LogicTaskInitializeException extends MasterException {

    /**
     * 构造逻辑任务初始化异常
     *
     * @param message 异常消息，描述初始化失败的具体原因
     */
    public LogicTaskInitializeException(String message) {
        super(message);
    }

    /**
     * 构造逻辑任务初始化异常
     *
     * @param message 异常消息，描述初始化失败的具体原因
     * @param cause   引起此异常的根本原因，用于异常链跟踪
     */
    public LogicTaskInitializeException(String message, Throwable cause) {
        super(message, cause);
    }

}
