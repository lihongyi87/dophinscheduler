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
 * Master节点任务执行异常
 * <p>
 * 当Master节点在执行任务过程中发生错误时抛出此异常。
 * 这包括任务调度、分发、监控以及状态管理等各个环节的执行错误。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>任务分发到Worker节点失败</li>
 *   <li>任务执行过程中Master节点异常</li>
 *   <li>任务状态更新失败</li>
 *   <li>任务超时处理异常</li>
 *   <li>任务依赖关系处理错误</li>
 *   <li>资源分配或调度策略执行失败</li>
 *   <li>与数据库交互异常导致任务信息丢失</li>
 *   <li>集群通信异常影响任务执行</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录详细的任务执行错误信息</li>
 *   <li>根据重试策略决定是否重新执行任务</li>
 *   <li>更新任务状态为失败或等待重试</li>
 *   <li>触发工作流的失败处理逻辑</li>
 *   <li>发送告警通知相关人员</li>
 *   <li>清理相关资源和临时数据</li>
 *   <li>记录执行日志供问题排查</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class MasterTaskExecuteException extends MasterException {

    /**
     * 构造Master节点任务执行异常
     *
     * @param message 异常消息，描述任务执行失败的具体原因
     */
    public MasterTaskExecuteException(String message) {
        super(message);
    }

    /**
     * 构造Master节点任务执行异常
     *
     * @param message 异常消息，描述任务执行失败的具体原因
     * @param cause   引起此异常的根本原因，用于异常链跟踪
     */
    public MasterTaskExecuteException(String message, Throwable cause) {
        super(message, cause);
    }
}
