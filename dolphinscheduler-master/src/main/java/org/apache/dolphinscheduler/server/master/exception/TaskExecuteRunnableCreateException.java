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
 * 任务执行线程创建异常
 * <p>
 * 当Master节点尝试创建任务执行的Runnable对象时发生错误时抛出此异常。
 * 任务执行线程是Master节点用来管理和控制任务生命周期的重要组件。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>任务执行上下文创建失败</li>
 *   <li>任务参数解析或验证错误</li>
 *   <li>执行环境初始化失败</li>
 *   <li>线程池资源不足或配置错误</li>
 *   <li>任务依赖的插件或组件加载失败</li>
 *   <li>权限验证失败导致无法创建执行线程</li>
 *   <li>系统资源不足（内存、文件句柄等）</li>
 *   <li>任务配置信息不完整或格式错误</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录详细的创建失败原因和上下文信息</li>
 *   <li>检查系统资源使用情况</li>
 *   <li>验证任务配置的完整性和正确性</li>
 *   <li>将任务状态标记为失败</li>
 *   <li>释放已分配的资源</li>
 *   <li>根据重试策略决定是否重新尝试</li>
 *   <li>发送告警通知相关人员</li>
 *   <li>记录失败日志供后续分析</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class TaskExecuteRunnableCreateException extends MasterException {

    /**
     * 构造任务执行线程创建异常
     *
     * @param message 异常消息，描述线程创建失败的具体原因
     */
    public TaskExecuteRunnableCreateException(String message) {
        super(message);
    }

    /**
     * 构造任务执行线程创建异常
     *
     * @param message   异常消息，描述线程创建失败的具体原因
     * @param throwable 引起此异常的根本原因，用于异常链跟踪
     */
    public TaskExecuteRunnableCreateException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
