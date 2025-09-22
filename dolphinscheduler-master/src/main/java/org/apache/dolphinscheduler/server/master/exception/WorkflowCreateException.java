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
 * 工作流创建异常
 * <p>
 * 当Master节点在创建工作流实例时发生错误时抛出此异常。
 * 工作流创建是调度系统的核心环节，涉及DAG解析、任务依赖关系构建、资源分配等多个步骤。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>工作流定义解析失败（JSON格式错误、字段缺失等）</li>
 *   <li>任务依赖关系存在循环依赖</li>
 *   <li>任务节点配置验证失败</li>
 *   <li>工作流参数解析或验证错误</li>
 *   <li>资源分配失败（队列、租户、环境等）</li>
 *   <li>权限验证失败（用户无权限执行该工作流）</li>
 *   <li>数据库操作异常导致工作流实例创建失败</li>
 *   <li>全局参数或变量解析错误</li>
 *   <li>工作流版本不兼容或已被删除</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录详细的创建失败原因和工作流信息</li>
 *   <li>验证工作流定义的完整性和正确性</li>
 *   <li>检查用户权限和资源配置</li>
 *   <li>将工作流实例状态标记为失败</li>
 *   <li>清理已创建的部分数据</li>
 *   <li>发送创建失败通知</li>
 *   <li>记录错误日志供问题排查</li>
 *   <li>不进行自动重试，需要修复问题后手动重新提交</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class WorkflowCreateException extends MasterException {

    /**
     * 构造工作流创建异常
     *
     * @param message 异常消息，描述工作流创建失败的具体原因
     */
    public WorkflowCreateException(String message) {
        super(message);
    }

    /**
     * 构造工作流创建异常
     *
     * @param message   异常消息，描述工作流创建失败的具体原因
     * @param throwable 引起此异常的根本原因，用于异常链跟踪
     */
    public WorkflowCreateException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
