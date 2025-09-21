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
 * 任务执行上下文创建异常
 * <p>
 * 当Master节点在创建任务执行上下文（TaskExecutionContext）时发生错误时抛出此异常。
 * 任务执行上下文包含了任务执行所需的所有信息，如参数、资源、环境配置等，
 * 是Master节点与Worker节点之间传递任务信息的重要载体。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>任务参数解析或序列化失败</li>
 *   <li>资源文件信息获取失败（UDF、数据源等）</li>
 *   <li>环境变量或配置信息缺失</li>
 *   <li>租户信息查询失败或权限不足</li>
 *   <li>队列信息获取失败</li>
 *   <li>数据源连接信息构建失败</li>
 *   <li>告警配置信息获取异常</li>
 *   <li>任务依赖的全局参数解析失败</li>
 *   <li>执行路径或工作目录创建失败</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录详细的上下文创建失败信息</li>
 *   <li>验证任务配置的完整性</li>
 *   <li>检查相关资源的可用性</li>
 *   <li>将任务状态标记为失败</li>
 *   <li>清理已分配的临时资源</li>
 *   <li>根据重试策略决定是否重新尝试</li>
 *   <li>发送创建失败通知</li>
 *   <li>记录失败日志供排查分析</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class TaskExecutionContextCreateException extends MasterException {

    /**
     * 构造任务执行上下文创建异常
     *
     * @param message 异常消息，描述上下文创建失败的具体原因
     */
    public TaskExecutionContextCreateException(String message) {
        super(message);
    }

}
