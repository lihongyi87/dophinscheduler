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
 * Master节点异常类
 * <p>
 * 这是DolphinScheduler Master服务中所有异常的基础类。
 * 用于表示Master节点在执行任务调度、工作流管理等操作时可能发生的各种错误情况。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>工作流创建或解析失败</li>
 *   <li>任务调度过程中发生错误</li>
 *   <li>逻辑任务初始化或执行失败</li>
 *   <li>Master与Worker节点通信异常</li>
 *   <li>资源分配或配置错误</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录详细错误日志</li>
 *   <li>根据异常类型进行相应的重试或回滚操作</li>
 *   <li>通知相关监控系统</li>
 *   <li>更新任务或工作流状态为失败</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class MasterException extends Exception {

    /**
     * 构造Master异常
     *
     * @param message 异常消息，描述具体的错误情况
     */
    public MasterException(String message) {
        super(message);
    }

    /**
     * 构造Master异常
     *
     * @param message   异常消息，描述具体的错误情况
     * @param throwable 引起此异常的原因，用于异常链跟踪
     */
    public MasterException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
