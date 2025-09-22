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

/**
 * 任务终止异常
 *
 * 当任务在执行过程中被强制终止（Kill）时抛出的异常。
 * 这是一个控制流异常，用于中断任务的正常执行流程。
 *
 * 触发场景：
 * 1. 用户手动终止：通过Web界面或API手动停止任务
 * 2. 工作流终止：整个工作流被停止时连带终止所有运行中的任务
 * 3. 超时终止：任务执行超过设定的超时时间
 * 4. 系统关闭：Master节点关闭时终止所有正在执行的任务
 * 5. 资源不足：系统资源紧张时主动终止低优先级任务
 *
 * 处理策略：
 * - 立即停止任务执行
 * - 释放任务占用的资源
 * - 更新任务状态为KILL
 * - 通知相关的依赖任务
 *
 * 类比：就像工厂生产线上的紧急停止按钮，
 * 当出现异常情况时立即停止当前的生产任务。
 */
public class TaskKillException extends RuntimeException {

    /**
     * 构造任务终止异常
     *
     * @param message 异常详细信息，说明终止的原因
     */
    public TaskKillException(String message) {
        // ==========调用父类构造器==========
        // 设置异常消息，用于后续的异常处理和日志记录
        super(message);
    }

    /**
     * 构造任务终止异常（带原因链）
     *
     * @param message 异常详细信息，说明终止的原因
     * @param cause 引起此异常的底层异常，用于异常链追踪
     */
    public TaskKillException(String message, Throwable cause) {
        // ==========调用父类构造器==========
        // 设置异常消息和原因链，便于问题诊断和调试
        super(message, cause);
    }

}
