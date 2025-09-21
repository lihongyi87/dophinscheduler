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
 * 任务暂停异常
 *
 * 当任务在执行过程中被暂停时抛出的异常。
 * 这是一个控制流异常，用于中断任务的正常执行并保持可恢复状态。
 *
 * 与TaskKillException的区别：
 * - 暂停是可恢复的，任务状态保存为PAUSE
 * - 终止是不可恢复的，任务状态变为KILL
 *
 * 触发场景：
 * 1. 用户手动暂停：通过Web界面或API暂停任务
 * 2. 工作流暂停：整个工作流被暂停时连带暂停所有运行中的任务
 * 3. 依赖条件变化：依赖的上游任务状态发生变化
 * 4. 资源临时不足：等待资源释放时暂停任务
 * 5. 维护模式：系统进入维护状态时暂停新任务
 *
 * 处理策略：
 * - 保存任务当前执行状态
 * - 释放部分可释放的资源
 * - 更新任务状态为PAUSE
 * - 等待恢复信号继续执行
 *
 * 类比：就像工厂生产线的暂停按钮，
 * 暂时停止生产但保持设备状态，随时可以继续。
 */
public class TaskPauseException extends RuntimeException {

    /**
     * 构造任务暂停异常
     *
     * @param message 异常详细信息，说明暂停的原因
     */
    public TaskPauseException(String message) {
        super(message);
    }

    /**
     * 构造任务暂停异常（带原因链）
     *
     * @param message 异常详细信息，说明暂停的原因
     * @param cause 引起此异常的底层异常，用于异常链追踪
     */
    public TaskPauseException(String message, Throwable cause) {
        super(message, cause);
    }

}
