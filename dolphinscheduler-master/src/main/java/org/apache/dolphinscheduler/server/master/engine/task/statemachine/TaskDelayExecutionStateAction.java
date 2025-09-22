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

package org.apache.dolphinscheduler.server.master.engine.task.statemachine;

import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 任务延迟执行状态动作处理器
 *
 * 处理任务处于DELAY_EXECUTION状态时的各种生命周期事件。
 * 这个状态表示任务需要延迟一段时间后才开始执行，继承自TaskSubmittedStateAction。
 *
 * 状态特征：
 * - 任务已提交但需要延迟执行
 * - 等待延迟时间到达
 * - 延迟期间可以被暂停或取消
 * - 延迟结束后自动转入正常分发流程
 *
 * 设计说明：
 * 由于延迟执行的行为与已提交状态基本相同，只是增加了时间延迟，
 * 所以直接继承TaskSubmittedStateAction的所有行为，只覆盖matchState()方法。
 *
 * 类比理解：
 * 就像定时生产订单，在指定时间之前保持等待状态，
 * 时间到达后按照正常订单流程处理。
 */
@Slf4j
@Component
public class TaskDelayExecutionStateAction extends TaskSubmittedStateAction {

    @Override
    public TaskExecutionStatus matchState() {
        return TaskExecutionStatus.DELAY_EXECUTION;
    }
}
