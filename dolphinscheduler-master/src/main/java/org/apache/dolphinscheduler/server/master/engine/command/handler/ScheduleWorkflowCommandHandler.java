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

package org.apache.dolphinscheduler.server.master.engine.command.handler;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.scheduler.api.SchedulerApi;

import org.springframework.stereotype.Component;

/**
 * 定时调度工作流命令处理器
 * 
 * 这个类专门处理定时调度类型的工作流启动命令。当调度器触发定时任务时，
 * 会生成SCHEDULER类型的命令，由这个处理器负责处理。
 * 
 * 主要功能：
 * 1. 处理SCHEDULER类型的命令
 * 2. 通过SchedulerApi启动工作流定义
 * 3. 继承RunWorkflowCommandHandler的所有功能
 * 4. 专门用于定时触发的工作流执行
 * 
 * 与父类的关系：
 * - 继承自RunWorkflowCommandHandler，复用其工作流启动逻辑
 * - 只是命令类型不同（SCHEDULER vs START_PROCESS）
 * - 处理流程完全一致
 * 
 * 简单理解：就像一个专门处理"定时启动工作流"指令的调度员，
 * 当定时器到时间时，负责启动对应的工作流。
 */
@Component
public class ScheduleWorkflowCommandHandler extends RunWorkflowCommandHandler {

    /**
     * 返回该处理器匹配的命令类型
     * 
     * @return SCHEDULER命令类型，用于定时调度触发的工作流
     */
    @Override
    public CommandType commandType() {
        return CommandType.SCHEDULER;
    }
}
