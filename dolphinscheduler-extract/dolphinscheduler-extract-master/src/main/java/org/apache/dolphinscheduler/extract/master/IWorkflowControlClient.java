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

package org.apache.dolphinscheduler.extract.master;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowBackfillTriggerRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowBackfillTriggerResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstancePauseRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstancePauseResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceRecoverFailureTasksRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceRecoverFailureTasksResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceRecoverSuspendTasksRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceRecoverSuspendTasksResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceRepeatRunningRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceRepeatRunningResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceStopRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowInstanceStopResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowManualTriggerRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowManualTriggerResponse;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowScheduleTriggerRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowScheduleTriggerResponse;

/**
 * 工作流控制客户端接口
 *
 * <p>该接口用于控制工作流实例的执行，提供各种触发和控制操作。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>手动触发工作流执行</li>
 *   <li>补数触发工作流</li>
 *   <li>定时调度触发</li>
 *   <li>重复运行工作流实例</li>
 *   <li>从失败或暂停的任务恢复执行</li>
 *   <li>暂停和停止工作流实例</li>
 * </ul>
 */
@RpcService
public interface IWorkflowControlClient {

    /**
     * 手动触发工作流执行
     *
     * @param workflowManualTriggerRequest 手动触发请求
     * @return 触发响应结果
     */
    @RpcMethod
    WorkflowManualTriggerResponse manualTriggerWorkflow(final WorkflowManualTriggerRequest workflowManualTriggerRequest);

    /**
     * 补数触发工作流
     * 用于补充执行历史数据处理任务
     *
     * @param workflowBackfillTriggerRequest 补数触发请求
     * @return 补数触发响应结果
     */
    @RpcMethod
    WorkflowBackfillTriggerResponse backfillTriggerWorkflow(final WorkflowBackfillTriggerRequest workflowBackfillTriggerRequest);

    /**
     * 定时调度触发工作流
     * 根据调度配置自动触发工作流执行
     *
     * @param workflowScheduleTriggerRequest 调度触发请求
     * @return 调度触发响应结果
     */
    @RpcMethod
    WorkflowScheduleTriggerResponse scheduleTriggerWorkflow(final WorkflowScheduleTriggerRequest workflowScheduleTriggerRequest);

    /**
     * 重复运行工作流实例
     * 重新执行已完成的工作流实例
     *
     * @param workflowInstanceRepeatRunningRequest 重复运行请求
     * @return 重复运行响应结果
     */
    @RpcMethod
    WorkflowInstanceRepeatRunningResponse repeatTriggerWorkflowInstance(final WorkflowInstanceRepeatRunningRequest workflowInstanceRepeatRunningRequest);

    /**
     * 从失败任务恢复执行
     * 从失败的任务节点开始继续执行工作流
     *
     * @param workflowInstanceRecoverFailureTasksRequest 恢复失败任务请求
     * @return 恢复失败任务响应结果
     */
    @RpcMethod
    WorkflowInstanceRecoverFailureTasksResponse triggerFromFailureTasks(final WorkflowInstanceRecoverFailureTasksRequest workflowInstanceRecoverFailureTasksRequest);

    /**
     * 从暂停任务恢复执行
     * 从暂停的任务节点开始继续执行工作流
     *
     * @param workflowInstanceRecoverSuspendTasksRequest 恢复暂停任务请求
     * @return 恢复暂停任务响应结果
     */
    @RpcMethod
    WorkflowInstanceRecoverSuspendTasksResponse triggerFromSuspendTasks(final WorkflowInstanceRecoverSuspendTasksRequest workflowInstanceRecoverSuspendTasksRequest);

    /**
     * 暂停工作流实例
     * 暂时停止工作流执行，可以后续恢复
     *
     * @param workflowInstancePauseRequest 暂停请求
     * @return 暂停响应结果
     */
    @RpcMethod
    WorkflowInstancePauseResponse pauseWorkflowInstance(final WorkflowInstancePauseRequest workflowInstancePauseRequest);

    /**
     * 停止工作流实例
     * 强制终止工作流执行
     *
     * @param workflowInstanceStopRequest 停止请求
     * @return 停止响应结果
     */
    @RpcMethod
    WorkflowInstanceStopResponse stopWorkflowInstance(final WorkflowInstanceStopRequest workflowInstanceStopRequest);
}
