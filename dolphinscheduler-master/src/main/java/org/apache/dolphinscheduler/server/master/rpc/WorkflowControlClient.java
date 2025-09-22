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

package org.apache.dolphinscheduler.server.master.rpc;

import org.apache.dolphinscheduler.extract.master.IWorkflowControlClient;
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
import org.apache.dolphinscheduler.server.master.engine.WorkflowCacheRepository;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowBackfillTrigger;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowInstanceRecoverFailureTaskTrigger;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowInstanceRecoverSuspendTaskTrigger;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowInstanceRepeatTrigger;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowManualTrigger;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowScheduleTrigger;

import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 工作流控制客户端实现类
 *
 * <p>该类是Master节点中负责工作流控制相关操作的RPC服务实现。
 * 作为工作流生命周期管理的核心组件，负责处理各种工作流触发和控制请求。</p>
 *
 * <p>主要功能包括：</p>
 * <ul>
 *   <li>手动触发工作流 - 用户手动启动工作流执行</li>
 *   <li>补数触发工作流 - 为历史时间节点执行工作流</li>
 *   <li>调度触发工作流 - 按照调度规则自动触发</li>
 *   <li>重跑工作流实例 - 重新执行已存在的工作流实例</li>
 *   <li>恢复失败任务 - 从失败的任务开始恢复执行</li>
 *   <li>恢复暂停任务 - 从暂停的任务开始恢复执行</li>
 *   <li>暂停工作流实例 - 暂停正在执行的工作流</li>
 *   <li>停止工作流实例 - 强制终止工作流执行</li>
 * </ul>
 *
 * <p>该类采用委托模式，将具体的工作流操作委托给相应的触发器（Trigger）实现，
 * 本身只负责RPC调用的接收、参数验证和异常处理。</p>
 *
 * @author DolphinScheduler Community
 * @see IWorkflowControlClient
 * @see WorkflowCacheRepository
 */
@Slf4j
@Service
public class WorkflowControlClient implements IWorkflowControlClient {

    /**
     * 工作流手动触发器
     * 负责处理用户手动触发的工作流执行请求
     */
    @Autowired
    private WorkflowManualTrigger workflowManualTrigger;

    /**
     * 工作流补数触发器
     * 负责处理补数执行的工作流请求，即为历史时间节点执行工作流
     */
    @Autowired
    private WorkflowBackfillTrigger workflowBackfillTrigger;

    /**
     * 工作流调度触发器
     * 负责处理按照调度规则自动触发的工作流执行请求
     */
    @Autowired
    private WorkflowScheduleTrigger workflowScheduleTrigger;

    /**
     * 工作流实例重跑触发器
     * 负责处理重新执行已存在工作流实例的请求
     */
    @Autowired
    private WorkflowInstanceRepeatTrigger workflowInstanceRepeatTrigger;

    /**
     * 工作流实例失败任务恢复触发器
     * 负责处理从失败任务开始恢复执行的请求
     */
    @Autowired
    private WorkflowInstanceRecoverFailureTaskTrigger workflowInstanceRecoverFailureTaskTrigger;

    /**
     * 工作流实例暂停任务恢复触发器
     * 负责处理从暂停任务开始恢复执行的请求
     */
    @Autowired
    private WorkflowInstanceRecoverSuspendTaskTrigger workflowInstanceRecoverSuspendTaskTrigger;

    /**
     * 工作流缓存仓库
     * 用于存储和管理当前正在执行的工作流实例的运行时信息
     */
    @Autowired
    private WorkflowCacheRepository workflowRepository;

    /**
     * 手动触发工作流执行
     *
     * <p>接收用户手动触发工作流的请求，并委托给工作流手动触发器进行处理。
     * 手动触发通常用于用户主动启动某个工作流的执行。</p>
     *
     * <p>触发特点：</p>
     * <ul>
     *   <li>由用户主动发起，不受调度规则限制</li>
     *   <li>可以传入自定义参数覆盖默认配置</li>
     *   <li>支持指定执行时间和其他执行选项</li>
     * </ul>
     *
     * @param manualTriggerRequest 手动触发请求，包含工作流定义、执行参数等信息
     * @return WorkflowManualTriggerResponse 手动触发响应，包含触发结果和工作流实例信息
     */
    @Override
    public WorkflowManualTriggerResponse manualTriggerWorkflow(final WorkflowManualTriggerRequest manualTriggerRequest) {
        try {
            return workflowManualTrigger.triggerWorkflow(manualTriggerRequest);
        } catch (Exception ex) {
            log.error("Handle workflowTriggerRequest: {} failed", manualTriggerRequest, ex);
            return WorkflowManualTriggerResponse.fail("Trigger workflow failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 补数触发工作流执行
     *
     * <p>接收补数执行工作流的请求，并委托给工作流补数触发器进行处理。
     * 补数执行用于为历史的某个或多个时间节点执行工作流。</p>
     *
     * <p>补数执行的应用场景：</p>
     * <ul>
     *   <li>数据补充 - 为缺失的历史数据进行补充处理</li>
     *   <li>重新处理 - 对历史数据进行重新处理或修正</li>
     *   <li>批量执行 - 为多个连续的时间节点批量执行工作流</li>
     * </ul>
     *
     * @param backfillTriggerRequest 补数触发请求，包含补数时间范围、执行参数等信息
     * @return WorkflowBackfillTriggerResponse 补数触发响应，包含触发结果和工作流实例信息
     */
    @Override
    public WorkflowBackfillTriggerResponse backfillTriggerWorkflow(final WorkflowBackfillTriggerRequest backfillTriggerRequest) {
        try {
            return workflowBackfillTrigger.triggerWorkflow(backfillTriggerRequest);
        } catch (Exception ex) {
            log.error("Handle workflowBackfillTriggerRequest: {} failed", backfillTriggerRequest, ex);
            return WorkflowBackfillTriggerResponse.fail("Backfill workflow failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 调度触发工作流执行
     *
     * <p>接收按照调度规则触发工作流的请求，并委托给工作流调度触发器进行处理。
     * 调度触发是系统自动根据预先定义的调度规则进行的工作流执行。</p>
     *
     * <p>调度触发的特点：</p>
     * <ul>
     *   <li>按照预定的时间表自动执行</li>
     *   <li>支持Cron表达式定义复杂的调度规则</li>
     *   <li>支持依赖关系和前置条件检查</li>
     *   <li>可配置调度的有效时间范围</li>
     * </ul>
     *
     * @param workflowScheduleTriggerRequest 调度触发请求，包含调度信息、执行时间等
     * @return WorkflowScheduleTriggerResponse 调度触发响应，包含触发结果和工作流实例信息
     */
    @Override
    public WorkflowScheduleTriggerResponse scheduleTriggerWorkflow(final WorkflowScheduleTriggerRequest workflowScheduleTriggerRequest) {
        try {
            return workflowScheduleTrigger.triggerWorkflow(workflowScheduleTriggerRequest);
        } catch (Exception ex) {
            log.error("Handle workflowScheduleTriggerRequest: {} failed", workflowScheduleTriggerRequest, ex);
            return WorkflowScheduleTriggerResponse
                    .fail("Schedule trigger workflow failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 重跑工作流实例
     *
     * <p>接收重新执行已存在工作流实例的请求，并委托给工作流实例重跑触发器进行处理。
     * 重跑功能允许用户对已经执行过的工作流实例进行重新执行。</p>
     *
     * <p>重跑的应用场景：</p>
     * <ul>
     *   <li>数据重处理 - 对已完成的工作流进行重新处理</li>
     *   <li>错误修正 - 修正早期执行中的错误或缺陷</li>
     *   <li>参数调整 - 使用不同的参数重新执行相同的逻辑</li>
     *   <li>测试验证 - 对生产环境的工作流进行验证测试</li>
     * </ul>
     *
     * @param workflowInstanceRepeatRunningRequest 工作流实例重跑请求，包含工作流实例ID等信息
     * @return WorkflowInstanceRepeatRunningResponse 重跑触发响应，包含触发结果和新实例信息
     */
    @Override
    public WorkflowInstanceRepeatRunningResponse repeatTriggerWorkflowInstance(final WorkflowInstanceRepeatRunningRequest workflowInstanceRepeatRunningRequest) {
        try {
            return workflowInstanceRepeatTrigger.triggerWorkflow(workflowInstanceRepeatRunningRequest);
        } catch (Exception ex) {
            log.error("Handle workflowInstanceRepeatRunningRequest: {} failed", workflowInstanceRepeatRunningRequest,
                    ex);
            return WorkflowInstanceRepeatRunningResponse
                    .fail("Repeat trigger workflow instance failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 从失败任务开始恢复工作流执行
     *
     * <p>接收从失败任务开始恢复执行工作流的请求，并委托给失败任务恢复触发器进行处理。
     * 该功能允许用户从工作流中的失败任务节点开始重新执行。</p>
     *
     * <p>恢复执行的特点：</p>
     * <ul>
     *   <li>只恢复执行失败的任务及其后续任务</li>
     *   <li>已成功的任务不会被重新执行</li>
     *   <li>保持原有的任务依赖关系</li>
     *   <li>可以在修复问题后继续执行</li>
     * </ul>
     *
     * <p>应用场景：</p>
     * <ul>
     *   <li>数据处理错误修复后的继续执行</li>
     *   <li>临时性问题（如网络、资源）解决后的恢复</li>
     *   <li>配置错误修正后的重新执行</li>
     * </ul>
     *
     * @param workflowInstanceRecoverFailureTasksRequest 失败任务恢复请求，包含工作流实例ID等信息
     * @return WorkflowInstanceRecoverFailureTasksResponse 失败任务恢复响应，包含恢复结果
     */
    @Override
    public WorkflowInstanceRecoverFailureTasksResponse triggerFromFailureTasks(final WorkflowInstanceRecoverFailureTasksRequest workflowInstanceRecoverFailureTasksRequest) {
        try {
            return workflowInstanceRecoverFailureTaskTrigger
                    .triggerWorkflow(workflowInstanceRecoverFailureTasksRequest);
        } catch (Exception ex) {
            log.error("Handle workflowInstanceRecoverFailureTaskRequest: {} failed",
                    workflowInstanceRecoverFailureTasksRequest, ex);
            return WorkflowInstanceRecoverFailureTasksResponse
                    .fail("Recover failure task failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 从暂停任务开始恢复工作流执行
     *
     * <p>接收从暂停任务开始恢复执行工作流的请求，并委托给暂停任务恢复触发器进行处理。
     * 该功能允许用户从工作流中的暂停任务节点开始重新执行。</p>
     *
     * <p>暂停任务恢复的特点：</p>
     * <ul>
     *   <li>只恢复执行暂停的任务及其后续任务</li>
     *   <li>已成功和已失败的任务不会被重新执行</li>
     *   <li>保持原有的任务依赖关系和数据状态</li>
     *   <li>支持用户主动暂停后的继续执行</li>
     * </ul>
     *
     * <p>应用场景：</p>
     * <ul>
     *   <li>人工干预检查后的继续执行</li>
     *   <li>等待外部资源或条件就绪后的恢复</li>
     *   <li>维护窗口结束后的任务恢复</li>
     *   <li>分阶段执行的工作流控制</li>
     * </ul>
     *
     * @param workflowInstanceRecoverSuspendTasksRequest 暂停任务恢复请求，包含工作流实例ID等信息
     * @return WorkflowInstanceRecoverSuspendTasksResponse 暂停任务恢复响应，包含恢复结果
     */
    @Override
    public WorkflowInstanceRecoverSuspendTasksResponse triggerFromSuspendTasks(WorkflowInstanceRecoverSuspendTasksRequest workflowInstanceRecoverSuspendTasksRequest) {
        try {
            return workflowInstanceRecoverSuspendTaskTrigger
                    .triggerWorkflow(workflowInstanceRecoverSuspendTasksRequest);
        } catch (Exception ex) {
            log.error("Handle workflowInstanceRecoverSuspendTaskRequest: {} failed",
                    workflowInstanceRecoverSuspendTasksRequest, ex);
            return WorkflowInstanceRecoverSuspendTasksResponse
                    .fail("Recover suspend task failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 暂停工作流实例执行
     *
     * <p>接收暂停正在执行的工作流实例的请求。
     * 暂停操作会使工作流停止执行新任务，但已在执行的任务会继续完成。</p>
     *
     * <p>暂停操作的特点：</p>
     * <ul>
     *   <li>是可逆操作，可以通过恢复操作继续执行</li>
     *   <li>已在执行的任务不会被打断，会继续完成</li>
     *   <li>新的任务不会被分发执行</li>
     *   <li>保持工作流的中间状态和数据</li>
     * </ul>
     *
     * <p>应用场景：</p>
     * <ul>
     *   <li>系统维护期间的任务暂停</li>
     *   <li>等待外部条件满足的临时暂停</li>
     *   <li>人工干预和检查的需要</li>
     *   <li>资源不足时的临时暂停</li>
     * </ul>
     *
     * @param workflowInstancePauseRequest 工作流实例暂停请求，包含工作流实例ID
     * @return WorkflowInstancePauseResponse 暂停操作响应，包含操作结果
     */
    @Override
    public WorkflowInstancePauseResponse pauseWorkflowInstance(final WorkflowInstancePauseRequest workflowInstancePauseRequest) {
        try {
            final Integer workflowInstanceId = workflowInstancePauseRequest.getWorkflowInstanceId();
            final IWorkflowExecutionRunnable workflow = workflowRepository.get(workflowInstanceId);
            if (workflow == null) {
                return WorkflowInstancePauseResponse.fail(
                        "Cannot find the WorkflowExecuteRunnable: " + workflowInstanceId);
            }
            workflow.pause();
            return WorkflowInstancePauseResponse.success();
        } catch (Exception ex) {
            log.error("Handle workflowInstancePauseRequest: {} failed", workflowInstancePauseRequest, ex);
            return WorkflowInstancePauseResponse.fail(
                    "Pause workflow instance failed: " + ExceptionUtils.getMessage(ex));
        }
    }

    /**
     * 停止工作流实例执行
     *
     * <p>接收强制终止正在执行的工作流实例的请求。
     * 停止操作会立即终止工作流的执行，包括正在运行的任务。</p>
     *
     * <p>停止操作的特点：</p>
     * <ul>
     *   <li>是不可逆操作，停止后无法直接恢复执行</li>
     *   <li>所有正在执行的任务都会被强制终止</li>
     *   <li>等待执行的任务不会被分发</li>
     *   <li>工作流进入终止状态，需要重新触发才能执行</li>
     * </ul>
     *
     * <p>应用场景：</p>
     * <ul>
     *   <li>工作流出现不可恢复的错误需要终止</li>
     *   <li>用户主动取消不再需要的工作流</li>
     *   <li>系统紧急状态下的快速停止</li>
     *   <li>资源紧张时的优先级调整</li>
     * </ul>
     *
     * @param workflowInstanceStopRequest 工作流实例停止请求，包含工作流实例ID
     * @return WorkflowInstanceStopResponse 停止操作响应，包含操作结果
     */
    @Override
    public WorkflowInstanceStopResponse stopWorkflowInstance(final WorkflowInstanceStopRequest workflowInstanceStopRequest) {
        try {
            final Integer workflowInstanceId = workflowInstanceStopRequest.getWorkflowInstanceId();
            final IWorkflowExecutionRunnable workflow = workflowRepository.get(workflowInstanceId);
            if (workflow == null) {
                return WorkflowInstanceStopResponse
                        .fail("Cannot find the WorkflowExecuteRunnable: " + workflowInstanceId);
            }
            workflow.stop();
            return WorkflowInstanceStopResponse.success();
        } catch (Exception ex) {
            log.error("Handle workflowInstanceStopRequest: {} failed", workflowInstanceStopRequest, ex);
            return WorkflowInstanceStopResponse.fail(
                    "Stop workflow instance failed:" + ExceptionUtils.getMessage(ex));
        }
    }
}
