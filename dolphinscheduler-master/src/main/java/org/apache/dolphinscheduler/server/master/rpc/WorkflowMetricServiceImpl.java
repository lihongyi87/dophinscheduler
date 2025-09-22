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

import org.apache.dolphinscheduler.extract.master.IWorkflowMetricService;
import org.apache.dolphinscheduler.server.master.metrics.WorkflowInstanceMetrics;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

/**
 * 工作流指标服务实现类
 *
 * <p>该类是Master节点中负责工作流指标管理相关操作的RPC服务实现。
 * 主要处理与工作流运行指标的清理、维护相关的远程调用请求。</p>
 *
 * <p>作为系统监控和指标管理的重要组件，该类负责：</p>
 * <ul>
 *   <li>清理工作流定义相关的指标数据</li>
 *   <li>维护系统指标数据的完整性</li>
 *   <li>防止指标数据的内存泄漏</li>
 *   <li>支持工作流定义删除时的数据清理</li>
 * </ul>
 *
 * <p>该服务通过RPC接口接收来自其他节点（如API节点）的指标管理请求，
 * 确保当工作流定义发生变更或删除时，相关的指标数据能够被及时清理，
 * 避免内存占用过多和数据不一致的问题。</p>
 *
 * @author DolphinScheduler Community
 * @see IWorkflowMetricService
 * @see WorkflowInstanceMetrics
 */
@Slf4j
@Service
public class WorkflowMetricServiceImpl implements IWorkflowMetricService {

    /**
     * 清理工作流指标数据
     *
     * <p>接收清理指定工作流定义相关指标数据的请求，并委托给
     * WorkflowInstanceMetrics进行实际的指标数据清理操作。</p>
     *
     * <p>该方法主要用于在以下场景中清理指标数据：</p>
     * <ul>
     *   <li>工作流定义被删除时，清理相关的实例计数指标</li>
     *   <li>工作流定义发生重大变更时，重置相关指标</li>
     *   <li>系统维护期间的指标数据清理</li>
     *   <li>防止长期运行导致的指标数据堆积</li>
     * </ul>
     *
     * <p>清理操作包括：</p>
     * <ul>
     *   <li>清理工作流实例计数相关的Prometheus指标</li>
     *   <li>释放相关的内存资源</li>
     *   <li>确保指标数据的一致性</li>
     * </ul>
     *
     * @param workflowDefinitionCode 工作流定义编码，用于标识要清理指标数据的工作流定义
     */
    @Override
    public void clearWorkflowMetrics(Long workflowDefinitionCode) {
        log.info("Receive clearWorkflowMetrics request: {}", workflowDefinitionCode);
        WorkflowInstanceMetrics.cleanUpWorkflowInstanceCountMetricsByDefinitionCode(workflowDefinitionCode);
    }

}
