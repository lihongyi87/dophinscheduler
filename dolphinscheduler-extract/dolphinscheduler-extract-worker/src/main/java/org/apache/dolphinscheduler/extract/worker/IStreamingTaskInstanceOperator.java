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

package org.apache.dolphinscheduler.extract.worker;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceTriggerSavepointRequest;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceTriggerSavepointResponse;

/**
 * 流式任务实例操作接口
 *
 * <p>该接口定义了流式任务实例的操作方法。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>触发保存点操作</li>
 *   <li>管理流式任务的状态保存</li>
 *   <li>支持流式任务的容错和恢复</li>
 * </ul>
 */
@RpcService
public interface IStreamingTaskInstanceOperator {

    /**
     * 触发保存点
     * 创建流式任务的保存点，用于故障恢复或任务重启
     *
     * @param taskInstanceTriggerSavepointRequest 触发保存点请求，包含任务实例信息
     * @return 触发保存点响应，包含保存点创建结果
     */
    @RpcMethod
    TaskInstanceTriggerSavepointResponse triggerSavepoint(TaskInstanceTriggerSavepointRequest taskInstanceTriggerSavepointRequest);

}
