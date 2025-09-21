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

package org.apache.dolphinscheduler.task.executor.eventbus;

import org.apache.dolphinscheduler.task.executor.events.IReportableTaskExecutorLifecycleEvent;

/**
 * 任务执行器事件远程报告客户端接口
 *
 * <p>负责将任务执行器的生命周期事件报告给Master节点。
 * 这是Worker与Master之间通信的重要组件，确保Master能够实时了解任务执行状态。
 */
public interface ITaskExecutorEventRemoteReporterClient {

    /**
     * 向Master报告任务执行事件
     *
     * <p>将可报告的任务执行器生命周期事件发送给指定的Master节点。
     * 这个方法通常通过RPC调用实现远程通信。
     *
     * @param masterAddress Master节点地址，格式通常为 "host:port"
     * @param reportableTaskExecutorLifecycleEvent 可报告的任务执行器生命周期事件
     */
    void reportTaskExecutionEventToMaster(final String masterAddress,
                                          final IReportableTaskExecutorLifecycleEvent reportableTaskExecutorLifecycleEvent);
}
