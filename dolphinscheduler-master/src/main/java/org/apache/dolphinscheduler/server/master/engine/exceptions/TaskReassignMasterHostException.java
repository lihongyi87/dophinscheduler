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
 * 任务重新分配Master主机异常
 *
 * 当任务需要重新分配到其他Master节点处理时抛出的异常。
 * 这通常发生在故障转移（Failover）或负载重新平衡的场景中。
 *
 * 触发场景：
 * 1. Master节点故障：原处理任务的Master节点宕机或网络不可达
 * 2. 负载重新平衡：集群负载不均时主动迁移任务
 * 3. 扩容缩容：集群规模变化时重新分配任务
 * 4. 维护升级：Master节点维护时迁移其处理的任务
 * 5. 资源约束：某个Master节点资源不足时迁移任务
 *
 * 处理策略：
 * - 停止当前Master对任务的处理
 * - 更新任务的归属Master信息
 * - 通知目标Master接管任务
 * - 确保任务状态的一致性转移
 *
 * 类比：就像货运调度中心重新安排货车司机，
 * 当某个司机无法继续工作时，将其负责的货物转交给其他司机。
 */
public class TaskReassignMasterHostException extends RuntimeException {

    /**
     * 构造任务重新分配Master主机异常
     *
     * @param message 异常详细信息，说明重新分配的原因
     */
    public TaskReassignMasterHostException(String message) {
        super(message);
    }

    /**
     * 构造任务重新分配Master主机异常（带原因链）
     *
     * @param message 异常详细信息，说明重新分配的原因
     * @param cause 引起此异常的底层异常，用于异常链追踪
     */
    public TaskReassignMasterHostException(String message, Throwable cause) {
        super(message, cause);
    }

}
