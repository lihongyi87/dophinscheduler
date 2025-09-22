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

import org.apache.dolphinscheduler.dao.entity.Command;

/**
 * 命令重复处理异常
 *
 * 在多Master集群环境中，当多个Master节点同时尝试处理同一个命令时抛出此异常。
 * 这是一种正常的竞争情况，而不是系统错误。
 *
 * 产生场景：
 * 1. 网络分区恢复后：Master节点重新加入集群时可能重复处理命令
 * 2. 槽位重新分配：Master节点数量变化导致命令归属权变更
 * 3. 时钟同步问题：不同Master节点的系统时间不一致
 * 4. 数据库并发：事务隔离级别导致的并发读取
 *
 * 处理策略：
 * - 此异常应被视为警告而非错误
 * - 不需要重试或报警
 * - 记录日志用于调试和监控
 *
 * 类比：就像两个人同时去取同一个快递包裹，
 * 快递员告诉后到的人"包裹已经被取走了"。
 */
public class CommandDuplicateHandleException extends RuntimeException {

    /**
     * 构造命令重复处理异常
     *
     * @param command 已被其他Master处理的命令对象
     */
    public CommandDuplicateHandleException(Command command) {
        // ==========构造异常消息==========
        // 包含命令ID信息，便于定位具体的重复处理命令
        super("The command: " + command.getId() + " has already been handled");
    }

}
