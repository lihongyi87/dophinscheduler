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

package org.apache.dolphinscheduler.server.master.engine.command;

import org.apache.dolphinscheduler.dao.entity.Command;

import java.util.List;

/**
 * 命令获取器接口
 *
 * 用于从数据库中获取待处理的命令，支持多种获取策略。
 * 不同的实现类可以采用不同的获取方式，如基于槽位、基于限制等。
 *
 * 类比：就像一个任务分发器，负责从任务队列中取出需要处理的任务。
 */
public interface ICommandFetcher {

    /**
     * 获取待处理的命令列表
     *
     * 从数据库中批量获取当前Master节点需要处理的命令。
     * 具体的获取策略由实现类决定，可能包括：
     * - 基于槽位的获取：根据Master的槽位获取特定范围的命令
     * - 基于限制的获取：每次获取固定数量的命令
     * - 基于优先级的获取：优先获取高优先级的命令
     *
     * @return 需要处理的命令列表，可能为空但不为null
     */
    List<Command> fetchCommands();

}
