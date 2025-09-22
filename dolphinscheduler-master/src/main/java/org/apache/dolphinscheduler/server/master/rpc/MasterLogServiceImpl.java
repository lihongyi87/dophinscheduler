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

import org.apache.dolphinscheduler.extract.common.ILogService;
import org.apache.dolphinscheduler.extract.common.service.impl.LogServiceImpl;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

/**
 * Master日志服务实现类
 *
 * <p>该类是Master节点中负责日志相关操作的RPC服务实现，
 * 继承自通用的LogServiceImpl并实现ILogService接口。</p>
 *
 * <p>作为Master节点的日志服务，该类主要负责处理与日志查询、
 * 日志获取相关的远程调用请求。虽然当前实现较为简单，
 * 但为未来扩展Master特有的日志处理功能提供了基础。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>继承通用日志服务的所有功能</li>
 *   <li>为Master节点提供日志服务的RPC接口</li>
 *   <li>支持任务执行日志的查询和获取</li>
 *   <li>提供日志文件的远程访问能力</li>
 * </ul>
 *
 * <p>该服务通过RPC机制，允许其他节点（如API节点）
 * 远程访问Master节点上的日志信息，是分布式日志管理的重要组成部分。</p>
 *
 * @author DolphinScheduler Community
 * @see ILogService
 * @see LogServiceImpl
 */
@Slf4j
@Service
public class MasterLogServiceImpl extends LogServiceImpl implements ILogService {

}
