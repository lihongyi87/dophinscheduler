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

import org.apache.dolphinscheduler.extract.base.config.NettyServerConfig;
import org.apache.dolphinscheduler.extract.base.server.SpringServerMethodInvokerDiscovery;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * Master RPC服务器
 *
 * <p>该类是Master节点的RPC服务器实现，基于Netty框架构建，
 * 负责处理来自其他节点（Worker、API等）的远程调用请求。</p>
 *
 * <p>作为Master节点对外提供服务的核心组件，该服务器：</p>
 * <ul>
 *   <li>继承SpringServerMethodInvokerDiscovery，支持基于Spring注解的服务发现</li>
 *   <li>实现AutoCloseable接口，支持优雅关闭</li>
 *   <li>使用Netty作为底层网络通信框架</li>
 *   <li>支持高并发、低延迟的RPC通信</li>
 * </ul>
 *
 * <p>服务器启动流程：</p>
 * <ol>
 *   <li>读取Master配置中的监听端口</li>
 *   <li>创建NettyServerConfig配置对象</li>
 *   <li>初始化Netty服务器</li>
 *   <li>注册服务方法调用器</li>
 *   <li>开始监听客户端连接</li>
 * </ol>
 *
 * <p>该服务器通过反射机制自动发现并注册Spring容器中所有标注了
 * 相应注解的RPC服务实现类，如LogicTaskExecutorOperatorImpl、
 * MasterContainerService等。</p>
 *
 * @author DolphinScheduler Community
 * @see SpringServerMethodInvokerDiscovery
 * @see NettyServerConfig
 * @see MasterConfig
 */
@Component
@Slf4j
public class MasterRpcServer extends SpringServerMethodInvokerDiscovery implements AutoCloseable {

    /**
     * 构造Master RPC服务器
     *
     * <p>根据Master配置初始化RPC服务器，设置服务器名称和监听端口。
     * 服务器名称用于标识和日志记录，监听端口用于接收客户端连接。</p>
     *
     * @param masterConfig Master节点配置，包含RPC服务器的监听端口等配置信息
     */
    public MasterRpcServer(MasterConfig masterConfig) {
        super(NettyServerConfig.builder().serverName("MasterRpcServer").listenPort(masterConfig.getListenPort())
                .build());
    }

}
