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

package org.apache.dolphinscheduler.server.worker.rpc;

import org.apache.dolphinscheduler.extract.base.config.NettyServerConfig;
import org.apache.dolphinscheduler.extract.base.server.SpringServerMethodInvokerDiscovery;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;

import java.io.Closeable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

/**
 * Worker RPC服务器
 *
 * <p>该类是Worker节点的RPC服务器实现，负责启动和管理Worker节点的网络通信服务。
 * 继承自SpringServerMethodInvokerDiscovery，提供基于Netty的RPC服务功能。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>启动Worker节点的RPC服务端口</li>
 *   <li>自动发现和注册Spring容器中的RPC服务方法</li>
 *   <li>处理Master节点和其他组件的RPC请求</li>
 *   <li>管理网络连接和资源</li>
 *   <li>提供服务器生命周期管理</li>
 * </ul>
 *
 * <p>该服务器通过配置的监听端口接收外部RPC请求，并将请求路由到相应的
 * 服务实现类，如PhysicalTaskExecutorOperatorImpl、StreamingTaskInstanceOperatorImpl等。</p>
 *
 * <p>服务器配置：</p>
 * <ul>
 *   <li>服务器名称：WorkerRpcServer</li>
 *   <li>监听端口：从WorkerConfig配置中获取</li>
 *   <li>协议：基于Netty的自定义RPC协议</li>
 * </ul>
 *
 * @see SpringServerMethodInvokerDiscovery
 * @see WorkerConfig
 * @see NettyServerConfig
 */
@Slf4j
@Service
public class WorkerRpcServer extends SpringServerMethodInvokerDiscovery implements Closeable {

    /**
     * 构造Worker RPC服务器
     *
     * <p>根据Worker配置创建RPC服务器实例，设置服务器名称和监听端口。
     * 服务器将在指定端口上启动Netty服务，等待外部RPC请求。</p>
     *
     * @param workerConfig Worker节点配置，包含监听端口等信息
     */
    public WorkerRpcServer(WorkerConfig workerConfig) {
        // 调用父类构造函数，初始化Netty服务器配置
        // 使用建造者模式创建NettyServerConfig对象
        super(NettyServerConfig.builder()
                // 设置服务器名称，用于日志和监控标识
                .serverName("WorkerRpcServer")
                // 从配置中获取监听端口，设置RPC服务器的网络监听端口
                .listenPort(workerConfig.getListenPort())
                // 构建配置对象
                .build());
    }

}
