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

package org.apache.dolphinscheduler.extract.base.server;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.extract.base.config.NettyServerConfig;
import org.apache.dolphinscheduler.extract.base.exception.RemoteException;
import org.apache.dolphinscheduler.extract.base.protocal.TransporterDecoder;
import org.apache.dolphinscheduler.extract.base.protocal.TransporterEncoder;
import org.apache.dolphinscheduler.extract.base.utils.NettyUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Netty远程通信服务器
 *
 * <p>该类使用Netty框架实现了RPC服务器端的网络通信功能。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>监听指定端口，接收客户端连接</li>
 *   <li>处理RPC请求并返回响应</li>
 *   <li>管理服务器端的生命周期</li>
 *   <li>提供方法调用执行器的并发执行</li>
 *   <li>心跳检测和连接管理</li>
 * </ul>
 */
@Slf4j
class NettyRemotingServer {

    /**
     * 服务器通道，用于监听端口
     */
    private Channel serverBootstrapChannel;

    /**
     * 服务器名称
     */
    @Getter
    private final String serverName;

    /**
     * 方法调用执行器，用于异步执行RPC方法
     */
    @Getter
    private final ExecutorService methodInvokerExecutor;

    /**
     * Boss线程组，负责接收连接
     */
    private final EventLoopGroup bossGroup;

    /**
     * Worker线程组，负责处理I/O事件
     */
    private final EventLoopGroup workGroup;

    /**
     * 服务器配置
     */
    private final NettyServerConfig serverConfig;

    /**
     * 通道处理器，处理RPC请求
     */
    private final JdkDynamicServerHandler channelHandler;

    /**
     * 启动状态标识
     */
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    /**
     * 构造函数，初始化Netty服务器
     *
     * @param serverConfig 服务器配置
     */
    NettyRemotingServer(final NettyServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.serverName = serverConfig.getServerName();
        // 创建方法调用执行器，线程数为CPU核心数的2倍+1
        this.methodInvokerExecutor = ThreadUtils.newDaemonFixedThreadExecutor(
                serverName + "-methodInvoker-%d", Runtime.getRuntime().availableProcessors() * 2 + 1);
        // 创建通道处理器
        this.channelHandler = new JdkDynamicServerHandler(methodInvokerExecutor);
        // 创建Boss线程工厂
        ThreadFactory bossThreadFactory =
                ThreadUtils.newDaemonThreadFactory(serverName + "-boss-%d");
        // 创建Worker线程工厂
        ThreadFactory workerThreadFactory =
                ThreadUtils.newDaemonThreadFactory(serverName + "-worker-%d");
        // 根据系统支持情况选择EventLoopGroup
        if (Epoll.isAvailable()) {
            // Linux系统使用Epoll
            this.bossGroup = new EpollEventLoopGroup(1, bossThreadFactory);
            this.workGroup = new EpollEventLoopGroup(serverConfig.getWorkerThread(), workerThreadFactory);
        } else {
            // 其他系统使用NIO
            this.bossGroup = new NioEventLoopGroup(1, bossThreadFactory);
            this.workGroup = new NioEventLoopGroup(serverConfig.getWorkerThread(), workerThreadFactory);
        }
    }

    /**
     * 启动服务器
     */
    void start() {
        // 确保只启动一次
        if (isStarted.compareAndSet(false, true)) {
            // 创建服务器引导类
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    // 设置boss和worker线程组
                    .group(this.bossGroup, this.workGroup)
                    // 设置通道类型
                    .channel(NettyUtils.getServerSocketChannelClass())
                    // 允许端口重用
                    .option(ChannelOption.SO_REUSEADDR, true)
                    // 设置连接队列长度
                    .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog())
                    // 启用TCP保活机制
                    .childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isSoKeepalive())
                    // 禁用Nagle算法
                    .childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpNoDelay())
                    // 设置发送缓冲区大小
                    .childOption(ChannelOption.SO_SNDBUF, serverConfig.getSendBufferSize())
                    // 设置接收缓冲区大小
                    .childOption(ChannelOption.SO_RCVBUF, serverConfig.getReceiveBufferSize())
                    // 设置子通道初始化器
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 初始化通道
                            initNettyChannel(ch);
                        }
                    });

            try {
                // 绑定端口并同步等待结果
                final ChannelFuture channelFuture = serverBootstrap.bind(serverConfig.getListenPort()).sync();
                if (channelFuture.isSuccess()) {
                    // 绑定成功，记录日志
                    log.info("{} bind success at port: {}", serverConfig.getServerName(), serverConfig.getListenPort());
                    // 保存服务器通道
                    this.serverBootstrapChannel = channelFuture.channel();
                } else {
                    // 绑定失败，抛出异常
                    throw new RemoteException(
                            String.format("%s bind %s fail", serverConfig.getServerName(),
                                    serverConfig.getListenPort()),
                            channelFuture.cause());
                }
            } catch (InterruptedException it) {
                ThreadUtils.rethrowInterruptedException(it);
            } catch (Exception e) {
                throw new RemoteException(
                        String.format("%s bind %s fail", serverConfig.getServerName(), serverConfig.getListenPort()),
                        e);
            }
        }
    }

    /**
     * 初始化Netty通道
     * 配置通道管道中的各种处理器
     *
     * @param ch Socket通道
     */
    private void initNettyChannel(SocketChannel ch) {
        ch.pipeline()
                // 添加编码器，将对象转换为字节流
                .addLast("encoder", new TransporterEncoder())
                // 添加解码器，将字节流转换为对象
                .addLast("decoder", new TransporterDecoder())
                // 添加空闲检测器，用于检测长时间不活动的连接
                .addLast("server-idle-handle",
                        new IdleStateHandler(serverConfig.getConnectionIdleTime(), 0, 0, TimeUnit.MILLISECONDS))
                // 添加业务处理器
                .addLast("handler", channelHandler);
    }

    /**
     * 注册方法调用器
     *
     * @param methodInvoker 方法调用器
     */
    void registerMethodInvoker(ServerMethodInvoker methodInvoker) {
        channelHandler.registerMethodInvoker(methodInvoker);
    }

    /**
     * 关闭服务器
     */
    void close() {
        // 确保只关闭一次
        if (isStarted.compareAndSet(true, false)) {
            log.info("{} closing", serverConfig.getServerName());
            try {
                if (serverBootstrapChannel != null) {
                    serverBootstrapChannel.close().sync();
                    log.info("{} stop bind at port: {}", serverConfig.getServerName(), serverConfig.getListenPort());
                }
                if (bossGroup != null) {
                    this.bossGroup.shutdownGracefully();
                }
                if (workGroup != null) {
                    this.workGroup.shutdownGracefully();
                }
                methodInvokerExecutor.shutdownNow();
            } catch (InterruptedException it) {
                ThreadUtils.consumeInterruptedException(it);
            } catch (Exception ex) {
                log.error("{} close failed", serverConfig.getServerName(), ex);
            }
            log.info("{} closed", serverConfig.getServerName());
        }
    }
}
