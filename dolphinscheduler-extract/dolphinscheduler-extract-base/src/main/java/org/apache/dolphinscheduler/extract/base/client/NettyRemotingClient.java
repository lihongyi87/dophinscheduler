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

package org.apache.dolphinscheduler.extract.base.client;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.extract.base.IRpcResponse;
import org.apache.dolphinscheduler.extract.base.RpcMethodRetryStrategy;
import org.apache.dolphinscheduler.extract.base.SyncRequestDto;
import org.apache.dolphinscheduler.extract.base.config.NettyClientConfig;
import org.apache.dolphinscheduler.extract.base.exception.RemoteException;
import org.apache.dolphinscheduler.extract.base.exception.RemoteTimeoutException;
import org.apache.dolphinscheduler.extract.base.future.ResponseFuture;
import org.apache.dolphinscheduler.extract.base.metrics.ClientSyncDurationMetrics;
import org.apache.dolphinscheduler.extract.base.metrics.ClientSyncExceptionMetrics;
import org.apache.dolphinscheduler.extract.base.metrics.RpcMetrics;
import org.apache.dolphinscheduler.extract.base.protocal.Transporter;
import org.apache.dolphinscheduler.extract.base.protocal.TransporterDecoder;
import org.apache.dolphinscheduler.extract.base.protocal.TransporterEncoder;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.base.utils.NettyUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import lombok.extern.slf4j.Slf4j;
import io.netty.bootstrap.Bootstrap;
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
 * Netty远程通信客户端
 *
 * <p>该类使用Netty框架实现了RPC客户端的网络通信功能。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>管理与服务端的网络连接</li>
 *   <li>发送RPC请求并接收响应</li>
 *   <li>连接池管理和复用</li>
 *   <li>自动重连和心跳检测</li>
 *   <li>请求超时和重试机制</li>
 * </ul>
 */
@Slf4j
public class NettyRemotingClient implements AutoCloseable {

    /**
     * Netty引导类，用于创建和配置客户端通道
     */
    private final Bootstrap bootstrap = new Bootstrap();

    /**
     * 通道锁，用于保护通道集合的并发访问
     */
    private final ReentrantLock channelsLock = new ReentrantLock();

    /**
     * 通道集合，缓存与各个服务端的连接
     * key: 服务端主机地址，value: Netty通道
     */
    private final Map<Host, Channel> channels = new ConcurrentHashMap<>();

    /**
     * 启动状态标识，确保客户端只启动一次
     */
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    /**
     * 工作线程组，用于处理I/O事件
     */
    private final EventLoopGroup workerGroup;

    /**
     * 客户端配置
     */
    private final NettyClientConfig clientConfig;

    /**
     * 客户端处理器，负责处理网络事件
     */
    private final NettyClientHandler clientHandler;

    /**
     * 构造函数，初始化Netty客户端
     *
     * @param clientConfig 客户端配置
     */
    public NettyRemotingClient(final NettyClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        // 创建守护线程工厂
        ThreadFactory nettyClientThreadFactory = ThreadUtils.newDaemonThreadFactory("NettyClientThread-");
        // 根据系统支持情况选择不同的EventLoopGroup
        if (Epoll.isAvailable()) {
            // Linux系统下使用Epoll以获得更高性能
            this.workerGroup = new EpollEventLoopGroup(clientConfig.getWorkerThreads(), nettyClientThreadFactory);
        } else {
            // 其他系统使用NIO
            this.workerGroup = new NioEventLoopGroup(clientConfig.getWorkerThreads(), nettyClientThreadFactory);
        }
        // 初始化客户端处理器
        this.clientHandler = new NettyClientHandler(this);

        // 启动客户端
        this.start();
    }

    /**
     * 启动客户端，配置Netty Bootstrap
     */
    private void start() {
        // 配置Bootstrap
        this.bootstrap
                // 设置工作线程组
                .group(this.workerGroup)
                // 设置通道类型
                .channel(NettyUtils.getSocketChannelClass())
                // 启用TCP保活机制
                .option(ChannelOption.SO_KEEPALIVE, clientConfig.isSoKeepalive())
                // 禁用Nagle算法，减小延迟
                .option(ChannelOption.TCP_NODELAY, clientConfig.isTcpNoDelay())
                // 设置发送缓冲区大小
                .option(ChannelOption.SO_SNDBUF, clientConfig.getSendBufferSize())
                // 设置接收缓冲区大小
                .option(ChannelOption.SO_RCVBUF, clientConfig.getReceiveBufferSize())
                // 设置连接超时时间
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, clientConfig.getConnectTimeoutMillis())
                // 设置通道初始化处理器
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) {
                        // 配置通道管道
                        ch.pipeline()
                                // 添加空闲检测处理器，用于心跳检测
                                .addLast("client-idle-handler",
                                        new IdleStateHandler(
                                                0, // 读空闲时间，0表示不检测
                                                clientConfig.getHeartBeatIntervalMillis(), // 写空闲时间，用于发送心跳
                                                0, // 读写空闲时间
                                                TimeUnit.MILLISECONDS))
                                // 添加解码器、处理器和编码器
                                .addLast(new TransporterDecoder(), clientHandler, new TransporterEncoder());
                    }
                });
        // 标记客户端已启动
        isStarted.compareAndSet(false, true);
    }

    /**
     * 同步发送RPC请求
     *
     * @param syncRequestDto 同步请求DTO，包含请求信息和配置
     * @return RPC响应
     * @throws RemoteException 远程调用异常
     */
    public IRpcResponse sendSync(final SyncRequestDto syncRequestDto) throws RemoteException {
        // 获取服务器主机地址
        final Host host = syncRequestDto.getServerHost();
        // 获取传输对象
        final Transporter transporter = syncRequestDto.getTransporter();
        // 获取超时时间，如果未设置则使用默认值
        final long timeoutMillis = syncRequestDto.getTimeoutMillis() < 0 ? clientConfig.getDefaultRpcTimeoutMillis()
                : syncRequestDto.getTimeoutMillis();

        // 获取重试策略
        final RpcMethodRetryStrategy retryStrategy = syncRequestDto.getRetryStrategy();

        // 获取最大重试次数
        int maxRetryTimes = retryStrategy.maxRetryTimes();
        // 记录当前执行次数
        int currentExecuteTimes = 1;

        // 重试循环
        while (true) {
            final long start = System.currentTimeMillis();
            try {
                // 尝试发送同步请求
                return doSendSync(transporter, host, timeoutMillis);
            } catch (Exception ex) {
                // 记录异常指标
                ClientSyncExceptionMetrics clientSyncExceptionMetrics =
                        ClientSyncExceptionMetrics.of(syncRequestDto, ex);
                RpcMetrics.recordClientSyncRequestException(clientSyncExceptionMetrics);

                // 判断是否可以重试
                if (currentExecuteTimes < maxRetryTimes
                        && Arrays.stream(retryStrategy.retryFor()).anyMatch(e -> e.isInstance(ex))) {
                    // 增加执行次数
                    currentExecuteTimes++;
                    // 如果设置了重试间隔，等待一段时间
                    if (retryStrategy.retryInterval() > 0) {
                        ThreadUtils.sleep(retryStrategy.retryInterval());
                    }
                    continue;
                }

                if (ex instanceof RemoteException) {
                    throw (RemoteException) ex;
                } else {
                    throw new RemoteException("Call method to " + host + " failed", ex);
                }
            } finally {
                ClientSyncDurationMetrics clientSyncDurationMetrics = ClientSyncDurationMetrics
                        .of(syncRequestDto)
                        .withMilliseconds(System.currentTimeMillis() - start);
                RpcMetrics.recordClientSyncRequestDuration(clientSyncDurationMetrics);
            }
        }
    }

    private IRpcResponse doSendSync(final Transporter transporter,
                                    final Host serverHost,
                                    long timeoutMills) throws RemoteException, InterruptedException {
        final Channel channel = getOrCreateChannel(serverHost);
        if (channel == null) {
            throw new RemoteException(String.format("connect to : %s fail", serverHost));
        }
        final ResponseFuture responseFuture = new ResponseFuture(transporter.getHeader().getOpaque(), timeoutMills);
        channel.writeAndFlush(transporter).addListener(future -> {
            if (future.isSuccess()) {
                responseFuture.setSendOk(true);
                return;
            } else {
                responseFuture.setSendOk(false);
            }
            responseFuture.setCause(future.cause());
            responseFuture.putResponse(null);
            log.error("Send Sync request {} to host {} failed", transporter, serverHost, responseFuture.getCause());
        });
        /*
         * sync wait for result
         */
        final IRpcResponse iRpcResponse = responseFuture.waitResponse();
        if (iRpcResponse != null) {
            return iRpcResponse;
        }
        if (responseFuture.isSendOK()) {
            throw new RemoteTimeoutException(serverHost.toString(), timeoutMills, responseFuture.getCause());
        } else {
            throw new RemoteException(serverHost.toString(), responseFuture.getCause());
        }
    }

    Channel getOrCreateChannel(Host host) {
        Channel channel = channels.get(host);
        if (channel != null && channel.isActive()) {
            return channel;
        }
        try {
            channelsLock.lock();
            channel = channels.get(host);
            if (channel != null && channel.isActive()) {
                return channel;
            }
            channel = createChannel(host);
            channels.put(host, channel);
        } finally {
            channelsLock.unlock();
        }
        return channel;
    }

    /**
     * create channel
     *
     * @param host host
     * @return channel
     */
    Channel createChannel(Host host) {
        try {
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(host.getIp(), host.getPort()));
            future = future.sync();
            if (future.isSuccess()) {
                return future.channel();
            } else {
                throw new IllegalArgumentException("connect to host: " + host + " failed", future.cause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Connect to host: " + host + " failed", e);
        }
    }

    @Override
    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                closeChannels();
                if (workerGroup != null) {
                    this.workerGroup.shutdownGracefully();
                }
                log.info("netty client closed");
            } catch (Exception ex) {
                log.error("netty client close exception", ex);
            }
        }
    }

    private void closeChannels() {
        try {
            channelsLock.lock();
            channels.values().forEach(Channel::close);
            channels.clear();
        } finally {
            channelsLock.unlock();
        }
    }

    public void onChannelInactive(final Host host) {
        channels.remove(host);
    }
}
