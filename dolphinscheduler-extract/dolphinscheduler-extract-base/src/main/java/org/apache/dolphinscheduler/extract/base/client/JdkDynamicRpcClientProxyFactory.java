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

import org.apache.dolphinscheduler.extract.base.utils.Host;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

/**
 * JDK动态RPC客户端代理工厂
 *
 * <p>该类使用JDK动态代理机制创建RPC客户端代理，将本地方法调用转换为远程调用。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建基于JDK动态代理的RPC客户端</li>
 *   <li>缓存代理客户端实例，避免重复创建</li>
 *   <li>自动清理长时间未使用的代理客户端</li>
 *   <li>支持多个服务器主机的代理管理</li>
 * </ul>
 */
@Slf4j
class JdkDynamicRpcClientProxyFactory implements IRpcClientProxyFactory {

    /**
     * Netty远程通信客户端
     */
    private final NettyRemotingClient nettyRemotingClient;

    /**
     * 代理客户端缓存
     * key: 服务器主机地址，value: 接口名称到代理实例的映射
     */
    private static final LoadingCache<String, Map<String, Object>> proxyClientCache = CacheBuilder.newBuilder()
            // 设置访问过期时间，清理长时间未使用的死亡主机
            // 安全地移除死亡主机，因为需要时会重新创建客户端
            // 客户端只是个代理，不会持有任何资源
            .expireAfterAccess(Duration.ofHours(1))
            .removalListener((RemovalListener<String, Map<String, Object>>) notification -> {
                // 记录缓存移除日志
                log.warn("Remove DynamicRpcClientProxy cache for host: {}", notification.getKey());
                // 清理缓存映射
                notification.getValue().clear();
            })
            .build(new CacheLoader<String, Map<String, Object>>() {

                @Override
                public Map<String, Object> load(String host) {
                    // 为新主机创建缓存映射
                    log.info("Create DynamicRpcClientProxy cache for host: {}", host);
                    // 返回线程安全的Map实现
                    return new ConcurrentHashMap<>();
                }
            });

    /**
     * 构造函数
     *
     * @param nettyRemotingClient Netty远程通信客户端
     */
    JdkDynamicRpcClientProxyFactory(NettyRemotingClient nettyRemotingClient) {
        this.nettyRemotingClient = nettyRemotingClient;
    }

    /**
     * 获取代理客户端
     * 从缓存中获取或创建新的代理客户端
     *
     * @param serverHost 服务器主机地址
     * @param clientInterface 客户端接口类
     * @param <T> 接口类型
     * @return 代理客户端实例
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProxyClient(String serverHost, Class<T> clientInterface) {
        // 从缓存中获取或创建代理客户端
        return (T) proxyClientCache.get(serverHost)
                .computeIfAbsent(clientInterface.getName(), key -> newProxyClient(serverHost, clientInterface));
    }

    /**
     * 创建新的代理客户端
     * 使用JDK动态代理创建客户端代理实例
     *
     * @param serverHost 服务器主机地址
     * @param clientInterface 客户端接口类
     * @param <T> 接口类型
     * @return 代理客户端实例
     */
    @SuppressWarnings("unchecked")
    private <T> T newProxyClient(String serverHost, Class<T> clientInterface) {
        // 使用JDK动态代理创建代理对象
        return (T) Proxy.newProxyInstance(
                clientInterface.getClassLoader(), // 使用接口的类加载器
                new Class[]{clientInterface}, // 代理的接口列表
                new ClientInvocationHandler(Host.of(serverHost), nettyRemotingClient)); // 调用处理器
    }
}
