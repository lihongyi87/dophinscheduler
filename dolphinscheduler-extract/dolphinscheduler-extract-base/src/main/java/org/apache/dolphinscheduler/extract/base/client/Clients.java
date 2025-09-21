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

import org.apache.dolphinscheduler.extract.base.config.NettyClientConfig;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * RPC客户端工厂类
 *
 * <p>该类用于创建动态代理客户端，通过JDK动态代理机制实现RPC调用。</p>
 *
 * <p>使用示例：</p>
 * <pre>
 *     final IService proxyClient = Clients
 *            .withService(IService.class)
 *            .withHost(serverAddress);
 * </pre>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Clients {

    /**
     * JDK动态代理RPC客户端工厂实例
     * 使用单例模式，全局共享一个Netty客户端
     */
    private static final JdkDynamicRpcClientProxyFactory jdkDynamicRpcClientProxyFactory =
            new JdkDynamicRpcClientProxyFactory(
                    NettyRemotingClientFactory.buildNettyRemotingClient(
                            new NettyClientConfig()));

    /**
     * 指定服务接口，开始构建代理客户端
     *
     * @param serviceClazz 服务接口类
     * @param <T> 服务接口类型
     * @return 代理客户端构建器
     */
    public static <T> JdkDynamicRpcClientProxyBuilder<T> withService(Class<T> serviceClazz) {
        return new JdkDynamicRpcClientProxyBuilder<>(serviceClazz);
    }

    /**
     * JDK动态RPC客户端代理构建器
     *
     * <p>使用构建器模式，支持链式调用来构建代理客户端。</p>
     *
     * @param <T> 服务接口类型
     */
    public static class JdkDynamicRpcClientProxyBuilder<T> {

        /**
         * 服务接口类
         */
        private final Class<T> serviceClazz;

        /**
         * 构造函数
         *
         * @param serviceClazz 服务接口类
         */
        public JdkDynamicRpcClientProxyBuilder(Class<T> serviceClazz) {
            this.serviceClazz = serviceClazz;
        }

        /**
         * 指定服务主机地址，创建代理客户端
         *
         * @param serviceHost 服务主机地址（格式：host:port）
         * @return 服务接口的代理实现
         */
        public T withHost(String serviceHost) {
            return jdkDynamicRpcClientProxyFactory.getProxyClient(serviceHost, serviceClazz);
        }
    }

}
