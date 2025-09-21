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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.utils.Host;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

/**
 * 客户端调用处理器
 *
 * <p>该类实现了JDK动态代理的InvocationHandler接口，负责拦截方法调用并转换为远程调用。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>拦截代理对象的方法调用</li>
 *   <li>判断方法是否需要远程调用</li>
 *   <li>将本地方法调用转换为远程RPC调用</li>
 *   <li>缓存方法调用器以提高性能</li>
 * </ul>
 */
@Slf4j
class ClientInvocationHandler implements InvocationHandler {

    /**
     * Netty远程通信客户端
     */
    private final NettyRemotingClient nettyRemotingClient;

    /**
     * 方法调用器缓存
     * key: 方法签名，value: 方法调用器
     */
    private final Map<String, ClientMethodInvoker> methodInvokerMap;

    /**
     * 服务器主机地址
     */
    private final Host serverHost;

    /**
     * 构造函数
     *
     * @param serverHost 服务器主机地址
     * @param nettyRemotingClient Netty远程通信客户端
     */
    ClientInvocationHandler(Host serverHost, NettyRemotingClient nettyRemotingClient) {
        // 确保参数非空
        this.serverHost = checkNotNull(serverHost);
        this.nettyRemotingClient = checkNotNull(nettyRemotingClient);
        // 初始化线程安全的方法调用器缓存
        this.methodInvokerMap = new ConcurrentHashMap<>();
    }

    /**
     * 代理方法调用
     * 拦截方法调用并根据注解判断是否进行远程调用
     *
     * @param proxy 代理对象
     * @param method 调用的方法
     * @param args 方法参数
     * @return 方法调用结果
     * @throws Throwable 可能抛出的异常
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 检查方法是否有RpcMethod注解
        if (method.getAnnotation(RpcMethod.class) == null) {
            // 没有注解的方法直接本地调用
            return method.invoke(proxy, args);
        }
        // 从缓存中获取或创建方法调用器
        ClientMethodInvoker methodInvoker = methodInvokerMap.computeIfAbsent(
                method.toGenericString(), m -> new SyncClientMethodInvoker(serverHost, method, nettyRemotingClient));
        try {
            // 执行远程方法调用
            return methodInvoker.invoke(proxy, method, args);
        } catch (UndeclaredThrowableException undeclaredThrowableException) {
            // 处理未声明的异常，抛出原始异常
            throw undeclaredThrowableException.getCause();
        } catch (Throwable throwable) {
            // 直接抛出其他异常
            throw throwable;
        }
    }

}
