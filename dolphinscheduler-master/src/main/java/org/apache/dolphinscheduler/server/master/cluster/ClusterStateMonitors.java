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

package org.apache.dolphinscheduler.server.master.cluster;

import org.apache.dolphinscheduler.server.master.engine.system.SystemEventBus;
import org.apache.dolphinscheduler.server.master.engine.system.event.MasterFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.WorkerFailoverEvent;

import java.util.Date;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 集群状态监控器
 *
 * 这个类负责监控DolphinScheduler集群中Master和Worker节点的状态变化，
 * 当节点下线时触发相应的故障转移处理。
 * 类比：工厂的安全监控系统，当发现管理人员或工人突然离岗时，立即启动应急处理程序。
 *
 * 主要职责：
 * 1. 监控Master节点状态：当Master节点下线时，触发Master故障转移
 * 2. 监控Worker节点状态：当Worker节点下线时，触发Worker故障转移
 * 3. 延迟处理机制：给节点30秒的重连时间，避免网络抖动造成的误判
 */
@Slf4j
@Component
public class ClusterStateMonitors {

    /**
     * 集群管理器
     *
     * 通过集群管理器获取Master和Worker集群的状态信息，并注册状态变化监听器。
     */
    @Autowired
    private ClusterManager clusterManager;

    /**
     * 系统事件总线
     *
     * 用于发布故障转移事件，通知系统其他组件进行相应的处理。
     * 类比：工厂的广播系统，当发生紧急情况时向全厂通知。
     */
    @Autowired
    private SystemEventBus systemEventBus;

    /**
     * 启动集群状态监控
     *
     * 这个方法会为Master集群和Worker集群分别注册服务器移除监听器。
     * 当集群中有节点下线时，会触发相应的故障转移处理。
     *
     * 类比：启动安全监控系统，开始全天候监控各个岗位的人员状态。
     */
    public void start() {
        // 为Master集群注册服务器移除监听器
        // 使用方法引用的方式注册回调函数，当Master节点从集群中移除时触发
        // 这里使用了Java 8的函数式接口和方法引用的特性
        clusterManager.getMasterClusters()
                .registerListener((IClusters.ServerRemovedListener<MasterServerMetadata>) this::masterRemoved);

        // 为Worker集群注册服务器移除监听器
        // 同样使用方法引用，当Worker节点从集群中移除时触发
        // 这种设计模式允许集群管理器与状态监控器解耦
        clusterManager.getWorkerClusters()
                .registerListener((IClusters.ServerRemovedListener<WorkerServerMetadata>) this::workerRemoved);

        // 记录启动成功的日志，方便运维人员确认监控系统已正常启动
        log.info("ClusterStateMonitors started...");
    }

    /**
     * 处理Master节点移除事件
     *
     * 当Master节点从集群中移除时，会触发Master故障转移事件。
     * 设置30秒的延迟时间，如果Master能在30秒内重新连接到注册中心，
     * 则会跳过故障转移处理，避免因网络抖动造成的误判。
     *
     * 类比：当发现一个管理人员突然离岗时，不立即启动接管程序，
     *      而是等待30秒看是否只是短暂离开（如上厕所、接电话等）。
     *
     * @param masterServer 被移除的Master服务器信息
     */
    void masterRemoved(final MasterServerMetadata masterServer) {
        // 创建Master故障转移事件并发布到系统事件总线
        // 参数说明：
        // - masterServer: 被移除的Master服务器元数据
        // - new Date(): 事件发生的时间戳
        // - 30_000: 延迟30秒执行故障转移，给Master节点一个重新连接的机会
        // 这种延迟机制可以避免因网络抖动或短暂故障造成的误判
        systemEventBus.publish(MasterFailoverEvent.of(masterServer, new Date(), 30_000));
    }

    /**
     * 处理Worker节点移除事件
     *
     * 当Worker节点从集群中移除时，会触发Worker故障转移事件。
     * 设置30秒的延迟时间，如果Worker能在30秒内重新连接到注册中心，
     * 则会跳过故障转移处理，避免因网络抖动造成的误判。
     *
     * 类比：当发现一个工人突然离岗时，不立即安排其他人接替，
     *      而是等待30秒看是否只是短暂离开。
     *
     * @param workerServer 被移除的Worker服务器信息
     */
    void workerRemoved(final WorkerServerMetadata workerServer) {
        // 创建Worker故障转移事件并发布到系统事件总线
        // 参数说明：
        // - workerServer: 被移除的Worker服务器元数据
        // - new Date(): 事件发生的时间戳
        // - 30_000: 延迟30秒执行故障转移（毫秒单位）
        // Worker故障转移主要处理正在该Worker上执行的任务，将它们重新调度到其他Worker
        systemEventBus.publish(WorkerFailoverEvent.of(workerServer, new Date(), 30_000));
    }

}
