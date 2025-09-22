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

package org.apache.dolphinscheduler.server.master.cluster.loadbalancer;

import org.apache.dolphinscheduler.server.master.cluster.IClusters;
import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;
import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

/**
 * 动态权重轮询Worker负载均衡器
 * 
 * 这个负载均衡器根据Worker节点的实时负载情况动态计算权重，实现智能的任务分发。
 * 类比：一个聪明的工作分配员，会根据每个工人当前的忙碌程度来分配任务，
 *      忙碌的工人分配较少任务，空闲的工人分配较多任务。
 * 
 * 工作原理：
 * 1. 实时监控Worker节点的CPU、内存、线程池使用率
 * 2. 根据这些指标动态计算每个节点的权重
 * 3. 使用加权轮询算法选择合适的Worker节点
 * 4. 权重越高的节点被选中的概率越大
 * 
 * 权重计算策略：
 * - 基础权重为100
 * - 根据CPU使用率、内存使用率、线程池使用率扣减权重
 * - 可通过配置调整各项指标的影响权重
 * 
 * This load balancer is used to select a worker from {@link WorkerClusters} by dynamic weights.
 * </p>
 * The dynamic weights are calculated by the worker's load. e.g. cpu/memory/disk usage/thread usage etc.
 * You can config the weight calculation strategy in {@link WorkerLoadBalancerConfigurationProperties.DynamicWeightConfigProperties}.
 */
public class DynamicWeightedRoundRobinWorkerLoadBalancer implements IWorkerLoadBalancer {

    /**
     * Worker集群信息
     * 
     * 包含所有Worker节点的状态信息，包括资源使用情况。
     * 类比：工人信息表，记录每个工人的当前工作状态。
     */
    private final WorkerClusters workerClusters;

    /**
     * 轮询索引计数器
     * 
     * 用于实现轮询算法的计数器，确保每次选择都是公平的轮询。
     * 类比：排队取号机，确保按顺序公平处理每个请求。
     */
    private final AtomicInteger robinIndex = new AtomicInteger(0);

    /**
     * 权重服务器映射表
     * 
     * 存储每个Worker节点的权重信息，key是节点地址，value是权重服务器对象。
     * 这个映射表会根据Worker节点的实时状态动态更新。
     * 类比：工人技能评分表，记录每个工人的当前技能评分和工作能力。
     */
    private Map<String, WeightedServer<WorkerServerMetadata>> weightedServerMap = new ConcurrentHashMap<>();

    /**
     * 构造函数
     * 
     * 初始化动态权重负载均衡器，设置集群监听器来实时更新节点权重。
     * 
     * @param workerClusters Worker集群信息
     * @param dynamicWeightConfigProperties 动态权重配置属性
     */
    public DynamicWeightedRoundRobinWorkerLoadBalancer(WorkerClusters workerClusters,
                                                       WorkerLoadBalancerConfigurationProperties.DynamicWeightConfigProperties dynamicWeightConfigProperties) {
        this.workerClusters = workerClusters;
        
        // 注册集群变化监听器，实时更新权重信息
        // 类比：安排一个管理员专门负责监控工人状态变化
        this.workerClusters.registerListener(new IClusters.IClustersChangeListener<WorkerServerMetadata>() {

            /**
             * 当有新Worker节点加入时的处理
             * 
             * 计算新节点的权重并加入权重映射表。
             * 类比：新工人入职时，评估其技能并录入档案。
             */
            @Override
            public void onServerAdded(WorkerServerMetadata server) {
                // 为新加入的Worker节点计算权重并创建权重服务器对象
                // calculateWeight方法会根据节点的实时负载情况计算权重
                // 权重越高表示该节点处理能力越强，被选中的概率越大
                weightedServerMap.put(server.getAddress(), new WeightedServer<>(server, calculateWeight(server)));
            }

            /**
             * 当Worker节点离开时的处理
             * 
             * 从权重映射表中移除该节点。
             * 类比：工人离职时，从工人档案中移除其记录。
             */
            @Override
            public void onServerRemove(WorkerServerMetadata server) {
                // 从权重映射表中移除已离开的Worker节点
                // 这样确保负载均衡算法不会尝试选择已经不存在的节点
                // 避免任务分发到不可用的节点上导致执行失败
                weightedServerMap.remove(server.getAddress());
            }

            /**
             * 当Worker节点状态更新时的处理
             * 
             * 重新计算该节点的权重并更新映射表。
             * 类比：定期评估工人状态，更新其技能评分。
             */
            @Override
            public void onServerUpdate(WorkerServerMetadata server) {
                // 重新计算更新后的Worker节点权重并更新映射表
                // 当Worker节点的负载状态发生变化时（如CPU使用率、内存使用率、线程池使用率变化）
                // 需要重新计算权重以反映节点的最新处理能力
                // 这确保了负载均衡算法能够根据实时状态做出最优选择
                weightedServerMap.put(server.getAddress(), new WeightedServer<>(server, calculateWeight(server)));
            }

            /**
             * 动态权重计算方法
             * 
             * 根据Worker节点的CPU使用率、内存使用率、线程池使用率计算权重。
             * 权重计算公式：100 - (CPU使用率权重 * CPU使用率 + 内存使用率权重 * 内存使用率 + 线程池使用率权重 * 线程池使用率) / 3
             * 
             * 类比：根据工人的疲劳程度、技能水平、工作饱和度综合评估其工作能力评分。
             * 
             * @param server Worker服务器元数据
             * @return 计算出的权重值
             */
            private double calculateWeight(WorkerServerMetadata server) {
                // ========== 动态权重计算算法 ==========
                // 基础权重为100，然后根据各项负载指标扣减权重
                // 权重越高表示节点处理能力越强，被选中概率越大

                // 计算各项负载指标的加权值：
                // 1. CPU使用率权重 * CPU实际使用率
                // 2. 内存使用率权重 * 内存实际使用率
                // 3. 线程池使用率权重 * 线程池实际使用率
                double totalLoadScore = dynamicWeightConfigProperties.getCpuUsageWeight() * server.getCpuUsage()
                        + dynamicWeightConfigProperties.getMemoryUsageWeight() * server.getMemoryUsage()
                        + dynamicWeightConfigProperties.getTaskThreadPoolUsageWeight() * server.getTaskThreadPoolUsage();

                // 除以3是为了计算平均负载分数
                // 最终权重 = 100 - 平均负载分数
                // 这样负载越高的节点权重越低，负载越低的节点权重越高
                return 100 - totalLoadScore / 3;
            }
        });
    }

    /**
     * 选择Worker节点的核心方法
     * 
     * 使用动态权重轮询算法从指定工作组中选择一个合适的Worker节点。
     * 这是负载均衡的核心逻辑，实现了平滑的加权轮询算法。
     * 
     * 算法原理：
     * 1. 获取指定工作组中所有可用的Worker节点
     * 2. 计算所有节点的总权重
     * 3. 使用轮询索引选择节点，但会根据权重调整选择概率
     * 4. 权重高的节点被选中概率更大
     * 
     * 类比：聪明的任务分配员根据工人的能力评分来分配任务，
     *      能力强的工人会被分配更多任务，但也会确保每个工人都有工作机会。
     * 
     * @param workerGroup 工作组名称，用于筛选特定组的Worker节点
     * @return 被选中的Worker节点地址，如果没有可用节点则返回空
     */
    @Override
    public Optional<String> select(@NotNull String workerGroup) {
        // 获取指定工作组中所有正常状态的Worker节点的权重信息
        // 类比：从特定班组中筛选出所有在岗的工人
        List<WeightedServer<WorkerServerMetadata>> weightedServers =
                workerClusters.getNormalWorkerServerAddressByGroup(workerGroup)
                        .stream()
                        .map(weightedServerMap::get)
                        .filter(Objects::nonNull) // 过滤空值，避免workerClusters和weightedServerMap之间的不一致
                        .collect(Collectors.toList());
        
        // 如果没有可用的Worker节点，返回空
        // 类比：如果这个班组没有在岗工人，就无法分配任务
        if (CollectionUtils.isEmpty(weightedServers)) {
            return Optional.empty();
        }

        // 计算所有可用Worker节点的总权重
        // 类比：计算所有在岗工人的总能力评分
        double totalWeight = weightedServers.stream().mapToDouble(WeightedServer::getWeight).sum();

        // 使用平滑加权轮询算法选择Worker节点
        // 这个算法确保权重高的节点被选中概率更大，同时保证分配的平滑性
        WeightedServer<WorkerServerMetadata> selectedWorker = null;
        while (selectedWorker == null) {
            // 轮询选择一个节点
            WeightedServer<WorkerServerMetadata> tmpWorker =
                    weightedServers.get((robinIndex.incrementAndGet()) % weightedServers.size());
            
            // 增加当前权重（累积权重）
            // 类比：给当前工人的"被选中机会"加分
            tmpWorker.setCurrentWeight(tmpWorker.getCurrentWeight() + tmpWorker.getWeight());

            // 如果当前累积权重大于等于总权重，则选中该节点
            // 类比：当某个工人的"被选中机会"累积到足够高时，就选择他
            if (tmpWorker.getCurrentWeight() >= totalWeight) {
                // 选中后减去总权重，为下次选择做准备
                tmpWorker.setCurrentWeight(tmpWorker.getCurrentWeight() - totalWeight);
                selectedWorker = tmpWorker;
            }
        }

        // 返回被选中的Worker节点地址
        return Optional.of(selectedWorker.getServer().getAddress());
    }

    /**
     * 获取负载均衡器类型
     * 
     * @return 动态权重轮询类型标识
     */
    @Override
    public WorkerLoadBalancerType getType() {
        return WorkerLoadBalancerType.DYNAMIC_WEIGHTED_ROUND_ROBIN;
    }
}
