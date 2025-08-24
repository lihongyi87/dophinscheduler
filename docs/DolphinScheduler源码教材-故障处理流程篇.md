# DolphinScheduler源码教材 - 故障处理流程篇

## 概述

分布式系统中故障是常态，DolphinScheduler设计了完善的故障检测、处理和恢复机制。本教材深入解析系统如何应对各种故障场景，确保服务的高可用性。

## 故障处理总体架构

```
故障检测 -> 故障分类 -> 应急处理 -> 故障恢复 -> 服务恢复 -> 状态同步
```

## 1. 故障检测机制

### 1.1 心跳检测系统

**HeartBeatManager - "生命体征监控器"**
```java
@Component
public class HeartBeatManager {
    
    // 心跳检测间隔（秒）
    private static final int HEARTBEAT_INTERVAL = 5;
    
    // 心跳超时阈值（秒）
    private static final int HEARTBEAT_TIMEOUT = 30;
    
    // 定期发送心跳
    @Scheduled(fixedRate = HEARTBEAT_INTERVAL * 1000)
    public void sendHeartbeat() {
        try {
            HeartBeatInfo heartbeat = HeartBeatInfo.builder()
                .serverType(getServerType())           // 服务类型：Master/Worker
                .host(getLocalHost())                  // 本机地址
                .port(getLocalPort())                  // 监听端口
                .processId(getProcessId())             // 进程ID
                .cpuUsage(getCpuUsage())               // CPU使用率
                .memoryUsage(getMemoryUsage())         // 内存使用率
                .loadAverage(getLoadAverage())         // 负载均衡
                .createTime(System.currentTimeMillis()) // 心跳时间
                .build();
            
            // 向注册中心发送心跳
            registryClient.heartbeat(heartbeat);
            
        } catch (Exception e) {
            logger.error("Send heartbeat failed", e);
        }
    }
    
    // 检测其他节点心跳超时
    @Scheduled(fixedRate = HEARTBEAT_INTERVAL * 1000)
    public void checkHeartbeatTimeout() {
        try {
            List<ServerNodeInfo> allNodes = registryClient.getServerNodes();
            long currentTime = System.currentTimeMillis();
            
            for (ServerNodeInfo node : allNodes) {
                long lastHeartbeatTime = node.getLastHeartbeatTime();
                
                if (currentTime - lastHeartbeatTime > HEARTBEAT_TIMEOUT * 1000) {
                    // 检测到节点心跳超时
                    handleNodeTimeout(node);
                }
            }
        } catch (Exception e) {
            logger.error("Check heartbeat timeout failed", e);
        }
    }
}
```

### 1.2 连接状态监控

**ConnectionMonitor - "连接状态监控器"**
```java
@Component
public class ConnectionMonitor {
    
    // 数据库连接监控
    @Scheduled(fixedRate = 10000) // 每10秒检查一次
    public void checkDatabaseConnection() {
        try {
            // 执行简单查询测试连接
            dataSource.getConnection().prepareStatement("SELECT 1").execute();
        } catch (Exception e) {
            // 数据库连接异常处理
            handleDatabaseConnectionFailure(e);
        }
    }
    
    // 注册中心连接监控
    @Scheduled(fixedRate = 5000) // 每5秒检查一次
    public void checkRegistryConnection() {
        try {
            registryClient.checkConnection();
        } catch (Exception e) {
            // 注册中心连接异常处理
            handleRegistryConnectionFailure(e);
        }
    }
}
```

### 1.3 任务超时检测

**TaskTimeoutDetector - "任务超时检测器"**
```java
@Component
public class TaskTimeoutDetector {
    
    // 状态轮询线程，定期检查任务超时
    private final StateWheelExecuteThread stateWheelExecuteThread;
    
    public void startTimeoutDetection() {
        stateWheelExecuteThread = new StateWheelExecuteThread(() -> {
            try {
                // 检查运行中任务的超时情况
                checkRunningTaskTimeout();
                
                // 检查工作流实例超时情况
                checkWorkflowInstanceTimeout();
                
            } catch (Exception e) {
                logger.error("Timeout detection error", e);
            }
        });
        
        stateWheelExecuteThread.start();
    }
    
    private void checkRunningTaskTimeout() {
        List<TaskInstance> runningTasks = taskInstanceDao.findByState(TaskExecutionStatus.RUNNING_EXECUTION);
        
        for (TaskInstance taskInstance : runningTasks) {
            if (isTaskTimeout(taskInstance)) {
                handleTaskTimeout(taskInstance);
            }
        }
    }
    
    private boolean isTaskTimeout(TaskInstance taskInstance) {
        if (taskInstance.getTimeout() <= 0) {
            return false; // 未设置超时时间
        }
        
        long startTime = taskInstance.getStartTime().getTime();
        long currentTime = System.currentTimeMillis();
        long runningTime = currentTime - startTime;
        
        return runningTime > taskInstance.getTimeout() * 1000;
    }
}
```

## 2. 故障分类和处理

### 2.1 节点故障处理

**NodeFailureHandler - "节点故障处理专家"**
```java
@Component
public class NodeFailureHandler {
    
    // 处理Master节点故障
    public void handleMasterFailure(ServerNodeInfo failedMaster) {
        logger.warn("Master node failed: {}", failedMaster.getAddress());
        
        try {
            // 1. 从注册中心移除故障Master
            registryClient.removeNode(failedMaster);
            
            // 2. 获取该Master上运行的工作流实例
            List<WorkflowInstance> runningWorkflows = 
                workflowInstanceDao.findRunningByHost(failedMaster.getAddress());
            
            // 3. 将运行中的工作流转移到其他Master
            for (WorkflowInstance workflow : runningWorkflows) {
                transferWorkflowToAnotherMaster(workflow);
            }
            
            // 4. 处理该Master上的运行中任务
            handleRunningTasksOnFailedMaster(failedMaster);
            
        } catch (Exception e) {
            logger.error("Handle master failure error", e);
        }
    }
    
    // 处理Worker节点故障
    public void handleWorkerFailure(ServerNodeInfo failedWorker) {
        logger.warn("Worker node failed: {}", failedWorker.getAddress());
        
        try {
            // 1. 从注册中心移除故障Worker
            registryClient.removeNode(failedWorker);
            
            // 2. 获取该Worker上运行的任务
            List<TaskInstance> runningTasks = 
                taskInstanceDao.findRunningByHost(failedWorker.getAddress());
            
            // 3. 重新调度运行中的任务
            for (TaskInstance taskInstance : runningTasks) {
                rescheduleTaskToAnotherWorker(taskInstance);
            }
            
            // 4. 更新工作组中Worker列表
            updateWorkerGroupNodes(failedWorker.getWorkerGroup(), failedWorker);
            
        } catch (Exception e) {
            logger.error("Handle worker failure error", e);
        }
    }
}
```

### 2.2 任务故障处理

**TaskFailureHandler - "任务故障处理专家"**
```java
@Component
public class TaskFailureHandler {
    
    // 处理任务执行失败
    public void handleTaskFailure(TaskInstance taskInstance, Exception exception) {
        logger.warn("Task execution failed: taskId={}, error={}", 
            taskInstance.getId(), exception.getMessage());
        
        try {
            // 1. 记录失败信息
            recordTaskFailure(taskInstance, exception);
            
            // 2. 检查重试策略
            if (shouldRetryTask(taskInstance)) {
                retryTask(taskInstance);
            } else {
                // 3. 标记任务最终失败
                markTaskFinalFailure(taskInstance);
                
                // 4. 检查工作流失败策略
                handleWorkflowFailureStrategy(taskInstance);
            }
            
        } catch (Exception e) {
            logger.error("Handle task failure error", e);
        }
    }
    
    // 判断是否应该重试任务
    private boolean shouldRetryTask(TaskInstance taskInstance) {
        TaskDefinition taskDef = getTaskDefinition(taskInstance);
        
        // 检查重试次数
        if (taskInstance.getRetryTimes() >= taskDef.getRetryTimes()) {
            return false;
        }
        
        // 检查重试间隔
        if (taskDef.getRetryInterval() <= 0) {
            return false;
        }
        
        return true;
    }
    
    // 重试任务
    private void retryTask(TaskInstance taskInstance) {
        // 1. 创建重试任务实例
        TaskInstance retryTaskInstance = createRetryTaskInstance(taskInstance);
        
        // 2. 计算重试延迟时间
        long retryDelay = calculateRetryDelay(taskInstance);
        
        // 3. 重新分发任务
        taskDispatcher.dispatchTask(retryTaskInstance, retryDelay);
        
        logger.info("Task retry scheduled: taskId={}, retryTimes={}", 
            taskInstance.getId(), retryTaskInstance.getRetryTimes());
    }
}
```

### 2.3 网络故障处理

**NetworkFailureHandler - "网络故障处理专家"**
```java
@Component
public class NetworkFailureHandler {
    
    // 处理RPC通信失败
    public void handleRpcFailure(String targetHost, Exception exception) {
        logger.warn("RPC communication failed: target={}, error={}", 
            targetHost, exception.getMessage());
        
        try {
            // 1. 检查目标节点状态
            if (isNodeAlive(targetHost)) {
                // 节点存活，可能是网络抖动，进行重试
                scheduleRpcRetry(targetHost);
            } else {
                // 节点不可达，标记节点故障
                markNodeUnavailable(targetHost);
            }
            
        } catch (Exception e) {
            logger.error("Handle RPC failure error", e);
        }
    }
    
    // RPC重试机制
    private void scheduleRpcRetry(String targetHost) {
        RetryTask retryTask = new RetryTask(targetHost);
        
        // 使用指数退避策略进行重试
        retryExecutor.schedule(retryTask, 
            calculateBackoffDelay(retryTask.getRetryCount()), 
            TimeUnit.MILLISECONDS);
    }
    
    // 计算退避延迟时间
    private long calculateBackoffDelay(int retryCount) {
        // 指数退避：1秒、2秒、4秒、8秒...最大60秒
        long baseDelay = 1000; // 1秒基础延迟
        long maxDelay = 60000;  // 最大60秒延迟
        
        long delay = baseDelay * (1L << retryCount);
        return Math.min(delay, maxDelay);
    }
}
```

## 3. 高可用切换机制

### 3.1 Master选举和切换

**MasterElection - "Master选举器"**
```java
@Component
public class MasterElection {
    
    // 分布式锁，确保只有一个Master协调器活跃
    private final DistributedLock coordinatorLock;
    
    // 尝试成为活跃Master
    public boolean tryBecomeActiveMaster() {
        try {
            // 1. 尝试获取Master协调器锁
            boolean acquired = coordinatorLock.tryLock(
                LOCK_TIMEOUT, TimeUnit.SECONDS);
            
            if (acquired) {
                // 2. 成功获取锁，成为活跃Master
                becomeActiveMaster();
                return true;
            } else {
                // 3. 获取锁失败，保持备用状态
                remainStandbyMaster();
                return false;
            }
            
        } catch (Exception e) {
            logger.error("Master election failed", e);
            return false;
        }
    }
    
    private void becomeActiveMaster() {
        logger.info("Become active master");
        
        // 1. 启动Master协调器
        masterCoordinator.start();
        
        // 2. 启动工作流引擎
        workflowEngine.start();
        
        // 3. 启动任务调度器
        taskScheduler.start();
        
        // 4. 注册为活跃Master
        registryClient.registerActiveMaster(getLocalServerInfo());
    }
    
    // 监控Master锁状态
    @Scheduled(fixedRate = 10000) // 每10秒检查一次
    public void monitorMasterLock() {
        if (isActiveMaster() && !coordinatorLock.isHeldByCurrentThread()) {
            // 锁丢失，可能网络分区或其他问题
            handleMasterLockLoss();
        }
    }
}
```

### 3.2 故障转移处理

**FailoverHandler - "故障转移处理器"**
```java
@Component
public class FailoverHandler {
    
    // 处理Master故障转移
    public void handleMasterFailover(String failedMasterHost) {
        logger.info("Handling master failover: {}", failedMasterHost);
        
        try {
            // 1. 获取故障Master上的运行实例
            List<WorkflowInstance> orphanWorkflows = 
                findOrphanWorkflows(failedMasterHost);
            
            // 2. 重新分配工作流实例到当前Master
            for (WorkflowInstance workflow : orphanWorkflows) {
                takeoverWorkflowInstance(workflow);
            }
            
            // 3. 恢复中断的任务调度
            resumeTaskScheduling(failedMasterHost);
            
        } catch (Exception e) {
            logger.error("Master failover error", e);
        }
    }
    
    // 接管工作流实例
    private void takeoverWorkflowInstance(WorkflowInstance workflow) {
        try {
            // 1. 更新工作流实例的Master主机
            workflow.setHost(getLocalHost());
            workflowInstanceDao.updateById(workflow);
            
            // 2. 重建工作流执行上下文
            WorkflowExecuteRunnable executeRunnable = 
                rebuildWorkflowExecuteRunnable(workflow);
            
            // 3. 提交到当前Master执行
            masterEngine.submitWorkflowExecuteRunnable(executeRunnable);
            
            logger.info("Takeover workflow instance: {}", workflow.getId());
            
        } catch (Exception e) {
            logger.error("Takeover workflow failed: {}", workflow.getId(), e);
        }
    }
}
```

## 4. 数据恢复机制

### 4.1 状态恢复

**StateRecoveryManager - "状态恢复管理器"**
```java
@Component
public class StateRecoveryManager {
    
    // 系统启动时恢复未完成的工作流
    @PostConstruct
    public void recoverUnfinishedWorkflows() {
        try {
            // 1. 查找所有未完成的工作流实例
            List<WorkflowInstance> unfinishedWorkflows = 
                workflowInstanceDao.findUnfinishedInstances();
            
            // 2. 按优先级排序
            unfinishedWorkflows.sort(Comparator.comparing(
                WorkflowInstance::getWorkflowInstancePriority).reversed());
            
            // 3. 逐个恢复工作流
            for (WorkflowInstance workflow : unfinishedWorkflows) {
                recoverWorkflowInstance(workflow);
            }
            
            logger.info("Recovered {} unfinished workflows", 
                unfinishedWorkflows.size());
            
        } catch (Exception e) {
            logger.error("Recover unfinished workflows failed", e);
        }
    }
    
    // 恢复单个工作流实例
    private void recoverWorkflowInstance(WorkflowInstance workflow) {
        try {
            // 1. 检查工作流状态
            if (workflow.getState().isFinished()) {
                return; // 已完成，无需恢复
            }
            
            // 2. 恢复任务实例状态
            recoverTaskInstances(workflow);
            
            // 3. 重建执行上下文
            WorkflowExecuteRunnable executeRunnable = 
                rebuildWorkflowExecuteRunnable(workflow);
            
            // 4. 提交执行
            masterEngine.submitWorkflowExecuteRunnable(executeRunnable);
            
        } catch (Exception e) {
            logger.error("Recover workflow instance failed: {}", 
                workflow.getId(), e);
        }
    }
}
```

### 4.2 任务状态同步

**TaskStateSynchronizer - "任务状态同步器"**
```java
@Component
public class TaskStateSynchronizer {
    
    // 同步Worker上的任务状态
    public void syncTaskStatesFromWorkers() {
        try {
            // 1. 获取所有活跃的Worker节点
            List<ServerNodeInfo> activeWorkers = getActiveWorkers();
            
            // 2. 并行查询各Worker的任务状态
            List<CompletableFuture<Void>> syncTasks = activeWorkers.stream()
                .map(worker -> CompletableFuture.runAsync(() -> 
                    syncTaskStatesFromWorker(worker)))
                .collect(Collectors.toList());
            
            // 3. 等待所有同步任务完成
            CompletableFuture.allOf(syncTasks.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            logger.error("Sync task states from workers failed", e);
        }
    }
    
    private void syncTaskStatesFromWorker(ServerNodeInfo worker) {
        try {
            // 1. 查询Worker上的任务状态
            List<TaskInstance> workerTasks = 
                workerRpcClient.queryRunningTasks(worker.getAddress());
            
            // 2. 与Master端状态比较并同步
            for (TaskInstance workerTask : workerTasks) {
                TaskInstance masterTask = 
                    taskInstanceDao.findById(workerTask.getId());
                
                if (masterTask != null && 
                    !masterTask.getState().equals(workerTask.getState())) {
                    // 状态不一致，以Worker端为准更新Master端
                    syncTaskState(masterTask, workerTask);
                }
            }
            
        } catch (Exception e) {
            logger.error("Sync task states from worker {} failed", 
                worker.getAddress(), e);
        }
    }
}
```

## 5. 监控和告警机制

### 5.1 故障监控指标

**FaultMonitoringMetrics - "故障监控指标收集器"**
```java
@Component
public class FaultMonitoringMetrics {
    
    // 各类故障计数器
    private final Counter masterFailureCounter = Counter.build()
        .name("master_failure_total")
        .help("Total number of master failures")
        .register();
    
    private final Counter workerFailureCounter = Counter.build()
        .name("worker_failure_total")
        .help("Total number of worker failures")
        .register();
    
    private final Counter taskFailureCounter = Counter.build()
        .name("task_failure_total")
        .help("Total number of task failures")
        .register();
    
    // 故障恢复时间直方图
    private final Histogram failoverDurationHistogram = Histogram.build()
        .name("failover_duration_seconds")
        .help("Duration of failover operations in seconds")
        .register();
    
    // 记录Master故障
    public void recordMasterFailure() {
        masterFailureCounter.inc();
    }
    
    // 记录故障转移时间
    public void recordFailoverDuration(double durationSeconds) {
        failoverDurationHistogram.observe(durationSeconds);
    }
}
```

### 5.2 告警触发机制

**AlertTrigger - "告警触发器"**
```java
@Component
public class AlertTrigger {
    
    // 节点故障告警
    public void triggerNodeFailureAlert(ServerNodeInfo failedNode) {
        AlertMessage alert = AlertMessage.builder()
            .alertType(AlertType.NODE_FAILURE)
            .severity(AlertSeverity.HIGH)
            .title("Node Failure Detected")
            .content(String.format("Node %s has failed and been removed from cluster", 
                failedNode.getAddress()))
            .timestamp(System.currentTimeMillis())
            .build();
        
        alertManager.sendAlert(alert);
    }
    
    // 任务大量失败告警
    @Scheduled(fixedRate = 60000) // 每分钟检查一次
    public void checkTaskFailureRate() {
        try {
            // 统计最近5分钟的任务失败率
            long recentFailureCount = getRecentTaskFailureCount(5);
            long recentTotalCount = getRecentTaskTotalCount(5);
            
            if (recentTotalCount > 0) {
                double failureRate = (double) recentFailureCount / recentTotalCount;
                
                if (failureRate > TASK_FAILURE_RATE_THRESHOLD) {
                    triggerHighTaskFailureRateAlert(failureRate);
                }
            }
            
        } catch (Exception e) {
            logger.error("Check task failure rate error", e);
        }
    }
}
```

## 6. 自动修复机制

### 6.1 服务自愈

**SelfHealingManager - "自愈管理器"**
```java
@Component
public class SelfHealingManager {
    
    // 检查并修复异常状态
    @Scheduled(fixedRate = 30000) // 每30秒检查一次
    public void performSelfHealing() {
        try {
            // 1. 检查并修复僵尸任务
            healZombieTasks();
            
            // 2. 检查并修复丢失的任务
            healLostTasks();
            
            // 3. 检查并修复状态不一致的工作流
            healInconsistentWorkflows();
            
        } catch (Exception e) {
            logger.error("Self healing error", e);
        }
    }
    
    // 修复僵尸任务（长时间运行中但Worker已不存在）
    private void healZombieTasks() {
        List<TaskInstance> runningTasks = 
            taskInstanceDao.findByState(TaskExecutionStatus.RUNNING_EXECUTION);
        
        for (TaskInstance task : runningTasks) {
            if (!isWorkerAlive(task.getHost())) {
                // Worker已不存在，将任务标记为失败并重新调度
                logger.warn("Detected zombie task: {}, worker: {}", 
                    task.getId(), task.getHost());
                
                task.setState(TaskExecutionStatus.FAILURE);
                taskInstanceDao.updateById(task);
                
                // 触发任务重新调度
                taskFailureHandler.handleTaskFailure(task, 
                    new RuntimeException("Worker node unavailable"));
            }
        }
    }
}
```

### 6.2 资源清理

**ResourceCleaner - "资源清理器"**
```java
@Component
public class ResourceCleaner {
    
    // 定期清理过期资源
    @Scheduled(cron = "0 0 2 * * ?") // 每天凌晨2点执行
    public void cleanExpiredResources() {
        try {
            // 1. 清理完成时间超过保留期的工作流实例
            cleanExpiredWorkflowInstances();
            
            // 2. 清理过期的任务实例
            cleanExpiredTaskInstances();
            
            // 3. 清理过期的日志文件
            cleanExpiredLogFiles();
            
        } catch (Exception e) {
            logger.error("Clean expired resources error", e);
        }
    }
    
    private void cleanExpiredWorkflowInstances() {
        // 计算保留截止时间
        Date retentionCutoff = DateUtils.addDays(new Date(), -WORKFLOW_RETENTION_DAYS);
        
        List<WorkflowInstance> expiredInstances = 
            workflowInstanceDao.findExpiredInstances(retentionCutoff);
        
        for (WorkflowInstance instance : expiredInstances) {
            // 删除相关的任务实例
            taskInstanceDao.deleteByWorkflowInstanceId(instance.getId());
            
            // 删除工作流实例
            workflowInstanceDao.deleteById(instance.getId());
            
            logger.info("Cleaned expired workflow instance: {}", instance.getId());
        }
    }
}
```

## 7. 性能优化

### 7.1 故障检测优化

- **智能心跳间隔**：根据集群规模动态调整心跳频率
- **分层检测**：不同级别故障使用不同检测策略
- **批量检测**：批量检查多个节点状态
- **缓存优化**：缓存节点状态信息

### 7.2 故障恢复优化

- **并行恢复**：并行处理多个故障实例
- **优先级恢复**：高优先级任务优先恢复
- **增量恢复**：只恢复变化的部分
- **资源预留**：为故障恢复预留系统资源

## 8. 最佳实践建议

### 8.1 故障预防

1. **合理的资源配置**：避免单点过载
2. **健康检查**：定期检查系统健康状态
3. **监控告警**：完善的监控和告警机制
4. **定期备份**：重要数据定期备份

### 8.2 故障应对

1. **快速检测**：尽快发现故障
2. **自动恢复**：优先使用自动恢复机制
3. **降级处理**：必要时进行服务降级
4. **人工介入**：复杂故障需要人工处理

## 小结

DolphinScheduler的故障处理机制体现了以下设计理念：

1. **故障常态化**：将故障视为分布式系统的常态
2. **快速检测**：多维度快速检测各类故障
3. **自动恢复**：优先使用自动恢复机制
4. **优雅降级**：在故障期间保持核心功能可用
5. **持续监控**：持续监控和改进故障处理机制

这种全方位的故障处理设计确保了DolphinScheduler在面对各种故障场景时都能保持高可用性和可靠性。