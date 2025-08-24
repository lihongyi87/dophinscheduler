# DolphinScheduler源码教材 - 任务调度流程篇

## 概述

任务调度是DolphinScheduler的核心能力，本教材深入解析任务从创建到执行完成的整个调度流程，包括任务分发、执行器选择、负载均衡、状态管理等关键环节。

## 任务调度完整链路

```
任务就绪 -> 分发器协调 -> 执行器选择 -> 任务分发 -> Worker执行 -> 状态汇报 -> 结果处理
```

## 1. 任务就绪判断阶段

### 1.1 任务就绪条件检查

**TaskReadyChecker - "任务就绪检查官"**
```java
public class TaskReadyChecker {
    
    public boolean isTaskReadyToSubmit(TaskInstance taskInstance) {
        // 1. 检查前置依赖任务是否完成
        if (!checkPreTasksFinished(taskInstance)) {
            return false;
        }
        
        // 2. 检查任务定义是否有效
        if (!checkTaskDefinitionValid(taskInstance)) {
            return false;
        }
        
        // 3. 检查工作组是否有可用Worker
        if (!checkWorkerGroupAvailable(taskInstance)) {
            return false;
        }
        
        // 4. 检查资源限制
        if (!checkResourceLimitation(taskInstance)) {
            return false;
        }
        
        return true;
    }
}
```

### 1.2 依赖关系解析

**任务依赖类型：**
```java
public enum TaskDependType {
    TASK_ONLY,          // 仅依赖任务完成
    TASK_SUCCESS,       // 依赖任务成功
    TASK_FAILURE        // 依赖任务失败
}
```

**依赖检查逻辑：**
```java
// 检查前置任务状态
private boolean checkPreTasksFinished(TaskInstance currentTask) {
    List<TaskInstance> preTasks = getPreTasks(currentTask);
    
    for (TaskInstance preTask : preTasks) {
        TaskDependType dependType = getDependType(currentTask, preTask);
        
        switch (dependType) {
            case TASK_ONLY:
                if (!preTask.getState().isFinished()) {
                    return false;
                }
                break;
            case TASK_SUCCESS:
                if (preTask.getState() != SUCCESS) {
                    return false;
                }
                break;
            case TASK_FAILURE:
                if (preTask.getState() != FAILURE) {
                    return false;
                }
                break;
        }
    }
    return true;
}
```

## 2. 任务分发协调阶段

### 2.1 WorkerGroupDispatcherCoordinator架构

**分发协调者 - "任务分发中心"**
```java
@Component
public class WorkerGroupDispatcherCoordinator {
    
    // 工作组分发器映射表
    private final ConcurrentHashMap<String, WorkerGroupDispatcher> workerGroupDispatcherMap;
    
    // 任务执行器客户端
    private ITaskExecutorClient taskExecutorClient;
    
    // 分发任务到指定工作组
    public void dispatchTask(ITaskExecutionRunnable taskExecutionRunnable, long delayTimeMills) {
        String workerGroup = taskExecutionRunnable.getTaskInstance().getWorkerGroup();
        
        // 获取或创建工作组分发器
        WorkerGroupDispatcher dispatcher = getOrCreateWorkerGroupDispatcher(workerGroup);
        
        // 添加任务到分发队列
        dispatcher.dispatchTask(taskExecutionRunnable, delayTimeMills);
    }
}
```

### 2.2 WorkerGroupDispatcher详解

**工作组分发器 - "专业分发员"**
```java
public class WorkerGroupDispatcher implements AutoCloseable {
    
    // 优先级延迟队列
    private PriorityDelayQueue<ITaskExecutionRunnable> taskQueue;
    
    // 分发线程
    private Thread dispatchThread;
    
    // 任务分发主循环
    @Override
    public void run() {
        while (!isStop) {
            try {
                // 1. 从队列中获取待分发任务
                ITaskExecutionRunnable taskRunnable = taskQueue.take();
                
                // 2. 检查任务是否仍然有效
                if (!isTaskValid(taskRunnable)) {
                    continue;
                }
                
                // 3. 选择合适的Worker节点
                String selectedWorker = selectWorkerNode(taskRunnable);
                
                if (selectedWorker != null) {
                    // 4. 分发任务到选中的Worker
                    dispatchTaskToWorker(taskRunnable, selectedWorker);
                } else {
                    // 5. 无可用Worker，延迟重试
                    reEnqueueTask(taskRunnable, RETRY_DELAY);
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Dispatch task error", e);
            }
        }
    }
}
```

## 3. 执行器选择阶段

### 3.1 Worker节点管理

**WorkerGroupRegistry - "工作组花名册"**
```java
@Component
public class WorkerGroupRegistry {
    
    // 工作组与Worker节点映射
    private final Map<String, Set<ServerNodeInfo>> workerGroupNodes;
    
    // 获取工作组中的可用Worker
    public Set<ServerNodeInfo> getAvailableWorkers(String workerGroup) {
        Set<ServerNodeInfo> workers = workerGroupNodes.get(workerGroup);
        
        if (CollectionUtils.isEmpty(workers)) {
            return Collections.emptySet();
        }
        
        // 过滤出活跃的Worker节点
        return workers.stream()
            .filter(this::isWorkerActive)
            .filter(this::isWorkerHealthy)
            .collect(Collectors.toSet());
    }
    
    // 检查Worker是否活跃
    private boolean isWorkerActive(ServerNodeInfo workerNode) {
        long lastHeartbeatTime = workerNode.getLastHeartbeatTime();
        long currentTime = System.currentTimeMillis();
        
        return (currentTime - lastHeartbeatTime) <= WORKER_HEARTBEAT_TIMEOUT;
    }
}
```

### 3.2 负载均衡策略

**LoadBalancer接口定义：**
```java
public interface LoadBalancer {
    /**
     * 从可用节点中选择一个最优节点
     */
    ServerNodeInfo select(List<ServerNodeInfo> availableNodes, TaskInstance taskInstance);
}
```

**轮询负载均衡实现：**
```java
@Component
public class RoundRobinLoadBalancer implements LoadBalancer {
    
    private final AtomicInteger currentIndex = new AtomicInteger(0);
    
    @Override
    public ServerNodeInfo select(List<ServerNodeInfo> availableNodes, TaskInstance taskInstance) {
        if (CollectionUtils.isEmpty(availableNodes)) {
            return null;
        }
        
        int index = currentIndex.getAndIncrement() % availableNodes.size();
        return availableNodes.get(index);
    }
}
```

**权重负载均衡实现：**
```java
@Component
public class WeightedLoadBalancer implements LoadBalancer {
    
    @Override
    public ServerNodeInfo select(List<ServerNodeInfo> availableNodes, TaskInstance taskInstance) {
        if (CollectionUtils.isEmpty(availableNodes)) {
            return null;
        }
        
        // 根据Worker负载计算权重
        List<WeightedNode> weightedNodes = availableNodes.stream()
            .map(node -> new WeightedNode(node, calculateWeight(node)))
            .collect(Collectors.toList());
        
        // 基于权重随机选择
        return weightedRandomSelect(weightedNodes).getNode();
    }
    
    private int calculateWeight(ServerNodeInfo node) {
        // 权重计算考虑因素：
        // 1. CPU使用率（权重越低CPU使用率越低）
        // 2. 内存使用率（权重越低内存使用率越低）  
        // 3. 当前运行任务数（权重越低任务数越少）
        
        double cpuUsage = node.getCpuUsage();
        double memoryUsage = node.getMemoryUsage();
        int runningTaskCount = node.getRunningTaskCount();
        
        // 基础权重100，根据负载情况递减
        int weight = 100;
        weight -= (int) (cpuUsage * 50);        // CPU影响权重
        weight -= (int) (memoryUsage * 30);     // 内存影响权重
        weight -= runningTaskCount * 2;         // 任务数影响权重
        
        return Math.max(weight, 1); // 保证最小权重为1
    }
}
```

## 4. 任务分发阶段

### 4.1 TaskExecutorClient架构

**任务执行器客户端 - "任务管理员"**
```java
@Component
public class TaskExecutorClient implements ITaskExecutorClient {
    
    // 逻辑任务执行器客户端代理
    private LogicTaskExecutorClientDelegator logicTaskExecutorClientDelegator;
    
    // 物理任务执行器客户端代理  
    private PhysicalTaskExecutorClientDelegator physicalTaskExecutorClientDelegator;
    
    @Override
    public void dispatch(ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException {
        try {
            // 根据任务类型选择对应的客户端代理
            ITaskExecutorClientDelegator delegator = getTaskExecutorClientDelegator(taskExecutionRunnable);
            
            // 委托给具体的代理执行分发
            delegator.dispatch(taskExecutionRunnable);
            
        } catch (Exception ex) {
            throw new TaskDispatchException("Dispatch task failed", ex);
        }
    }
    
    // 根据任务类型选择代理
    private ITaskExecutorClientDelegator getTaskExecutorClientDelegator(ITaskExecutionRunnable taskExecutionRunnable) {
        TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        
        if (TaskTypeUtils.isLogicTask(taskInstance.getTaskType())) {
            return logicTaskExecutorClientDelegator;  // 逻辑任务
        }
        return physicalTaskExecutorClientDelegator;   // 物理任务
    }
}
```

### 4.2 物理任务分发详解

**PhysicalTaskExecutorClientDelegator - "物理任务分发专家"**
```java
@Component
public class PhysicalTaskExecutorClientDelegator implements ITaskExecutorClientDelegator {
    
    @Override
    public void dispatch(ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException {
        TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        
        try {
            // 1. 准备任务分发请求
            TaskDispatchRequest dispatchRequest = buildTaskDispatchRequest(taskInstance);
            
            // 2. 获取目标Worker地址
            String workerAddress = taskInstance.getHost();
            
            // 3. 通过RPC发送分发请求
            TaskDispatchResponse response = sendDispatchRequest(workerAddress, dispatchRequest);
            
            // 4. 处理分发响应
            handleDispatchResponse(taskInstance, response);
            
        } catch (Exception e) {
            throw new TaskDispatchException("Physical task dispatch failed", e);
        }
    }
    
    // 构建任务分发请求
    private TaskDispatchRequest buildTaskDispatchRequest(TaskInstance taskInstance) {
        return TaskDispatchRequest.builder()
            .taskInstanceId(taskInstance.getId())
            .taskCode(taskInstance.getTaskCode())
            .taskType(taskInstance.getTaskType())
            .taskParams(taskInstance.getTaskParams())
            .workerGroup(taskInstance.getWorkerGroup())
            .environmentCode(taskInstance.getEnvironmentCode())
            .build();
    }
}
```

### 4.3 逻辑任务处理

**LogicTaskExecutorClientDelegator - "逻辑任务处理专家"**
```java
@Component  
public class LogicTaskExecutorClientDelegator implements ITaskExecutorClientDelegator {
    
    @Override
    public void dispatch(ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException {
        TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        String taskType = taskInstance.getTaskType();
        
        try {
            // 根据逻辑任务类型选择处理策略
            switch (taskType) {
                case "CONDITIONS":
                    handleConditionsTask(taskInstance);
                    break;
                case "DEPENDENT":
                    handleDependentTask(taskInstance);
                    break;
                case "SUB_PROCESS":
                    handleSubProcessTask(taskInstance);
                    break;
                default:
                    throw new TaskDispatchException("Unsupported logic task type: " + taskType);
            }
        } catch (Exception e) {
            throw new TaskDispatchException("Logic task dispatch failed", e);
        }
    }
}
```

## 5. Worker端任务执行

### 5.1 TaskExecutor架构

**Worker端任务执行器：**
```java
@Component
public class TaskExecutor {
    
    // 任务执行线程池
    private ThreadPoolExecutor taskExecutorThreadPool;
    
    // 任务管理器
    private TaskManager taskManager;
    
    // 接收Master分发的任务
    @EventListener
    public void handleTaskDispatchEvent(TaskDispatchEvent event) {
        TaskInstance taskInstance = event.getTaskInstance();
        
        try {
            // 1. 验证任务有效性
            validateTask(taskInstance);
            
            // 2. 创建任务执行线程
            TaskExecutionThread taskThread = createTaskExecutionThread(taskInstance);
            
            // 3. 提交到线程池执行
            taskExecutorThreadPool.submit(taskThread);
            
            // 4. 向Master汇报任务接收成功
            reportTaskReceived(taskInstance);
            
        } catch (Exception e) {
            // 向Master汇报任务接收失败
            reportTaskFailed(taskInstance, e);
        }
    }
}
```

### 5.2 任务执行线程

**TaskExecutionThread - "任务执行者"**
```java
public class TaskExecutionThread implements Runnable {
    
    private final TaskInstance taskInstance;
    private final TaskExecutionContext taskExecutionContext;
    
    @Override
    public void run() {
        try {
            // 1. 任务执行前准备
            prepareTaskExecution();
            
            // 2. 向Master汇报任务开始执行
            reportTaskRunning();
            
            // 3. 根据任务类型创建具体执行器
            AbstractTask taskExecutor = createTaskExecutor();
            
            // 4. 执行任务
            taskExecutor.handle(taskExecutionContext);
            
            // 5. 处理执行结果
            handleTaskResult(taskExecutor.getExitStatus());
            
        } catch (Exception e) {
            handleTaskException(e);
        } finally {
            // 清理资源
            cleanupResources();
        }
    }
    
    // 创建任务执行器
    private AbstractTask createTaskExecutor() {
        String taskType = taskInstance.getTaskType();
        
        return taskExecutorFactory.createTaskExecutor(taskType, taskExecutionContext);
    }
}
```

## 6. 任务状态管理

### 6.1 任务状态生命周期

```
SUBMITTED_SUCCESS -> DISPATCH -> RUNNING_EXECUTION -> SUCCESS/FAILURE -> COMPLETED
```

### 6.2 状态汇报机制

**Worker向Master状态汇报：**
```java
@Component
public class TaskStateReporter {
    
    // 向Master汇报任务状态变化
    public void reportTaskState(TaskInstance taskInstance, TaskExecutionStatus newStatus) {
        TaskStateChangeEvent event = TaskStateChangeEvent.builder()
            .taskInstanceId(taskInstance.getId())
            .processInstanceId(taskInstance.getProcessInstanceId())
            .newStatus(newStatus)
            .executeTime(new Date())
            .workerAddress(getLocalWorkerAddress())
            .build();
        
        try {
            // 通过RPC向Master发送状态变化事件
            masterRpcClient.reportTaskStateChange(event);
        } catch (Exception e) {
            // 状态汇报失败，加入重试队列
            stateReportRetryQueue.add(event);
        }
    }
}
```

### 6.3 Master端状态处理

**TaskStateChangeHandler - "状态变化处理器"**
```java
@Component
public class TaskStateChangeHandler {
    
    @RpcHandler
    public void handleTaskStateChange(TaskStateChangeEvent event) {
        try {
            // 1. 获取任务实例
            TaskInstance taskInstance = taskInstanceDao.findById(event.getTaskInstanceId());
            
            // 2. 验证状态变化的合法性
            if (!isStateChangeValid(taskInstance.getState(), event.getNewStatus())) {
                return;
            }
            
            // 3. 更新任务状态
            taskInstance.setState(event.getNewStatus());
            taskInstance.setEndTime(event.getExecuteTime());
            taskInstanceDao.updateById(taskInstance);
            
            // 4. 发布状态变化事件到事件总线
            publishTaskStateChangeEvent(taskInstance);
            
            // 5. 检查工作流是否需要状态更新
            checkAndUpdateWorkflowState(taskInstance);
            
        } catch (Exception e) {
            logger.error("Handle task state change failed", e);
        }
    }
}
```

## 7. 负载保护机制

### 7.1 Worker负载监控

**WorkerLoadMonitor - "负载监控器"**
```java
@Component
public class WorkerLoadMonitor {
    
    // 定期采集负载指标
    @Scheduled(fixedRate = 5000)  // 每5秒采集一次
    public void collectLoadMetrics() {
        WorkerLoadInfo loadInfo = WorkerLoadInfo.builder()
            .cpuUsage(getCpuUsage())
            .memoryUsage(getMemoryUsage())
            .diskUsage(getDiskUsage())
            .runningTaskCount(getRunningTaskCount())
            .build();
        
        // 更新本地负载信息
        updateLocalLoadInfo(loadInfo);
        
        // 向注册中心汇报负载信息
        reportLoadInfoToRegistry(loadInfo);
        
        // 检查是否需要负载保护
        checkLoadProtection(loadInfo);
    }
    
    // 负载保护检查
    private void checkLoadProtection(WorkerLoadInfo loadInfo) {
        if (isOverloaded(loadInfo)) {
            // 启动负载保护
            enableLoadProtection();
        } else if (isLoadNormal(loadInfo)) {
            // 解除负载保护
            disableLoadProtection();
        }
    }
}
```

### 7.2 任务排队机制

**TaskQueue - "任务队列"**
```java
public class PriorityTaskQueue {
    
    // 基于优先级的延迟队列
    private final PriorityDelayQueue<TaskInstance> taskQueue;
    
    // 队列容量限制
    private final int maxQueueSize;
    
    // 添加任务到队列
    public boolean offer(TaskInstance taskInstance, long delayMills) {
        if (taskQueue.size() >= maxQueueSize) {
            // 队列已满，拒绝新任务
            return false;
        }
        
        // 设置任务延迟时间
        taskInstance.setDelayTime(System.currentTimeMillis() + delayMills);
        
        return taskQueue.offer(taskInstance);
    }
    
    // 从队列获取任务
    public TaskInstance take() throws InterruptedException {
        return taskQueue.take();
    }
}
```

## 8. 错误处理和重试

### 8.1 分发失败处理

```java
// 任务分发失败处理策略
public class TaskDispatchFailureHandler {
    
    public void handleDispatchFailure(TaskInstance taskInstance, Exception exception) {
        // 1. 记录分发失败日志
        logDispatchFailure(taskInstance, exception);
        
        // 2. 判断是否需要重试
        if (shouldRetry(taskInstance, exception)) {
            // 计算重试延迟时间（指数退避）
            long retryDelay = calculateRetryDelay(taskInstance.getRetryTimes());
            
            // 重新加入分发队列
            reEnqueueTask(taskInstance, retryDelay);
        } else {
            // 标记任务为分发失败
            markTaskDispatchFailed(taskInstance, exception);
        }
    }
}
```

### 8.2 执行失败恢复

```java
// 任务执行失败恢复机制
public class TaskExecutionRecoveryHandler {
    
    public void handleExecutionFailure(TaskInstance taskInstance) {
        TaskDefinition taskDef = getTaskDefinition(taskInstance);
        
        // 检查重试策略
        if (taskDef.getRetryTimes() > 0 && 
            taskInstance.getRetryTimes() < taskDef.getRetryTimes()) {
            
            // 创建重试任务实例
            TaskInstance retryTaskInstance = createRetryTaskInstance(taskInstance);
            
            // 重新分发任务
            dispatchTask(retryTaskInstance);
            
        } else {
            // 重试次数用完，检查失败策略
            handleFinalFailure(taskInstance);
        }
    }
}
```

## 9. 性能优化策略

### 9.1 分发性能优化

- **批量分发**：合并多个任务一次性分发
- **预分发**：提前分发下一批任务
- **缓存优化**：缓存Worker节点信息
- **连接复用**：复用RPC连接

### 9.2 执行性能优化

- **线程池调优**：合理配置核心线程数和最大线程数
- **资源预分配**：提前分配执行资源
- **并发控制**：控制同时执行任务数量
- **内存优化**：及时清理任务执行上下文

### 9.3 通信性能优化

- **异步通信**：使用异步RPC调用
- **消息压缩**：压缩大消息内容
- **连接池**：维护RPC连接池
- **超时控制**：合理设置超时时间

## 小结

DolphinScheduler的任务调度流程体现了以下核心特性：

1. **智能分发**：基于负载均衡的智能任务分发
2. **高可靠性**：完善的错误处理和重试机制
3. **负载保护**：动态负载监控和保护机制
4. **灵活扩展**：支持多种任务类型和执行器
5. **性能优化**：多维度的性能优化策略

这种设计确保了任务能够高效、可靠地调度和执行，同时具备良好的扩展性和容错能力。