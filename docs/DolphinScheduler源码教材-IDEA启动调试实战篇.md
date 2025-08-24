# DolphinScheduler源码教材 - IDEA启动调试实战篇

## 概述

本教材基于当前改造后的DolphinScheduler源码项目，详细讲解如何在IntelliJ IDEA中导入、配置、启动和调试本项目。该项目已经完成了去中心化改造，内置Raft一致性算法，无需外部注册中心即可运行。

## 项目特点

✅ **完全去中心化**: 无需Zookeeper等外部注册中心  
✅ **内嵌Raft算法**: 提供高可用的分布式一致性保证  
✅ **80%+中文注释**: 详细的中文注释，便于学习理解  
✅ **完整教材体系**: 包含9个源码学习教材  
✅ **一键启动**: 简化的启动流程，适合开发调试  

## 1. 环境准备

### 1.1 软件环境要求

**必需软件列表：**
```yaml
development_environment:
  jdk: "JDK 1.8 或 JDK 11 (推荐JDK 1.8)"
  ide: "IntelliJ IDEA 2020.3+ (推荐Ultimate版)"
  maven: "Apache Maven 3.6+"
  database: "内置H2数据库 (开发) / MySQL 8.0+ (生产)"
  git: "Git 2.20+"
```

**硬件建议配置：**
```yaml
hardware_requirements:
  cpu: "4核心以上"
  memory: "8GB以上 (推荐16GB)"
  disk: "至少10GB可用空间"
  network: "稳定的网络连接"
```

### 1.2 当前项目获取

**从个人仓库克隆：**
```bash
# 克隆改造后的源码
git clone git@github.com:lihongyi87/dophinscheduler.git
cd dophinscheduler

# 切换到开发分支
git checkout dev

# 查看项目结构
ls -la
```

## 2. 项目导入IDEA

### 2.1 项目结构说明

**改造后的项目结构：**
```
dolphinscheduler/
├── dolphinscheduler-api/              # REST API模块
├── dolphinscheduler-alert/            # 告警模块
├── dolphinscheduler-common/           # 公共模块
├── dolphinscheduler-dao/              # 数据访问层
├── dolphinscheduler-master/           # Master模块 ⭐️
├── dolphinscheduler-worker/           # Worker模块 ⭐️
├── dolphinscheduler-standalone-server/# 单机版服务器 ⭐️
├── dolphinscheduler-raft/             # 新增Raft模块 🆕
├── dolphinscheduler-task-plugin/      # 任务插件
├── dolphinscheduler-ui/               # 前端UI
├── docs/                              # 源码教材文档 🆕
│   ├── DolphinScheduler源码教材-模块架构篇.md
│   ├── DolphinScheduler源码教材-工作流执行流程篇.md
│   ├── DolphinScheduler源码教材-任务调度流程篇.md
│   ├── DolphinScheduler源码教材-故障处理流程篇.md
│   ├── DolphinScheduler源码教材-核心设计模式篇.md
│   ├── DolphinScheduler源码教材-性能调优实战篇.md
│   ├── DolphinScheduler源码教材-插件开发实战篇.md
│   ├── DolphinScheduler源码教材-Raft注册中心改造实战篇.md
│   └── DolphinScheduler源码教材-IDEA启动调试实战篇.md
├── pom.xml                           # Maven根配置文件
└── README.md                         # 项目说明
```

### 2.2 IDEA导入步骤

**Step 1: 打开IDEA导入项目**
```
1. 启动IntelliJ IDEA
2. 选择 "File" -> "Open"
3. 选择 dolphinscheduler 项目根目录
4. 点击 "OK"
```

**Step 2: Maven项目识别**
```
IDEA会自动识别这是一个Maven多模块项目
等待Maven依赖下载完成（首次可能需要10-20分钟）
```

**Step 3: 项目结构验证**
检查Project Structure中是否包含以下模块：
- dolphinscheduler-standalone-server ⭐️ (单机版启动模块)
- dolphinscheduler-master ⭐️ (分布式Master模块) 
- dolphinscheduler-worker ⭐️ (分布式Worker模块)
- dolphinscheduler-raft 🆕 (内嵌Raft模块)

## 3. 数据库配置

### 3.1 开发环境配置（推荐）

**使用内置H2数据库：**
```yaml
# dolphinscheduler-standalone-server/src/main/resources/application.yaml
spring:
  profiles:
    active: h2  # 使用H2内存数据库，无需额外配置
  datasource:
    driver-class-name: org.h2.Driver
    url: jdbc:h2:mem:dolphinscheduler;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=true
    username: sa
    password: ""
```

**优点：**
- ✅ 无需安装数据库
- ✅ 启动速度快
- ✅ 适合开发调试
- ✅ 数据隔离性好

### 3.2 生产环境配置（可选）

**使用MySQL数据库：**
```yaml
# application-mysql.yaml
spring:
  profiles:
    active: mysql
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://localhost:3306/dolphinscheduler?useSSL=false&serverTimezone=UTC
    username: dolphinscheduler
    password: dolphinscheduler123
```

## 4. IDEA运行配置

### 4.1 单机版启动配置（推荐新手）

**创建单机版运行配置：**
```
Name: DolphinScheduler-Standalone
Main class: org.apache.dolphinscheduler.StandaloneServer
Module: dolphinscheduler-standalone-server
Working directory: $MODULE_WORKING_DIR$

VM options:
-Xms1024m
-Xmx2048m
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-Dspring.profiles.active=h2
-Dlogging.config=src/main/resources/logback-spring.xml

Program arguments:
--spring.config.location=src/main/resources/application.yaml

Environment variables:
DOLPHINSCHEDULER_HOME=$MODULE_DIR$
```

### 4.2 分布式模式启动配置（高级用户）

**Master节点配置：**
```
Name: DolphinScheduler-Master
Main class: org.apache.dolphinscheduler.server.master.MasterServer
Module: dolphinscheduler-master

VM options:
-Xms1024m
-Xmx2048m
-XX:+UseG1GC
-Dspring.profiles.active=mysql
-Ddolphinscheduler.raft.enabled=true
```

**Worker节点配置：**
```
Name: DolphinScheduler-Worker
Main class: org.apache.dolphinscheduler.server.worker.WorkerServer
Module: dolphinscheduler-worker

VM options:
-Xms1024m
-Xmx2048m
-XX:+UseG1GC
-Dspring.profiles.active=mysql
```

**API Server配置：**
```
Name: DolphinScheduler-API
Main class: org.apache.dolphinscheduler.api.ApiApplicationServer
Module: dolphinscheduler-api

VM options:
-Xms512m
-Xmx1024m
-XX:+UseG1GC

Program arguments:
--server.port=12345
```

## 5. 启动和验证

### 5.1 单机模式启动（推荐）

**启动步骤：**
```
1. 选择 "DolphinScheduler-Standalone" 运行配置
2. 点击绿色运行按钮 ▶️ 
3. 观察控制台输出，等待启动完成
```

**启动成功日志示例：**
```
  ______       _       __    _         _____       __              __      __
 |  __  \     | |     / /   | |       |  ___|     |  \            /  |    |  |
 | |  \  |    | |    / /    | |__     | |___      |   \    /\    /   |    |  |
 | |   | |    | |   / /     |  __ \   |___  \     |    \  /  \  /    |    |  |
 | |__/  |____| |_ / /      | |  | |   ___| |     |  |\ \     /    | |    |  |
 |_______/|_______/_/       |_|  |_|  |____/      |__| \__\ /__/     |_|   |__|

DolphinScheduler Standalone Server启动成功!
内嵌Raft注册中心已就绪
Web界面访问地址: http://localhost:12345
默认账号: admin / dolphinscheduler123
```

### 5.2 验证启动状态

**检查服务状态：**
```bash
# 检查端口监听
netstat -tlnp | grep :12345

# 访问健康检查接口
curl http://localhost:12345/actuator/health

# 预期响应
{
  "status": "UP",
  "components": {
    "raft": {
      "status": "UP",
      "details": {
        "leader": "localhost:8888",
        "term": 1,
        "nodeState": "LEADER"
      }
    }
  }
}
```

**访问Web界面：**
```
访问地址: http://localhost:12345
默认账号: admin
默认密码: dolphinscheduler123
```

## 6. 核心功能验证

### 6.1 创建简单工作流

**Step 1: 登录系统**
- 访问 http://localhost:12345
- 使用 admin/dolphinscheduler123 登录

**Step 2: 创建项目**
- 点击"项目管理" -> "创建项目"
- 项目名称: test-project
- 项目描述: 测试项目

**Step 3: 创建工作流**
- 进入项目 -> 点击"工作流定义"
- 拖拽"Shell"任务到画布
- 配置Shell任务：
  ```bash
  #!/bin/bash
  echo "Hello DolphinScheduler!"
  echo "当前时间: $(date)"
  echo "系统信息: $(uname -a)"
  ```

**Step 4: 运行工作流**
- 点击"上线" -> 点击"运行"
- 观察执行状态和日志输出

## 7. 调试技巧和方法

### 7.1 断点调试设置

**关键断点位置：**
```java
// 1. 工作流启动入口
org.apache.dolphinscheduler.api.controller.WorkflowInstanceController.startWorkflowInstance()

// 2. Master调度处理（有详细中文注释）
org.apache.dolphinscheduler.server.master.engine.command.handler.RunWorkflowCommandHandler.handle()

// 3. 任务分发（有详细中文注释）
org.apache.dolphinscheduler.server.master.engine.task.client.TaskExecutorClientDelegator.dispatch()

// 4. Worker任务执行（有详细中文注释）
org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskExecutor.doTriggerTaskPlugin()

// 5. Raft选举过程（新增模块）
org.apache.dolphinscheduler.raft.core.RaftRegistryStateMachine.onApply()
```

### 7.2 日志调试技巧

**调整日志级别：**
```yaml
# src/main/resources/logback-spring.xml
logging:
  level:
    org.apache.dolphinscheduler.server.master: DEBUG    # Master调试日志
    org.apache.dolphinscheduler.server.worker: DEBUG    # Worker调试日志
    org.apache.dolphinscheduler.raft: DEBUG             # Raft调试日志
```

**查看关键日志：**
```bash
# 实时查看日志
tail -f logs/dolphinscheduler.log

# 过滤工作流相关日志
grep -E "(WorkflowInstance|TaskInstance)" logs/dolphinscheduler.log

# 查看Raft选举日志
grep -E "(RaftNode|Election|Leader)" logs/dolphinscheduler.log
```

### 7.3 Raft调试专题

**监控Raft状态：**
```bash
# 查看Raft集群状态
curl http://localhost:12345/actuator/raft/cluster

# 监控选举过程
curl http://localhost:12345/actuator/raft/health
```

**关键Raft断点：**
```java
// 选举过程
org.apache.dolphinscheduler.raft.core.RaftNode.startElection()

// 日志复制
org.apache.dolphinscheduler.raft.core.RaftNode.replicateLog()

// 状态机应用
org.apache.dolphinscheduler.raft.core.RaftRegistryStateMachine.onApply()
```

## 8. 常见问题解决

### 8.1 启动失败问题

**问题1: 端口占用**
```bash
# 解决方案：修改端口配置
server.port=12346
```

**问题2: 内存不足**
```bash
# 解决方案：调整JVM参数
-Xms512m
-Xmx1024m
```

**问题3: Maven依赖问题**
```bash
# 解决方案：清理重新下载
mvn clean install -U
```

### 8.2 Raft相关问题

**问题1: Raft选举失败**
```bash
# 检查Raft配置
dolphinscheduler.raft.enabled=true
dolphinscheduler.raft.port=8888
```

**问题2: 集群脑裂**
```bash
# 检查节点配置，确保奇数个节点
dolphinscheduler.raft.cluster-nodes=node1:8888,node2:8889,node3:8890
```

## 9. 源码学习路径

### 9.1 推荐学习顺序

**基础架构理解：**
1. 📖 [模块架构篇](./DolphinScheduler源码教材-模块架构篇.md)
2. 📖 [核心设计模式篇](./DolphinScheduler源码教材-核心设计模式篇.md)

**核心流程掌握：**
3. 📖 [工作流执行流程篇](./DolphinScheduler源码教材-工作流执行流程篇.md)
4. 📖 [任务调度流程篇](./DolphinScheduler源码教材-任务调度流程篇.md)
5. 📖 [故障处理流程篇](./DolphinScheduler源码教材-故障处理流程篇.md)

**高级特性学习：**
6. 📖 [Raft注册中心改造实战篇](./DolphinScheduler源码教材-Raft注册中心改造实战篇.md)
7. 📖 [插件开发实战篇](./DolphinScheduler源码教材-插件开发实战篇.md)
8. 📖 [性能调优实战篇](./DolphinScheduler源码教材-性能调优实战篇.md)

### 9.2 调试建议

**源码阅读技巧：**
- ✅ 充分利用80%+的中文注释
- ✅ 结合生活化类比理解复杂概念  
- ✅ 使用断点跟踪完整执行流程
- ✅ 参考教材文档加深理解

**实践建议：**
- 🎯 先运行单机版理解基本流程
- 🎯 再配置分布式版本理解集群机制
- 🎯 通过修改配置观察系统行为变化
- 🎯 尝试开发简单的自定义插件

## 小结

通过本教材，您已经掌握了：

### 🎯 核心技能

1. **完整开发环境搭建**：基于改造后项目的环境配置
2. **IDEA项目导入和运行**：多种启动模式的配置
3. **内嵌Raft集群验证**：去中心化架构的验证方法
4. **源码级调试技巧**：基于80%+中文注释的深度调试
5. **常见问题解决**：快速定位和解决启动问题

### 🚀 项目优势

- **零依赖启动**：无需外部注册中心，一键启动完整系统
- **中文注释丰富**：80%+注释覆盖率，便于学习理解
- **完整教材体系**：9个教材涵盖各个方面
- **实战导向**：基于实际改造项目的真实体验

### 📈 进阶路径

- **深度源码学习**：结合中文注释和教材深入理解
- **功能定制开发**：基于插件化架构扩展功能
- **性能调优实践**：通过监控和调优提升性能
- **分布式系统设计**：学习Raft算法在生产环境的应用

现在您可以轻松地启动和调试这个去中心化的DolphinScheduler了！这为深入学习分布式调度系统和进行二次开发打下了坚实的基础。