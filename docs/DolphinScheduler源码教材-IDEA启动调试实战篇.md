# DolphinScheduler源码教材 - IDEA启动调试实战篇

## 概述

本教材详细讲解如何在IntelliJ IDEA中导入、配置、启动和调试DolphinScheduler源码项目。通过这个教材，您将学会在开发环境中运行完整的分布式调度系统，并掌握源码级调试技巧。

## 1. 环境准备

### 1.1 软件环境要求

**必需软件列表：**
```yaml
development_environment:
  jdk: "JDK 1.8 或 JDK 11 (推荐JDK 1.8)"
  ide: "IntelliJ IDEA 2020.3+ (推荐Ultimate版)"
  maven: "Apache Maven 3.6+"
  database: "MySQL 8.0+ (开发测试用)"
  git: "Git 2.20+"
  
optional_tools:
  docker: "Docker 20.10+ (用于快速启动依赖服务)"
  node_js: "Node.js 16+ (前端开发)"
```

**硬件建议配置：**
```yaml
hardware_requirements:
  cpu: "4核心以上"
  memory: "8GB以上 (推荐16GB)"
  disk: "至少20GB可用空间"
  network: "稳定的网络连接"
```

### 1.2 JDK环境配置

**配置JAVA_HOME：**
```bash
# Windows系统
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_301
set PATH=%JAVA_HOME%\bin;%PATH%

# Linux/MacOS系统
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# 验证Java版本
java -version
javac -version
```

**验证输出示例：**
```
java version "1.8.0_301"
Java(TM) SE Runtime Environment (build 1.8.0_301-b09)
Java HotSpot(TM) 64-Bit Server VM (build 25.301-b09, mixed mode)
```

### 1.3 Maven环境配置

**配置Maven镜像源：**
```xml
<!-- ~/.m2/settings.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<settings>
    <mirrors>
        <mirror>
            <id>aliyun-maven</id>
            <name>Aliyun Maven</name>
            <url>https://maven.aliyun.com/repository/public</url>
            <mirrorOf>central</mirrorOf>
        </mirror>
    </mirrors>
    
    <profiles>
        <profile>
            <id>jdk-1.8</id>
            <activation>
                <activeByDefault>true</activeByDefault>
                <jdk>1.8</jdk>
            </activation>
            <properties>
                <maven.compiler.source>1.8</maven.compiler.source>
                <maven.compiler.target>1.8</maven.compiler.target>
                <maven.compiler.compilerVersion>1.8</maven.compiler.compilerVersion>
            </properties>
        </profile>
    </profiles>
</settings>
```

## 2. 项目导入IDEA

### 2.1 获取源码

**方法1：Git克隆（推荐）**
```bash
# 从GitHub克隆最新源码
git clone https://github.com/apache/dolphinscheduler.git
cd dolphinscheduler

# 切换到稳定版本分支
git checkout 3.2.0

# 查看项目结构
ls -la
```

**方法2：下载源码包**
1. 访问 https://github.com/apache/dolphinscheduler/releases
2. 下载对应版本的Source code (zip)
3. 解压到本地目录

### 2.2 IDEA项目导入步骤

**Step 1: 打开IDEA导入项目**
```
1. 启动IntelliJ IDEA
2. 选择 "File" -> "Open" 或 "Import Project"
3. 选择dolphinscheduler项目根目录
4. 点击 "OK"
```

**Step 2: 选择导入方式**
```
1. 选择 "Import project from external model"
2. 选择 "Maven"
3. 点击 "Create"
```

**Step 3: Maven导入配置**
```
✓ Search for projects recursively
✓ Import Maven projects automatically
✓ Create module groups for multi-module Maven projects
✓ Create separate module per source set

Maven settings:
- Maven home directory: [选择Maven安装路径]
- User settings file: [选择settings.xml路径]  
- Local repository: [选择本地仓库路径]
```

**Step 4: 等待项目导入完成**
```
导入过程需要10-30分钟，取决于网络速度和机器性能
IDEA会自动下载依赖包，请耐心等待
```

### 2.3 项目结构解析

导入完成后的项目结构：
```
dolphinscheduler/
├── dolphinscheduler-api/              # REST API模块
├── dolphinscheduler-alert/            # 告警模块
├── dolphinscheduler-common/           # 公共模块
├── dolphinscheduler-dao/              # 数据访问层
├── dolphinscheduler-datasource-plugin/# 数据源插件
├── dolphinscheduler-dist/             # 打包分发模块
├── dolphinscheduler-master/           # Master模块 ⭐️
├── dolphinscheduler-worker/           # Worker模块 ⭐️
├── dolphinscheduler-standalone-server/# 单机版服务器 ⭐️
├── dolphinscheduler-task-plugin/      # 任务插件
├── dolphinscheduler-ui/               # 前端UI
├── tools/                             # 工具脚本
├── pom.xml                           # Maven根配置文件
└── README.md                         # 项目说明
```

## 3. 数据库环境配置

### 3.1 MySQL数据库准备

**方法1：本地MySQL安装**
```sql
-- 创建数据库
CREATE DATABASE dolphinscheduler DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;

-- 创建用户并授权
CREATE USER 'dolphinscheduler'@'%' IDENTIFIED BY 'dolphinscheduler123';
GRANT ALL PRIVILEGES ON dolphinscheduler.* TO 'dolphinscheduler'@'%';
FLUSH PRIVILEGES;
```

**方法2：Docker快速启动MySQL**
```bash
# 启动MySQL容器
docker run -d \
  --name mysql-dolphin \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_DATABASE=dolphinscheduler \
  -e MYSQL_USER=dolphinscheduler \
  -e MYSQL_PASSWORD=dolphinscheduler123 \
  mysql:8.0

# 验证数据库连接
docker exec -it mysql-dolphin mysql -udolphinscheduler -pdolphinscheduler123 -e "SHOW DATABASES;"
```

### 3.2 数据库初始化脚本

**自动化数据库初始化：**
```bash
# 进入项目根目录
cd dolphinscheduler

# 执行数据库初始化脚本
bash tools/bin/upgrade-schema.sh
```

**手动初始化（如果自动脚本失败）：**
```bash
# 1. 找到SQL脚本位置
ls tools/sql/

# 2. 执行基础表结构脚本
mysql -h127.0.0.1 -P3306 -udolphinscheduler -pdolphinscheduler123 dolphinscheduler < tools/sql/dolphinscheduler_mysql.sql

# 3. 执行升级脚本（如有需要）
mysql -h127.0.0.1 -P3306 -udolphinscheduler -pdolphinscheduler123 dolphinscheduler < tools/sql/upgrade/3.2.0_schema/mysql/dolphinscheduler_ddl.sql
```

## 4. 配置文件设置

### 4.1 数据源配置

**编辑application.yaml配置：**
```yaml
# dolphinscheduler-standalone-server/src/main/resources/application.yaml
spring:
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://127.0.0.1:3306/dolphinscheduler?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useSSL=false&serverTimezone=UTC
    username: dolphinscheduler
    password: dolphinscheduler123
    hikari:
      connection-test-query: select 1
      pool-name: DolphinScheduler
      maximum-pool-size: 30
      minimum-idle: 5
      connection-timeout: 180000
      validation-timeout: 3000
      idle-timeout: 600000
      leak-detection-threshold: 60000
      initialization-fail-timeout: 1

# 注册中心配置
registry:
  type: standalone  # 单机模式无需外部注册中心
```

### 4.2 日志配置

**编辑logback-spring.xml：**
```xml
<!-- dolphinscheduler-standalone-server/src/main/resources/logback-spring.xml -->
<configuration>
    <!-- 控制台输出 -->
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <!-- 文件输出 -->
    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/dolphinscheduler.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/dolphinscheduler.%d{yyyy-MM-dd}.%i.log.gz</fileNamePattern>
            <maxFileSize>100MB</maxFileSize>
            <maxHistory>30</maxHistory>
            <totalSizeCap>1GB</totalSizeCap>
        </rollingPolicy>
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <!-- 设置日志级别 -->
    <logger name="org.apache.dolphinscheduler" level="INFO"/>
    <logger name="org.springframework" level="WARN"/>
    <logger name="com.zaxxer.hikari" level="WARN"/>
    
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
        <appender-ref ref="FILE"/>
    </root>
</configuration>
```

## 5. IDEA运行配置

### 5.1 创建运行配置

**Step 1: 添加运行配置**
```
1. 点击IDEA右上角的运行配置下拉菜单
2. 选择 "Edit Configurations..."
3. 点击左上角的 "+" 号
4. 选择 "Application"
```

**Step 2: 配置单机版启动器**
```
Name: DolphinScheduler-Standalone
Main class: org.apache.dolphinscheduler.StandaloneServer
Module: dolphinscheduler-standalone-server
Working directory: $MODULE_WORKING_DIR$
Use classpath of module: dolphinscheduler-standalone-server

VM options:
-Xms1024m
-Xmx2048m
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-Dspring.profiles.active=standalone
-Dlogging.config=src/main/resources/logback-spring.xml

Program arguments: 
--spring.config.location=src/main/resources/application.yaml

Environment variables:
DOLPHINSCHEDULER_HOME=$MODULE_DIR$
```

### 5.2 Master单独启动配置

**创建Master运行配置：**
```
Name: DolphinScheduler-Master
Main class: org.apache.dolphinscheduler.server.master.MasterServer
Module: dolphinscheduler-master

VM options:
-Xms1024m
-Xmx2048m
-XX:+UseG1GC
-Dspring.profiles.active=master
-DDOLPHINSCHEDULER_HOME=$MODULE_DIR$
```

### 5.3 Worker单独启动配置

**创建Worker运行配置：**
```
Name: DolphinScheduler-Worker
Main class: org.apache.dolphinscheduler.server.worker.WorkerServer
Module: dolphinscheduler-worker

VM options:
-Xms1024m
-Xmx2048m
-XX:+UseG1GC
-Dspring.profiles.active=worker
-DDOLPHINSCHEDULER_HOME=$MODULE_DIR$
```

### 5.4 API Server启动配置

**创建API运行配置：**
```
Name: DolphinScheduler-API
Main class: org.apache.dolphinscheduler.api.ApiApplicationServer
Module: dolphinscheduler-api

VM options:
-Xms512m
-Xmx1024m
-XX:+UseG1GC
-Dspring.profiles.active=api

Program arguments:
--server.port=12345
```

## 6. 启动和验证

### 6.1 单机模式启动（推荐新手）

**Step 1: 启动单机服务**
```
1. 选择 "DolphinScheduler-Standalone" 运行配置
2. 点击绿色运行按钮或按 Shift+F10
3. 观察控制台输出，等待服务启动完成
```

**启动成功日志示例：**
```
2024-01-20 10:30:45.123  INFO [main] o.a.d.StandaloneServer - Starting StandaloneServer
2024-01-20 10:30:47.456  INFO [main] o.s.b.w.embedded.tomcat.TomcatWebServer - Tomcat started on port(s): 12345 (http)
2024-01-20 10:30:47.789  INFO [main] o.a.d.StandaloneServer - Started StandaloneServer in 12.345 seconds
2024-01-20 10:30:48.012  INFO [WorkflowExecuteThread] o.a.d.s.m.e.WorkflowExecuteRunnable - Workflow execute thread started
2024-01-20 10:30:48.234  INFO [TaskExecuteThread] o.a.d.s.w.r.TaskExecuteRunnable - Task execute thread started
```

**Step 2: 验证服务状态**
```bash
# 检查端口是否正常监听
netstat -tlnp | grep :12345

# 或使用curl测试健康检查接口
curl http://localhost:12345/actuator/health

# 预期响应
{"status":"UP"}
```

### 6.2 分布式模式启动（高级用户）

**启动顺序：**
```
1. 先启动数据库和注册中心(Zookeeper)
2. 启动 DolphinScheduler-Master
3. 启动 DolphinScheduler-Worker  
4. 启动 DolphinScheduler-API
```

**注册中心配置（使用Zookeeper）：**
```yaml
registry:
  type: zookeeper
  zookeeper:
    namespace: dolphinscheduler
    connect-string: localhost:2181
    retry-policy:
      base-sleep-time: 60ms
      max-sleep: 300ms
      max-retries: 5
    session-timeout: 30s
    connection-timeout: 9s
    block-until-connected: 600ms
```

### 6.3 前端UI启动

**方法1: 使用内置前端**
```bash
# 单机模式已内置前端，直接访问
http://localhost:12345

# 默认登录账号
用户名: admin
密码: dolphinscheduler123
```

**方法2: 单独启动前端开发服务**
```bash
# 进入前端目录
cd dolphinscheduler-ui

# 安装依赖
npm install

# 启动开发服务器
npm run dev

# 访问前端界面
http://localhost:8888
```

## 7. 调试技巧和方法

### 7.1 断点调试设置

**设置关键断点位置：**
```java
// 1. 工作流触发入口
org.apache.dolphinscheduler.api.controller.WorkflowInstanceController.startWorkflowInstance()

// 2. Master命令处理
org.apache.dolphinscheduler.server.master.runner.MasterSchedulerService.masterSchedulerService()

// 3. 任务分发
org.apache.dolphinscheduler.server.master.dispatch.ExecutorDispatcher.dispatch()

// 4. Worker任务执行
org.apache.dolphinscheduler.server.worker.runner.TaskExecuteThread.run()

// 5. 状态更新
org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable.updateWorkflowState()
```

### 7.2 日志调试技巧

**临时调整日志级别：**
```yaml
# 在application.yaml中添加
logging:
  level:
    org.apache.dolphinscheduler.server.master: DEBUG
    org.apache.dolphinscheduler.server.worker: DEBUG
    org.apache.dolphinscheduler.dao: DEBUG
```

**查看关键日志：**
```bash
# 实时查看日志
tail -f logs/dolphinscheduler.log

# 过滤特定日志
grep -E "(WorkflowInstance|TaskInstance)" logs/dolphinscheduler.log

# 查看错误日志
grep -E "(ERROR|Exception)" logs/dolphinscheduler.log
```

### 7.3 数据库调试

**常用SQL查询：**
```sql
-- 查看工作流实例
SELECT * FROM t_ds_process_instance ORDER BY start_time DESC LIMIT 10;

-- 查看任务实例
SELECT * FROM t_ds_task_instance WHERE process_instance_id = ? ORDER BY start_time DESC;

-- 查看命令队列
SELECT * FROM t_ds_command ORDER BY create_time DESC;

-- 查看错误日志
SELECT * FROM t_ds_task_instance WHERE state = 6 ORDER BY end_time DESC LIMIT 10;
```

### 7.4 性能调试

**JVM参数调优：**
```bash
# 启用JFR性能记录
-XX:+FlightRecorder
-XX:StartFlightRecording=duration=60s,filename=dolphin.jfr

# 启用GC日志
-XX:+PrintGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Xloggc:gc.log

# 内存dump设置
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=./heapdump.hprof
```

## 8. 常见问题解决

### 8.1 启动失败问题

**问题1: 端口占用**
```bash
# 解决方案：查找并杀掉占用进程
lsof -i :12345
kill -9 [PID]

# 或者修改端口配置
server.port=12346
```

**问题2: 数据库连接失败**
```bash
# 检查数据库服务状态
systemctl status mysql
# 或
docker ps | grep mysql

# 测试数据库连接
mysql -h127.0.0.1 -P3306 -udolphinscheduler -pdolphinscheduler123 -e "SELECT 1;"
```

**问题3: 依赖下载失败**
```bash
# 清理Maven缓存重新下载
mvn clean install -U

# 或者删除本地仓库重新下载
rm -rf ~/.m2/repository/org/apache/dolphinscheduler
mvn install
```

### 8.2 内存不足问题

**增加JVM内存设置：**
```
VM options:
-Xms2048m
-Xmx4096m
-XX:MetaspaceSize=256m
-XX:MaxMetaspaceSize=512m
```

**IDEA内存配置：**
```
# 修改 idea.vmoptions 文件
-Xms2048m
-Xmx4096m
-XX:ReservedCodeCacheSize=1024m
```

### 8.3 编码问题

**设置统一编码：**
```
File -> Settings -> Editor -> File Encodings
- Global Encoding: UTF-8
- Project Encoding: UTF-8  
- Default encoding for properties files: UTF-8
- ✓ Transparent native-to-ascii conversion
```

## 9. 开发环境优化

### 9.1 IDEA插件推荐

**必装插件：**
```
1. Lombok - 简化Java代码
2. MyBatis Log Plugin - MyBatis SQL日志美化
3. Maven Helper - Maven依赖管理
4. GitToolBox - Git增强工具
5. SonarLint - 代码质量检查
6. Translation - 中英文翻译
7. Rainbow Brackets - 彩虹括号
8. Alibaba Java Coding Guidelines - 阿里Java规范
```

### 9.2 代码格式化配置

**导入代码格式化规则：**
```
1. 下载 dolphinscheduler-codestyle.xml
2. File -> Settings -> Editor -> Code Style
3. 点击齿轮图标 -> Import Scheme -> IntelliJ IDEA code style XML
4. 选择下载的xml文件导入
```

### 9.3 Git配置

**配置Git忽略文件：**
```gitignore
# IDE
.idea/
*.iml
*.ipr
*.iws

# Build
target/
build/
out/

# Logs
logs/
*.log

# OS
.DS_Store
Thumbs.db

# DolphinScheduler specific
dolphinscheduler_env.sh
```

## 10. 生产调试技巧

### 10.1 远程调试配置

**启用远程调试：**
```bash
# 在生产环境启动参数中添加
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

# IDEA中配置Remote Debug
Run -> Edit Configurations -> + -> Remote JVM Debug
Host: [生产服务器IP]
Port: 5005
```

### 10.2 线程dump分析

**获取线程dump：**
```bash
# 方法1: jstack命令
jstack [PID] > thread_dump.txt

# 方法2: kill命令
kill -3 [PID]

# 方法3: JVisualVM工具
jvisualvm -> 选择进程 -> 线程 -> 线程Dump
```

### 10.3 内存dump分析

**获取内存dump：**
```bash
# 使用jmap命令
jmap -dump:live,format=b,file=heap_dump.hprof [PID]

# 使用jcmd命令
jcmd [PID] GC.run_finalization
jcmd [PID] VM.gc -full
jcmd [PID] GC.dump heap_dump.hprof
```

**分析工具：**
```
1. Eclipse MAT (Memory Analyzer Tool)
2. JVisualVM
3. JProfiler
4. YourKit Java Profiler
```

## 11. 单元测试和集成测试

### 11.1 运行单元测试

**在IDEA中运行测试：**
```
1. 右键点击测试类或测试方法
2. 选择 "Run '[TestName]'"
3. 查看测试结果和覆盖率

# 或使用Maven命令
mvn test
mvn test -Dtest=WorkflowInstanceControllerTest
```

### 11.2 集成测试配置

**配置测试数据库：**
```yaml
# src/test/resources/application-test.yaml
spring:
  profiles:
    active: test
  datasource:
    url: jdbc:h2:mem:testdb;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=true
    driver-class-name: org.h2.Driver
    username: sa
    password: 
```

### 11.3 API测试

**使用PostMan或IDEA HTTP Client：**
```http
### 登录获取token
POST http://localhost:12345/dolphinscheduler/login
Content-Type: application/x-www-form-urlencoded

userName=admin&userPassword=dolphinscheduler123

### 创建工作流
POST http://localhost:12345/dolphinscheduler/projects/test-project/process-definition
Authorization: token [从登录接口获取]
Content-Type: application/json

{
  "name": "test-workflow",
  "description": "测试工作流",
  "processDefinitionJson": "{...}"
}
```

## 12. 开发最佳实践

### 12.1 代码调试流程

**标准调试步骤：**
```
1. 🎯 确定问题范围 (工作流/任务/调度/执行)
2. 🔍 查看相关日志 (定位错误信息)
3. 📍 设置关键断点 (业务逻辑关键节点)
4. 🏃 重现问题场景 (最小化复现步骤)
5. 🔧 分析调用栈 (理解执行路径)
6. ✅ 验证修复效果 (回归测试)
```

### 12.2 性能调试技巧

**性能瓶颈定位：**
```java
// 1. 使用StopWatch测量方法执行时间
StopWatch stopWatch = new StopWatch();
stopWatch.start("methodName");
// ... 业务代码
stopWatch.stop();
logger.info("Method execution time: {}", stopWatch.getTotalTimeMillis());

// 2. 使用Micrometer进行度量
@Timed(name = "workflow.execution.time", description = "Workflow execution time")
public void executeWorkflow() {
    // 业务代码
}

// 3. SQL执行时间监控
// 在logback配置中开启SQL日志
<logger name="org.apache.ibatis" level="DEBUG"/>
```

### 12.3 错误处理调试

**异常调试策略：**
```java
try {
    // 业务代码
} catch (Exception e) {
    // 1. 记录完整异常信息
    logger.error("Business operation failed, context: {}", context, e);
    
    // 2. 分析异常类型和原因
    if (e instanceof SQLException) {
        // 数据库相关处理
    } else if (e instanceof RemoteException) {
        // 网络相关处理
    }
    
    // 3. 向上抛出业务异常
    throw new BusinessException("Operation failed", e);
}
```

## 小结

通过这个IDEA启动调试教材，您已经掌握了：

### 🎯 核心技能

1. **完整开发环境搭建**：从JDK到数据库的完整配置
2. **IDEA项目导入和配置**：Maven项目的标准导入流程
3. **多种启动方式**：单机模式和分布式模式启动
4. **调试技巧精通**：断点调试、日志调试、性能调试
5. **问题排查能力**：常见问题的快速定位和解决

### 🚀 实用价值

- **快速上手**：按照教材可在30分钟内启动完整系统
- **深度学习**：通过调试理解源码执行流程
- **问题解决**：具备生产环境问题排查能力
- **开发效率**：掌握高效的开发和调试工具

### 📈 进阶路径

- **源码阅读**：结合断点调试深入理解代码逻辑
- **功能扩展**：在现有基础上开发新功能
- **性能优化**：通过性能调试工具优化系统性能
- **问题定位**：快速定位和修复生产问题

现在您可以轻松地在IDEA中启动和调试DolphinScheduler了！这为深入学习源码和进行二次开发打下了坚实的基础。