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

package org.apache.dolphinscheduler.server.worker.utils;

import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.plugin.storage.api.ResourceMetadata;
import org.apache.dolphinscheduler.plugin.storage.api.StorageOperator;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.resource.ResourceContext;
import org.apache.dolphinscheduler.server.worker.metrics.WorkerServerMetrics;

import org.apache.commons.collections4.CollectionUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务执行上下文工具类
 *
 * 此工具类提供任务执行过程中所需的上下文环境准备和资源管理功能。
 * 任务执行上下文包含了任务运行所需的所有信息和环境配置，确保任务能够
 * 在正确的环境中执行。
 *
 * 主要功能：
 * 1. 创建任务实例的专属工作目录，确保任务执行的隔离性
 * 2. 下载任务执行所需的资源文件到本地工作目录
 * 3. 管理资源文件的本地化和权限设置
 * 4. 提供资源文件的路径映射和上下文信息
 *
 * 任务执行上下文的作用：
 * - 为每个任务实例提供独立的执行环境
 * - 管理任务依赖的文件资源和配置参数
 * - 确保任务执行时的安全隔离和资源访问控制
 * - 提供任务执行所需的路径、权限、环境变量等信息
 */
@Slf4j
public class TaskExecutionContextUtils {

    /**
     * 为任务实例创建专属的工作目录
     *
     * 此方法为每个任务实例创建一个独立的工作目录，确保不同任务实例之间的执行环境隔离。
     * 工作目录是任务执行的基础，包含任务脚本、临时文件、日志文件等。
     *
     * 执行流程：
     * 1. 根据任务实例ID生成唯一的工作目录路径
     * 2. 如果目录已存在则先删除，确保环境干净
     * 3. 创建新的工作目录并设置适当的权限（775）
     * 4. 设置任务执行上下文的执行路径和应用信息路径
     *
     * 目录权限说明：
     * - 775权限：所有者和组具有读写执行权限，其他用户具有读执行权限
     * - 确保任务执行时有足够的权限访问工作目录
     * - 同时保证系统安全性，防止权限过度开放
     *
     * @param taskExecutionContext 任务执行上下文，包含任务实例ID等信息
     * @throws TaskException 如果创建工作目录失败
     */
    public static void createTaskInstanceWorkingDirectory(TaskExecutionContext taskExecutionContext) throws TaskException {
        // 根据任务实例ID生成本地执行路径
        // 使用任务实例ID作为目录名，确保每个任务实例拥有独立的工作空间
        String taskInstanceWorkingDirectory =
                FileUtils.getTaskInstanceWorkingDirectory(taskExecutionContext.getTaskInstanceId());
        try {
            // 如果工作目录已存在则先删除，确保环境干净
            // 避免上次任务执行残留的文件影响当前任务执行
            if (new File(taskInstanceWorkingDirectory).exists()) {
                // 递归删除目录及其所有子文件和子目录
                FileUtils.deleteFile(taskInstanceWorkingDirectory);
                log.warn("The TaskInstance WorkingDirectory: {} is exist, will recreate again",
                        taskInstanceWorkingDirectory);
            }

            // 创建工作目录并设置775权限
            // 775权限说明：所有者(7=rwx)和组(7=rwx)具有完全权限，其他用户(5=r-x)具有读取和执行权限
            // 这样确保任务执行进程能够在目录中创建、修改、删除文件，同时保持适当的安全性
            FileUtils.createDirectoryWithPermission(Paths.get(taskInstanceWorkingDirectory), FileUtils.PERMISSION_775);

            // 设置任务执行上下文的路径信息
            // 设置任务执行路径，供后续任务执行时使用
            taskExecutionContext.setExecutePath(taskInstanceWorkingDirectory);
            // 设置应用信息路径，用于存储任务执行过程中的应用相关信息文件
            taskExecutionContext.setAppInfoPath(FileUtils.getAppInfoPath(taskInstanceWorkingDirectory));
        } catch (Throwable ex) {
            // 捕获所有异常并转换为TaskException，提供统一的异常处理机制
            // 包装原始异常信息，便于问题定位和调试
            throw new TaskException(
                    "Cannot create TaskInstance WorkingDirectory: " + taskInstanceWorkingDirectory + " failed", ex);
        }
    }

    /**
     * 根据需要下载任务执行所需的资源文件
     *
     * 此方法负责将任务依赖的资源文件从存储系统下载到本地工作目录，
     * 确保任务执行时能够访问到所需的资源文件。资源文件可能包括脚本文件、
     * 配置文件、数据文件等。
     *
     * 执行流程：
     * 1. 解析任务参数，获取资源文件列表
     * 2. 检查本地是否已存在资源文件，避免重复下载
     * 3. 从存储系统下载不存在的资源文件到本地工作目录
     * 4. 设置资源文件的执行权限（755）
     * 5. 记录下载性能指标（时间、大小、成功/失败次数）
     * 6. 构建资源上下文，提供资源文件的路径映射信息
     *
     * 资源管理机制：
     * - 建立存储路径与本地路径的映射关系
     * - 保持资源文件的相对路径结构
     * - 设置适当的文件权限确保任务能够访问
     * - 记录详细的下载指标用于监控和优化
     *
     * @param taskChannel 任务通道，用于解析任务参数
     * @param storageOperator 存储操作器，用于从存储系统下载资源
     * @param taskExecutionContext 任务执行上下文，包含工作目录等信息
     * @return 资源上下文，包含所有资源文件的路径映射信息
     * @throws TaskException 如果下载资源文件失败
     */
    public static ResourceContext downloadResourcesIfNeeded(TaskChannel taskChannel,
                                                            StorageOperator storageOperator,
                                                            TaskExecutionContext taskExecutionContext) {
        // 解析任务参数，获取资源文件列表
        // 通过任务通道解析任务的具体参数，提取出任务执行所需的所有资源文件信息
        AbstractParameters abstractParameters = taskChannel.parseParameters(taskExecutionContext.getTaskParams());
        // 获取资源文件列表，包含每个资源文件的名称、路径等信息
        List<ResourceInfo> resourceFilesList = abstractParameters.getResourceFilesList();

        // 如果没有资源文件需要下载，返回空的资源上下文
        if (CollectionUtils.isEmpty(resourceFilesList)) {
            log.debug("There is no resource file need to download");
            return new ResourceContext();
        }

        // 创建资源上下文对象，用于管理资源文件的路径映射关系
        ResourceContext resourceContext = new ResourceContext();
        // 获取任务的工作目录路径，资源文件将下载到此目录下
        String taskWorkingDirectory = taskExecutionContext.getExecutePath();

        // 遍历并下载每个资源文件
        for (ResourceInfo resourceInfo : resourceFilesList) {
            // 获取资源文件在存储系统中的绝对路径
            String resourceAbsolutePathInStorage = resourceInfo.getResourceName();
            // 从存储操作器获取资源元数据，包含相对路径、大小、修改时间等信息
            ResourceMetadata resourceMetaData = storageOperator.getResourceMetaData(resourceAbsolutePathInStorage);
            // 构建资源文件在本地的绝对路径，保持原有的目录结构
            String resourceAbsolutePathInLocal =
                    Paths.get(taskWorkingDirectory, resourceMetaData.getResourceRelativePath()).toString();
            // 创建本地文件对象，用于检查文件是否存在
            File file = new File(resourceAbsolutePathInLocal);

            // 检查文件是否已存在，避免重复下载
            // 这样可以提高效率，特别是在任务重试或多次执行时
            if (!file.exists()) {
                try {
                    // 记录下载开始时间，用于性能监控
                    // 用于计算资源下载耗时，便于性能分析和优化
                    long resourceDownloadStartTime = System.currentTimeMillis();

                    // 从存储系统下载资源文件到本地
                    // 第三个参数true表示如果本地目录不存在则自动创建
                    storageOperator.download(resourceAbsolutePathInStorage, resourceAbsolutePathInLocal, true);
                    log.info("Download resource file {} -> {} successfully", resourceAbsolutePathInStorage,
                            resourceAbsolutePathInLocal);

                    // 设置文件执行权限为755
                    // 755权限：所有者具有读写执行权限，组和其他用户具有读执行权限
                    // 确保任务执行时能够正确访问和执行资源文件
                    FileUtils.setFileTo755(file);

                    // 记录下载性能指标
                    // 记录资源下载耗时，用于性能监控和分析
                    WorkerServerMetrics
                            .recordWorkerResourceDownloadTime(System.currentTimeMillis() - resourceDownloadStartTime);
                    // 记录下载文件的大小，用于网络带宽和存储使用情况分析
                    WorkerServerMetrics
                            .recordWorkerResourceDownloadSize(Files.size(Paths.get(resourceAbsolutePathInLocal)));
                    // 增加资源下载成功计数器
                    WorkerServerMetrics.incWorkerResourceDownloadSuccessCount();
                } catch (Exception ex) {
                    // 记录下载失败指标
                    // 增加资源下载失败计数器，用于监控下载成功率
                    WorkerServerMetrics.incWorkerResourceDownloadFailureCount();
                    // 抛出TaskException，包装原始异常信息
                    throw new TaskException(
                            String.format("Download resource file: %s error", resourceAbsolutePathInStorage), ex);
                }
            }

            // 构建资源项目，建立存储路径与本地路径的映射
            // 无论文件是否需要下载，都要建立路径映射关系供任务执行时使用
            ResourceContext.ResourceItem resourceItem = ResourceContext.ResourceItem.builder()
                    .resourceAbsolutePathInStorage(resourceAbsolutePathInStorage)
                    .resourceAbsolutePathInLocal(resourceAbsolutePathInLocal)
                    .build();
            // 将资源项添加到资源上下文中，供后续任务执行时查找和使用
            resourceContext.addResourceItem(resourceItem);
        }
        // 返回包含所有资源文件路径映射的资源上下文
        return resourceContext;
    }

}
