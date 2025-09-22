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

package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.dto.resources.CreateDirectoryDto;
import org.apache.dolphinscheduler.api.dto.resources.CreateDirectoryRequest;
import org.apache.dolphinscheduler.api.dto.resources.CreateFileDto;
import org.apache.dolphinscheduler.api.dto.resources.CreateFileFromContentDto;
import org.apache.dolphinscheduler.api.dto.resources.CreateFileFromContentRequest;
import org.apache.dolphinscheduler.api.dto.resources.CreateFileRequest;
import org.apache.dolphinscheduler.api.dto.resources.DeleteResourceDto;
import org.apache.dolphinscheduler.api.dto.resources.DeleteResourceRequest;
import org.apache.dolphinscheduler.api.dto.resources.DownloadFileDto;
import org.apache.dolphinscheduler.api.dto.resources.DownloadFileRequest;
import org.apache.dolphinscheduler.api.dto.resources.FetchFileContentDto;
import org.apache.dolphinscheduler.api.dto.resources.FetchFileContentRequest;
import org.apache.dolphinscheduler.api.dto.resources.PagingResourceItemRequest;
import org.apache.dolphinscheduler.api.dto.resources.QueryResourceDto;
import org.apache.dolphinscheduler.api.dto.resources.RenameDirectoryDto;
import org.apache.dolphinscheduler.api.dto.resources.RenameDirectoryRequest;
import org.apache.dolphinscheduler.api.dto.resources.RenameFileDto;
import org.apache.dolphinscheduler.api.dto.resources.RenameFileRequest;
import org.apache.dolphinscheduler.api.dto.resources.ResourceComponent;
import org.apache.dolphinscheduler.api.dto.resources.UpdateFileDto;
import org.apache.dolphinscheduler.api.dto.resources.UpdateFileFromContentDto;
import org.apache.dolphinscheduler.api.dto.resources.UpdateFileFromContentRequest;
import org.apache.dolphinscheduler.api.dto.resources.UpdateFileRequest;
import org.apache.dolphinscheduler.api.dto.resources.visitor.ResourceTreeVisitor;
import org.apache.dolphinscheduler.api.dto.resources.visitor.Visitor;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.metrics.ApiServerMetrics;
import org.apache.dolphinscheduler.api.service.ResourcesService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.validator.resource.CreateDirectoryDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.CreateDirectoryRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.CreateFileDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.CreateFileFromContentDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.DeleteResourceDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.DownloadFileDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.FetchFileContentDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.FileFromContentRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.FileRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.PagingResourceItemRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.RenameDirectoryDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.RenameDirectoryRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.RenameFileDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.RenameFileRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.UpdateFileDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.UpdateFileFromContentDtoValidator;
import org.apache.dolphinscheduler.api.validator.resource.UpdateFileFromContentRequestTransformer;
import org.apache.dolphinscheduler.api.validator.resource.UpdateFileRequestTransformer;
import org.apache.dolphinscheduler.api.vo.ResourceItemVO;
import org.apache.dolphinscheduler.api.vo.resources.FetchFileContentResponse;
import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.dao.repository.TenantDao;
import org.apache.dolphinscheduler.plugin.storage.api.StorageEntity;
import org.apache.dolphinscheduler.plugin.storage.api.StorageOperator;
import org.apache.dolphinscheduler.spi.enums.ResourceType;

import org.apache.commons.collections4.CollectionUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/**
 * 资源服务实现类
 *
 * <p>该类实现了资源文件的管理功能。资源文件包括脚本文件、配置文件、
 * JAR包等，用于支持任务执行时的文件依赖管理。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建和管理文件和目录</li>
 *   <li>上传和下载资源文件</li>
 *   <li>文件内容的在线编辑</li>
 *   <li>资源文件的重命名和删除</li>
 *   <li>分页查询资源列表</li>
 *   <li>资源权限管理</li>
 * </ul>
 */
@Service
@Slf4j
public class ResourcesServiceImpl extends BaseServiceImpl implements ResourcesService {

    @Autowired
    private TenantDao tenantDao;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private StorageOperator storageOperator;

    @Autowired
    private CreateDirectoryRequestTransformer createDirectoryRequestTransformer;

    @Autowired
    private CreateDirectoryDtoValidator createDirectoryDtoValidator;

    @Autowired
    private RenameDirectoryRequestTransformer renameDirectoryRequestTransformer;

    @Autowired
    private RenameDirectoryDtoValidator renameDirectoryDtoValidator;

    @Autowired
    private RenameFileRequestTransformer renameFileRequestTransformer;

    @Autowired
    private RenameFileDtoValidator renameFileDtoValidator;

    @Autowired
    private FileFromContentRequestTransformer createFileFromContentRequestTransformer;

    @Autowired
    private CreateFileFromContentDtoValidator createFileFromContentDtoValidator;

    @Autowired
    private FetchFileContentDtoValidator fetchFileContentDtoValidator;

    @Autowired
    private UpdateFileFromContentRequestTransformer updateFileFromContentRequestTransformer;

    @Autowired
    private UpdateFileFromContentDtoValidator updateFileFromContentDtoValidator;

    @Autowired
    private FileRequestTransformer createFileRequestTransformer;

    @Autowired
    private CreateFileDtoValidator createFileDtoValidator;

    @Autowired
    private UpdateFileRequestTransformer updateFileRequestTransformer;

    @Autowired
    private UpdateFileDtoValidator updateFileDtoValidator;

    @Autowired
    private DeleteResourceDtoValidator deleteResourceDtoValidator;

    @Autowired
    private DownloadFileDtoValidator downloadFileDtoValidator;

    @Autowired
    private PagingResourceItemRequestTransformer pagingResourceItemRequestTransformer;

    /**
     * 创建目录
     *
     * @param createDirectoryRequest 创建目录请求对象
     */
    @Override
    public void createDirectory(CreateDirectoryRequest createDirectoryRequest) {
        // 将请求对象转换为DTO对象
        CreateDirectoryDto createDirectoryDto = createDirectoryRequestTransformer.transform(createDirectoryRequest);
        // 验证DTO对象的合法性，包括权限、路径等
        createDirectoryDtoValidator.validate(createDirectoryDto);

        // 通过存储操作器创建目录
        storageOperator.createStorageDir(createDirectoryDto.getDirectoryAbsolutePath());
        log.info("Success create directory: {}", createDirectoryRequest.getParentAbsoluteDirectory());
    }

    /**
     * 创建文件（通过文件上传）
     *
     * @param createFileRequest 创建文件请求对象
     */
    @Override
    public void createFile(CreateFileRequest createFileRequest) {
        // 将请求对象转换为DTO对象
        CreateFileDto createFileDto = createFileRequestTransformer.transform(createFileRequest);
        // 验证文件创建的合法性，包括权限、文件大小、文件类型等
        createFileDtoValidator.validate(createFileDto);

        // todo: use storage proxy
        // 获取上传的文件对象
        MultipartFile file = createFileDto.getFile();
        // 获取文件的绝对路径
        String fileAbsolutePath = createFileDto.getFileAbsolutePath();
        // 先将文件复制到本地临时目录
        String srcLocalTmpFileAbsolutePath = copyFileToLocal(file);
        try {
            // 上传文件到存储系统（如HDFS、S3等）
            storageOperator.upload(srcLocalTmpFileAbsolutePath, fileAbsolutePath, true, false);
            // 记录文件上传大小的监控指标
            ApiServerMetrics.recordApiResourceUploadSize(file.getSize());
            log.info("Success upload resource file: {} complete.", fileAbsolutePath);
        } catch (Exception ex) {
            // 如果上传失败，清理临时文件
            FileUtils.deleteFile(srcLocalTmpFileAbsolutePath);
            throw ex;
        }
    }

    /**
     * 通过内容创建文件（在线编辑）
     *
     * @param createFileFromContentRequest 创建文件请求对象
     */
    @Override
    public void createFileFromContent(CreateFileFromContentRequest createFileFromContentRequest) {
        // 将请求转换为DTO对象
        CreateFileFromContentDto createFileFromContentDto =
                createFileFromContentRequestTransformer.transform(createFileFromContentRequest);
        // 验证文件内容和路径的合法性
        createFileFromContentDtoValidator.validate(createFileFromContentDto);

        // todo: use storage proxy
        // 获取文件内容
        String fileContent = createFileFromContentDto.getFileContent();
        // 获取文件的绝对路径
        String fileAbsolutePath = createFileFromContentDto.getFileAbsolutePath();
        String srcLocalTmpFileAbsolutePath = copyFileToLocal(fileContent);
        try {
            storageOperator.upload(srcLocalTmpFileAbsolutePath, fileAbsolutePath, true, false);
            ApiServerMetrics.recordApiResourceUploadSize(fileContent.length());
            log.info("Success upload resource file: {} complete.", fileAbsolutePath);
        } catch (Exception ex) {
            // If exception, clear the tmp path
            FileUtils.deleteFile(srcLocalTmpFileAbsolutePath);
            throw ex;
        }
    }

    @Override
    public void renameDirectory(RenameDirectoryRequest renameDirectoryRequest) {
        RenameDirectoryDto renameDirectoryDto = renameDirectoryRequestTransformer.transform(renameDirectoryRequest);
        renameDirectoryDtoValidator.validate(renameDirectoryDto);

        String originDirectoryAbsolutePath = renameDirectoryDto.getOriginDirectoryAbsolutePath();
        String targetDirectoryAbsolutePath = renameDirectoryDto.getTargetDirectoryAbsolutePath();
        storageOperator.copy(originDirectoryAbsolutePath, targetDirectoryAbsolutePath, true, true);
        log.info("Success rename directory: {} -> {} ", originDirectoryAbsolutePath, targetDirectoryAbsolutePath);
    }

    @Override
    public void renameFile(RenameFileRequest renameFileRequest) {
        RenameFileDto renameFileDto = renameFileRequestTransformer.transform(renameFileRequest);
        renameFileDtoValidator.validate(renameFileDto);

        String originFileAbsolutePath = renameFileDto.getOriginFileAbsolutePath();
        String targetFileAbsolutePath = renameFileDto.getTargetFileAbsolutePath();
        storageOperator.copy(originFileAbsolutePath, targetFileAbsolutePath, true, true);
        log.info("Success rename file: {} -> {} ", originFileAbsolutePath, targetFileAbsolutePath);
    }

    @Override
    public void updateFile(UpdateFileRequest updateFileRequest) {
        UpdateFileDto updateFileDto = updateFileRequestTransformer.transform(updateFileRequest);
        updateFileDtoValidator.validate(updateFileDto);

        String srcLocalTmpFileAbsolutePath = copyFileToLocal(updateFileDto.getFile());
        try {
            storageOperator.upload(srcLocalTmpFileAbsolutePath, updateFileDto.getFileAbsolutePath(), true, true);
            ApiServerMetrics.recordApiResourceUploadSize(updateFileDto.getFile().getSize());
            log.info("Success upload resource file: {} complete.", updateFileDto.getFileAbsolutePath());
        } catch (Exception ex) {
            // If exception, clear the tmp path
            FileUtils.deleteFile(srcLocalTmpFileAbsolutePath);
            throw ex;
        }
    }

    @Override
    public PageInfo<ResourceItemVO> pagingResourceItem(PagingResourceItemRequest pagingResourceItemRequest) {

        QueryResourceDto queryResourceDto = pagingResourceItemRequestTransformer.transform(pagingResourceItemRequest);
        List<String> resourceAbsolutePaths = queryResourceDto.getResourceAbsolutePaths();
        if (CollectionUtils.isEmpty(resourceAbsolutePaths)) {
            return new PageInfo<>(pagingResourceItemRequest.getPageNo(), pagingResourceItemRequest.getPageSize());
        }

        for (String resourceAbsolutePath : resourceAbsolutePaths) {
            createDirectoryDtoValidator.exceptionResourceAbsolutePathInvalidated(resourceAbsolutePath);
            createDirectoryDtoValidator.exceptionUserNoResourcePermission(pagingResourceItemRequest.getLoginUser(),
                    resourceAbsolutePath);
        }

        Integer pageNo = pagingResourceItemRequest.getPageNo();
        Integer pageSize = pagingResourceItemRequest.getPageSize();

        List<StorageEntity> storageEntities = resourceAbsolutePaths.stream()
                .flatMap(resourceAbsolutePath -> storageOperator.listStorageEntity(resourceAbsolutePath).stream())
                .collect(Collectors.toList());

        List<ResourceItemVO> result = storageEntities
                .stream()
                .filter(storageEntity -> storageEntity.getFileName()
                        .contains(pagingResourceItemRequest.getResourceNameKeyWord()))
                .skip((long) (pageNo - 1) * pageSize)
                .limit(pageSize)
                .map(ResourceItemVO::new)
                .collect(Collectors.toList());

        return PageInfo.<ResourceItemVO>builder()
                .pageNo(pagingResourceItemRequest.getPageNo())
                .pageSize(pagingResourceItemRequest.getPageSize())
                .total(storageEntities.size())
                .totalList(result)
                .build();
    }

    @Override
    public List<ResourceComponent> queryResourceFiles(User loginUser, ResourceType resourceType) {
        Tenant tenant = tenantDao.queryOptionalById(loginUser.getTenantId())
                .orElseThrow(() -> new ServiceException(Status.TENANT_NOT_EXIST, loginUser.getTenantId()));
        String storageBaseDirectory = storageOperator.getStorageBaseDirectory(tenant.getTenantCode(), resourceType);
        List<StorageEntity> allResourceFiles = storageOperator.listFileStorageEntityRecursively(storageBaseDirectory);

        Visitor visitor = new ResourceTreeVisitor(allResourceFiles);
        return visitor.visit("").getChildren();
    }

    @Override
    public void delete(DeleteResourceRequest deleteResourceRequest) {
        DeleteResourceDto deleteResourceDto = DeleteResourceDto.builder()
                .loginUser(deleteResourceRequest.getLoginUser())
                .resourceAbsolutePath(deleteResourceRequest.getResourceAbsolutePath())
                .build();
        deleteResourceDtoValidator.validate(deleteResourceDto);
        storageOperator.delete(deleteResourceDto.getResourceAbsolutePath(), true);
    }

    @Override
    public FetchFileContentResponse fetchResourceFileContent(FetchFileContentRequest fetchFileContentRequest) {
        FetchFileContentDto fetchFileContentDto = FetchFileContentDto.builder()
                .loginUser(fetchFileContentRequest.getLoginUser())
                .resourceFileAbsolutePath(fetchFileContentRequest.getResourceFileAbsolutePath())
                .skipLineNum(fetchFileContentRequest.getSkipLineNum())
                .limit(fetchFileContentRequest.getLimit())
                .build();
        fetchFileContentDtoValidator.validate(fetchFileContentDto);

        String content = storageOperator
                .fetchFileContent(
                        fetchFileContentRequest.getResourceFileAbsolutePath(),
                        fetchFileContentRequest.getSkipLineNum(),
                        fetchFileContentRequest.getLimit())
                .stream()
                .collect(Collectors.joining("\n"));

        ApiServerMetrics.recordApiResourceDownloadSize(content.length());

        return FetchFileContentResponse.builder()
                .content(content)
                .build();
    }

    @Override
    public void updateFileFromContent(UpdateFileFromContentRequest updateFileContentRequest) {
        UpdateFileFromContentDto updateFileFromContentDto =
                updateFileFromContentRequestTransformer.transform(updateFileContentRequest);
        updateFileFromContentDtoValidator.validate(updateFileFromContentDto);

        String srcLocalTmpFileAbsolutePath = copyFileToLocal(updateFileFromContentDto.getFileContent());
        try {
            storageOperator.upload(srcLocalTmpFileAbsolutePath, updateFileFromContentDto.getFileAbsolutePath(), true,
                    true);
            ApiServerMetrics.recordApiResourceUploadSize(updateFileFromContentDto.getFileContent().length());
            log.info("Success upload resource file: {} complete.", updateFileFromContentDto.getFileAbsolutePath());
        } catch (Exception ex) {
            // If exception, clear the tmp path
            FileUtils.deleteFile(srcLocalTmpFileAbsolutePath);
            throw new ServiceException("Update the resource file from content: "
                    + updateFileFromContentDto.getFileAbsolutePath() + " failed", ex);
        }
    }

    @Override
    public void downloadResource(HttpServletResponse response, DownloadFileRequest downloadFileRequest) {
        DownloadFileDto downloadFileDto = DownloadFileDto.builder()
                .loginUser(downloadFileRequest.getLoginUser())
                .fileAbsolutePath(downloadFileRequest.getFileAbsolutePath())
                .build();
        downloadFileDtoValidator.validate(downloadFileDto);

        String fileName = new File(downloadFileDto.getFileAbsolutePath()).getName();
        String localTmpFileAbsolutePath = FileUtils.getDownloadFilename(fileName);

        try {
            storageOperator.download(downloadFileRequest.getFileAbsolutePath(), localTmpFileAbsolutePath, true);
            int length = (int) new File(localTmpFileAbsolutePath).length();
            ApiServerMetrics.recordApiResourceDownloadSize(length);

            response.reset();
            response.setContentType("application/octet-stream");
            response.setCharacterEncoding("utf-8");
            response.setContentLength(length);
            response.setHeader("Content-Disposition", "attachment;filename=" + fileName);
            Files.copy(Paths.get(localTmpFileAbsolutePath), response.getOutputStream());
        } catch (Exception e) {
            throw new ServiceException(
                    "Download the resource file: " + downloadFileRequest.getFileAbsolutePath() + " failed", e);
        } finally {
            FileUtils.deleteFile(localTmpFileAbsolutePath);
        }
    }

    @Override
    public StorageEntity queryFileStatus(String userName, String fileAbsolutePath) {
        return storageOperator.getStorageEntity(fileAbsolutePath);
    }

    @Override
    public String queryResourceBaseDir(User loginUser, ResourceType type) {

        User user = userMapper.selectById(loginUser.getId());
        if (user == null) {
            throw new ServiceException(Status.USER_NOT_EXIST);
        }

        Tenant tenant = tenantDao.queryOptionalById(user.getTenantId())
                .orElseThrow(() -> new ServiceException(Status.CURRENT_LOGIN_USER_TENANT_NOT_EXIST));
        return storageOperator.getStorageBaseDirectory(tenant.getTenantCode(), type);
    }

    // Copy the file to the local file system and return the local file absolute path
    @SneakyThrows
    private String copyFileToLocal(MultipartFile multipartFile) {
        String localTmpFileAbsolutePath = FileUtils.getUploadFileLocalTmpAbsolutePath();
        FileUtils.copyInputStreamToFile(multipartFile.getInputStream(), localTmpFileAbsolutePath);
        return localTmpFileAbsolutePath;
    }

    // Copy the file to the local file system and return the local file absolute path
    private String copyFileToLocal(String fileContent) {
        String localTmpFileAbsolutePath = FileUtils.getUploadFileLocalTmpAbsolutePath();
        FileUtils.writeContent2File(fileContent, localTmpFileAbsolutePath);
        return localTmpFileAbsolutePath;
    }

}
