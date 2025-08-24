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

package org.apache.dolphinscheduler.api.controller;

import static org.apache.dolphinscheduler.api.enums.Status.QUERY_WORKFLOW_INSTANCE_LIST_PAGING_ERROR;

import org.apache.dolphinscheduler.api.audit.OperatorLog;
import org.apache.dolphinscheduler.api.audit.enums.AuditType;
import org.apache.dolphinscheduler.api.dto.DynamicSubWorkflowDto;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.WorkflowInstanceService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * 工作流实例控制器
 * 
 * 这是DolphinScheduler API层中处理工作流实例管理的核心控制器。
 * 类比：项目管理系统中的"任务执行记录"管理页面，用于查看、管理和监控工作流的执行历史。
 * 
 * 主要功能：
 * 1. 工作流实例查询：支持分页查询、按条件筛选、按执行时长排序等
 * 2. 实例详情查看：查看工作流实例的详细执行信息和任务列表  
 * 3. 实例状态管理：更新、删除工作流实例及其相关数据
 * 4. 子工作流关系：处理父子工作流之间的查询和关联关系
 * 5. 变量管理：查看工作流实例中的全局变量和局部变量
 * 6. 甘特图展示：以时间轴方式展示工作流和任务的执行进度
 * 
 * API设计特点：
 * - RESTful风格：使用标准的HTTP方法(GET/POST/PUT/DELETE)
 * - 路径参数：通过/projects/{projectCode}/workflow-instances定义资源层次
 * - 参数验证：支持分页参数校验和参数转义处理
 * - 异常处理：使用@ApiException统一异常响应格式
 * - 审计日志：关键操作自动记录操作日志用于审计追踪
 * 
 * 类比理解：
 * 这个控制器就像是一个"工作执行记录管理员"，负责：
 * - 查找历史执行记录（分页查询）
 * - 查看具体执行详情（实例详情）
 * - 管理执行记录（更新删除）
 * - 追踪任务关系（父子工作流）
 * - 监控执行进度（甘特图）
 */
@Tag(name = "WORKFLOW_INSTANCE_TAG")
@RestController
@RequestMapping("/projects/{projectCode}/workflow-instances")
@Slf4j
public class WorkflowInstanceController extends BaseController {

    /**
     * 工作流实例服务
     * 
     * 注入的业务服务层，处理具体的工作流实例管理逻辑。
     * 控制器只负责HTTP请求处理，具体的业务逻辑委托给服务层执行。
     * 类比：管理员（Controller）接收用户请求，然后交给专业人员（Service）处理。
     */
    @Autowired
    private WorkflowInstanceService workflowInstanceService;

    /**
     * 分页查询工作流实例列表
     * 
     * 这是最常用的API接口，用于在前端页面展示工作流的执行历史记录。
     * 支持多种查询条件的组合筛选，帮助用户快速定位目标工作流实例。
     * 
     * 类比：在图书馆系统中查询借阅记录，可以按书名、借阅人、时间段等条件筛选。
     * 
     * 查询条件说明：
     * @param loginUser 登录用户 - 用于权限验证，确保用户只能查看有权限的项目数据
     * @param projectCode 项目编码 - 指定查询哪个项目下的工作流实例
     * @param pageNo 页码 - 分页参数，指定查询第几页（从1开始）
     * @param pageSize 每页大小 - 分页参数，指定每页显示多少条记录
     * @param workflowDefinitionCode 工作流定义编码 - 可选，筛选特定工作流定义的实例
     * @param searchVal 搜索值 - 可选，支持模糊搜索工作流实例名称
     * @param stateType 状态类型 - 可选，按执行状态筛选（成功、失败、运行中等）
     * @param host 主机地址 - 可选，筛选在特定主机上执行的实例
     * @param startTime 开始时间 - 可选，时间范围筛选的起始时间
     * @param endTime 结束时间 - 可选，时间范围筛选的结束时间
     * @param otherParamsJson 其他参数JSON - 可选，扩展参数，用于处理特殊筛选条件
     * @return 工作流实例分页列表，包含实例基本信息、执行状态、时间等
     * 
     * 实现特点：
     * 1. 参数校验：自动校验分页参数的合法性（页码>0，页面大小在合理范围）
     * 2. 参数转义：对搜索值进行转义处理，防止SQL注入攻击
     * 3. 权限控制：基于登录用户的权限，只返回用户有权查看的数据
     * 4. 性能优化：使用分页查询避免大量数据加载影响性能
     */
    @Operation(summary = "queryWorkflowInstanceListPaging", description = "QUERY_WORKFLOW_INSTANCE_LIST_NOTES")
    @Parameters({
            @Parameter(name = "workflowDefinitionCode", description = "WORKFLOW_DEFINITION_CODE", schema = @Schema(implementation = long.class, example = "100")),
            @Parameter(name = "searchVal", description = "SEARCH_VAL", schema = @Schema(implementation = String.class)),
            @Parameter(name = "executorName", description = "EXECUTOR_NAME", schema = @Schema(implementation = String.class)),
            @Parameter(name = "stateType", description = "EXECUTION_STATUS", schema = @Schema(implementation = WorkflowExecutionStatus.class)),
            @Parameter(name = "host", description = "HOST", schema = @Schema(implementation = String.class)),
            @Parameter(name = "startDate", description = "START_DATE", schema = @Schema(implementation = String.class)),
            @Parameter(name = "endDate", description = "END_DATE", schema = @Schema(implementation = String.class)),
            @Parameter(name = "pageNo", description = "PAGE_NO", required = true, schema = @Schema(implementation = int.class, example = "1")),
            @Parameter(name = "pageSize", description = "PAGE_SIZE", required = true, schema = @Schema(implementation = int.class, example = "10"))
    })
    @GetMapping()
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_WORKFLOW_INSTANCE_LIST_PAGING_ERROR)
    public Result queryWorkflowInstanceList(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                            @RequestParam(value = "workflowDefinitionCode", required = false, defaultValue = "0") long workflowDefinitionCode,
                                            @RequestParam(value = "searchVal", required = false) String searchVal,
                                            @RequestParam(value = "executorName", required = false) String executorName,
                                            @RequestParam(value = "stateType", required = false) WorkflowExecutionStatus stateType,
                                            @RequestParam(value = "host", required = false) String host,
                                            @RequestParam(value = "startDate", required = false) String startTime,
                                            @RequestParam(value = "endDate", required = false) String endTime,
                                            @RequestParam(value = "otherParamsJson", required = false) String otherParamsJson,
                                            @RequestParam("pageNo") Integer pageNo,
                                            @RequestParam("pageSize") Integer pageSize) {

        checkPageParams(pageNo, pageSize);
        searchVal = ParameterUtils.handleEscapes(searchVal);
        return workflowInstanceService.queryWorkflowInstanceList(loginUser, projectCode, workflowDefinitionCode,
                startTime,
                endTime,
                searchVal, executorName, stateType, host, otherParamsJson, pageNo, pageSize);
    }

    /**
     * query task list by workflow instance id
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param id workflow instance id
     * @return task list for the workflow instance
     */
    @Operation(summary = "queryTaskListByWorkflowInstanceId", description = "QUERY_TASK_LIST_BY_WORKFLOW_INSTANCE_ID_NOTES")
    @Parameters({
            @Parameter(name = "id", description = "WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @GetMapping(value = "/{id}/tasks")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_TASK_LIST_BY_WORKFLOW_INSTANCE_ID_ERROR)
    public Result queryTaskListByWorkflowInstanceId(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                    @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                                    @PathVariable("id") Integer id) throws IOException {
        Map<String, Object> result =
                workflowInstanceService.queryTaskListByWorkflowInstanceId(loginUser, projectCode, id);
        return returnDataList(result);
    }

    /**
     * update workflow instance
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param taskRelationJson workflow task relation json
     * @param taskDefinitionJson taskDefinitionJson
     * @param id workflow instance id
     * @param scheduleTime schedule time
     * @param syncDefine sync define
     * @param locations locations
     * @return update result code
     */
    @Operation(summary = "updateWorkflowInstance", description = "UPDATE_WORKFLOW_INSTANCE_NOTES")
    @Parameters({
            @Parameter(name = "taskRelationJson", description = "TASK_RELATION_JSON", schema = @Schema(implementation = String.class)),
            @Parameter(name = "taskDefinitionJson", description = "TASK_DEFINITION_JSON", schema = @Schema(implementation = String.class)),
            @Parameter(name = "id", description = "WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "1")),
            @Parameter(name = "scheduleTime", description = "SCHEDULE_TIME", schema = @Schema(implementation = String.class)),
            @Parameter(name = "syncDefine", description = "SYNC_DEFINE", required = true, schema = @Schema(implementation = boolean.class, example = "false")),
            @Parameter(name = "globalParams", description = "WORKFLOW_GLOBAL_PARAMS", schema = @Schema(implementation = String.class, example = "[]")),
            @Parameter(name = "locations", description = "WORKFLOW_INSTANCE_LOCATIONS", schema = @Schema(implementation = String.class)),
            @Parameter(name = "timeout", description = "WORKFLOW_TIMEOUT", schema = @Schema(implementation = int.class, example = "0")),
    })
    @PutMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.UPDATE_WORKFLOW_INSTANCE_ERROR)
    @OperatorLog(auditType = AuditType.WORKFLOW_INSTANCE_UPDATE)
    public Result updateWorkflowInstance(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                         @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                         @RequestParam(value = "taskRelationJson", required = true) String taskRelationJson,
                                         @RequestParam(value = "taskDefinitionJson", required = true) String taskDefinitionJson,
                                         @PathVariable(value = "id") Integer id,
                                         @RequestParam(value = "scheduleTime", required = false) String scheduleTime,
                                         @RequestParam(value = "syncDefine", required = true) Boolean syncDefine,
                                         @RequestParam(value = "globalParams", required = false, defaultValue = "[]") String globalParams,
                                         @RequestParam(value = "locations", required = false) String locations,
                                         @RequestParam(value = "timeout", required = false, defaultValue = "0") int timeout) {
        Map<String, Object> result = workflowInstanceService.updateWorkflowInstance(loginUser, projectCode, id,
                taskRelationJson, taskDefinitionJson, scheduleTime, syncDefine, globalParams, locations, timeout);
        return returnDataList(result);
    }

    /**
     * query workflow instance by id
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param id workflow instance id
     * @return workflow instance detail
     */
    @Operation(summary = "queryWorkflowInstanceById", description = "QUERY_WORKFLOW_INSTANCE_BY_ID_NOTES")
    @Parameters({
            @Parameter(name = "id", description = "WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @GetMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_WORKFLOW_INSTANCE_BY_ID_ERROR)
    public Result queryWorkflowInstanceById(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                            @PathVariable("id") Integer id) {
        Map<String, Object> result = workflowInstanceService.queryWorkflowInstanceById(loginUser, projectCode, id);
        return returnDataList(result);
    }

    /**
     * query top n workflow instance order by running duration
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param size number of workflow instance
     * @param startTime start time
     * @param endTime end time
     * @return list of workflow instance
     */
    @Operation(summary = "queryTopNLongestRunningWorkflowInstance", description = "QUERY_TOPN_LONGEST_RUNNING_WORKFLOW_INSTANCE_NOTES")
    @Parameters({
            @Parameter(name = "size", description = "WORKFLOW_INSTANCE_SIZE", required = true, schema = @Schema(implementation = int.class, example = "10")),
            @Parameter(name = "startTime", description = "WORKFLOW_INSTANCE_START_TIME", required = true, schema = @Schema(implementation = String.class)),
            @Parameter(name = "endTime", description = "WORKFLOW_INSTANCE_END_TIME", required = true, schema = @Schema(implementation = String.class)),
    })
    @GetMapping(value = "/top-n")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_WORKFLOW_INSTANCE_BY_ID_ERROR)
    public Result<WorkflowInstance> queryTopNLongestRunningWorkflowInstance(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                                            @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                                                            @RequestParam("size") Integer size,
                                                                            @RequestParam(value = "startTime", required = true) String startTime,
                                                                            @RequestParam(value = "endTime", required = true) String endTime) {
        Map<String, Object> result = workflowInstanceService.queryTopNLongestRunningWorkflowInstance(loginUser,
                projectCode, size, startTime, endTime);
        return returnDataList(result);
    }

    /**
     * delete workflow instance by id, at the same time,
     * delete task instance and their mapping relation data
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param id workflow instance id
     * @return delete result code
     */
    @Operation(summary = "deleteWorkflowInstanceById", description = "DELETE_WORKFLOW_INSTANCE_BY_ID_NOTES")
    @Parameters({
            @Parameter(name = "id", description = "WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.DELETE_WORKFLOW_INSTANCE_BY_ID_ERROR)
    @OperatorLog(auditType = AuditType.WORKFLOW_INSTANCE_DELETE)
    public Result<Void> deleteWorkflowInstanceById(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                                   @PathVariable("id") Integer id) {
        workflowInstanceService.deleteWorkflowInstanceById(loginUser, id);
        return Result.success();
    }

    /**
     * query sub workflow instance detail info by task id
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param taskId task id
     * @return sub workflow instance detail
     */
    @Operation(summary = "querySubWorkflowInstanceByTaskCode", description = "QUERY_SUB_WORKFLOW_INSTANCE_BY_TASK_CODE_NOTES")
    @Parameters({
            @Parameter(name = "taskCode", description = "TASK_CODE", required = true, schema = @Schema(implementation = long.class, example = "100"))
    })
    @GetMapping(value = "/query-sub-by-parent")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_SUB_WORKFLOW_INSTANCE_DETAIL_INFO_BY_TASK_ID_ERROR)
    public Result querySubWorkflowInstanceByTaskId(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                                   @RequestParam("taskId") Integer taskId) {
        Map<String, Object> result =
                workflowInstanceService.querySubWorkflowInstanceByTaskId(loginUser, projectCode, taskId);
        return returnDataList(result);
    }

    /**
     * query parent workflow instance detail info by sub workflow instance id
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param subId sub workflow id
     * @return parent instance detail
     */
    @Operation(summary = "queryParentInstanceBySubId", description = "QUERY_PARENT_WORKFLOW_INSTANCE_BY_SUB_WORKFLOW_INSTANCE_ID_NOTES")
    @Parameters({
            @Parameter(name = "subId", description = "SUB_WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @GetMapping(value = "/query-parent-by-sub")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_PARENT_WORKFLOW_INSTANCE_DETAIL_INFO_BY_SUB_WORKFLOW_INSTANCE_ID_ERROR)
    public Result queryParentInstanceBySubId(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                             @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                             @RequestParam("subId") Integer subId) {
        Map<String, Object> result = workflowInstanceService.queryParentInstanceBySubId(loginUser, projectCode, subId);
        return returnDataList(result);
    }

    /**
     * query dynamic sub workflow instance detail info by task id
     *
     * @param loginUser login user
     * @param taskId task id
     * @return sub workflow instance detail
     */
    @Operation(summary = "queryDynamicSubWorkflowInstances", description = "QUERY_DYNAMIC_SUB_WORKFLOW_INSTANCE_BY_TASK_CODE_NOTES")
    @Parameters({
            @Parameter(name = "taskId", description = "taskInstanceId", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @GetMapping(value = "/query-dynamic-sub-workflows")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_SUB_WORKFLOW_INSTANCE_DETAIL_INFO_BY_TASK_ID_ERROR)
    public Result<List<DynamicSubWorkflowDto>> queryDynamicSubWorkflowInstances(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                                                @RequestParam("taskId") Integer taskId) {
        List<DynamicSubWorkflowDto> dynamicSubWorkflowDtos =
                workflowInstanceService.queryDynamicSubWorkflowInstances(loginUser, taskId);
        return new Result(Status.SUCCESS.getCode(), Status.SUCCESS.getMsg(), dynamicSubWorkflowDtos);
    }

    /**
     * query workflow instance global variables and local variables
     *
     * @param loginUser login user
     * @param id workflow instance id
     * @return variables data
     */
    @Operation(summary = "viewVariables", description = "QUERY_WORKFLOW_INSTANCE_GLOBAL_VARIABLES_AND_LOCAL_VARIABLES_NOTES")
    @Parameters({
            @Parameter(name = "id", description = "WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @GetMapping(value = "/{id}/view-variables")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.QUERY_WORKFLOW_INSTANCE_ALL_VARIABLES_ERROR)
    public Result viewVariables(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                                @PathVariable("id") Integer id) {
        Map<String, Object> result = workflowInstanceService.viewVariables(projectCode, id);
        return returnDataList(result);
    }

    /**
     * encapsulation gantt structure
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param id workflow instance id
     * @return gantt tree data
     */
    @Operation(summary = "vieGanttTree", description = "VIEW_GANTT_NOTES")
    @Parameters({
            @Parameter(name = "id", description = "WORKFLOW_INSTANCE_ID", required = true, schema = @Schema(implementation = int.class, example = "100"))
    })
    @GetMapping(value = "/{id}/view-gantt")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.ENCAPSULATION_WORKFLOW_INSTANCE_GANTT_STRUCTURE_ERROR)
    public Result viewTree(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                           @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true) @PathVariable long projectCode,
                           @PathVariable("id") Integer id) throws Exception {
        Map<String, Object> result = workflowInstanceService.viewGantt(projectCode, id);
        return returnDataList(result);
    }

    /**
     * batch delete workflow instance by ids, at the same time,
     * delete task instance and their mapping relation data
     *
     * @param loginUser login user
     * @param projectCode project code
     * @param workflowInstanceIds workflow instance id
     * @return delete result code
     */
    @Operation(summary = "batchDeleteWorkflowInstanceByIds", description = "BATCH_DELETE_WORKFLOW_INSTANCE_BY_IDS_NOTES")
    @Parameters({
            @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true, schema = @Schema(implementation = int.class)),
            @Parameter(name = "workflowInstanceIds", description = "WORKFLOW_INSTANCE_IDS", required = true, schema = @Schema(implementation = String.class)),
    })
    @PostMapping(value = "/batch-delete")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(Status.BATCH_DELETE_WORKFLOW_INSTANCE_BY_IDS_ERROR)
    @OperatorLog(auditType = AuditType.WORKFLOW_INSTANCE_BATCH_DELETE)
    public Result batchDeleteWorkflowInstanceByIds(@RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @PathVariable long projectCode,
                                                   @RequestParam("workflowInstanceIds") String workflowInstanceIds) {
        // task queue
        Map<String, Object> result = new HashMap<>();
        List<String> deleteFailedIdList = new ArrayList<>();
        if (!StringUtils.isEmpty(workflowInstanceIds)) {
            String[] workflowInstanceIdArray = workflowInstanceIds.split(Constants.COMMA);

            for (String strWorkflowInstanceId : workflowInstanceIdArray) {
                int workflowInstanceId = Integer.parseInt(strWorkflowInstanceId);
                try {
                    workflowInstanceService.deleteWorkflowInstanceById(loginUser, workflowInstanceId);
                } catch (Exception e) {
                    log.error("Delete workflow instance: {} error", strWorkflowInstanceId, e);
                    deleteFailedIdList
                            .add(MessageFormat.format(Status.WORKFLOW_INSTANCE_ERROR.getMsg(), strWorkflowInstanceId));
                }
            }
        }
        if (!deleteFailedIdList.isEmpty()) {
            putMsg(result, Status.BATCH_DELETE_WORKFLOW_INSTANCE_BY_IDS_ERROR, String.join("\n", deleteFailedIdList));
        } else {
            putMsg(result, Status.SUCCESS);
        }
        return returnDataList(result);
    }

    // Todo: This is unstable, in some case the command trigger failed, we cannot get workflow instance
    // And it's a bad design to use trigger code to get workflow instance why not directly get by workflow instanceId or
    // inject the trigger id into workflow instance?
    @Deprecated
    @Operation(summary = "queryWorkflowInstanceListByTrigger", description = "QUERY_WORKFLOW_INSTANCE_BY_TRIGGER_NOTES")
    @Parameters({
            @Parameter(name = "projectCode", description = "PROJECT_CODE", required = true, schema = @Schema(implementation = Long.class)),
            @Parameter(name = "triggerCode", description = "TRIGGER_CODE", required = true, schema = @Schema(implementation = Long.class))
    })
    @GetMapping("/trigger")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_WORKFLOW_INSTANCE_LIST_PAGING_ERROR)
    public Result queryWorkflowInstancesByTriggerCode(@RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                      @PathVariable long projectCode,
                                                      @RequestParam(value = "triggerCode") Long triggerCode) {
        Map<String, Object> result = workflowInstanceService.queryByTriggerCode(loginUser, projectCode, triggerCode);
        return returnDataList(result);
    }
}
