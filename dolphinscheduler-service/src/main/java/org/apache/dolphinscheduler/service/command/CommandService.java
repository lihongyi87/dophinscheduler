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

package org.apache.dolphinscheduler.service.command;

import org.apache.dolphinscheduler.dao.entity.Command;

/**
 * 命令服务接口
 *
 * 管理DolphinScheduler中的命令对象，命令是工作流执行的触发器。
 * 每个工作流实例的启动都是通过命令触发的。
 *
 * 命令系统特点：
 * - 异步执行：命令先入队列，由Master节点异步消费
 * - 错误处理：失败的命令会移动到错误命令表
 * - 重复检查：避免重复创建相同的命令
 *
 * 命令类型包括：
 * - START_PROCESS：启动工作流
 * - START_FAILURE_TASK_PROCESS：从失败任务开始
 * - RECOVER_TOLERANCE_FAULT_PROCESS：容错恢复
 * - RECOVER_SUSPENDED_PROCESS：恢复暂停的工作流
 * - START_CURRENT_TASK_PROCESS：从当前任务开始
 * - REPEAT_RUNNING：重复运行
 * - PAUSE：暂停
 * - STOP：停止
 * - RECOVER_WAITING_THREAD：恢复等待线程
 *
 * 类比：就像餐厅的订单系统，顾客下单（创建命令），
 *      订单进入队列，厨房（Master）依次处理。
 */
public interface CommandService {

    /**
     * 移动命令到错误命令表
     *
     * 保存错误命令并删除原始命令。
     * 如果给定的命令已经被移动到错误命令表，
     * 将抛出 {@link java.sql.SQLIntegrityConstraintViolationException}
     *
     * @param command 要移动的命令
     * @param message 错误信息
     */
    void moveToErrorCommand(Command command, String message);

    /**
     * 创建新命令
     *
     * 将新命令保存到数据库，等待Master节点消费
     *
     * @param command 要创建的命令
     * @return 创建结果，成功返回1
     */
    int createCommand(Command command);

    /**
     * 验证是否需要创建命令
     *
     * 检查输入的命令是否已经存在于队列中，
     * 避免重复创建相同的命令
     *
     * @param command 要检查的命令
     * @return true表示需要创建，false表示命令已存在
     */
    boolean verifyIsNeedCreateCommand(Command command);

}
