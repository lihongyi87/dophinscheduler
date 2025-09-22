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

package org.apache.dolphinscheduler.server.master.engine;

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.TaskGroupQueueStatus;
import org.apache.dolphinscheduler.dao.entity.TaskGroupQueue;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

/**
 * 任务组协调器接口
 * 用于管理任务组槽位，任务组槽位用于限制同时运行的任务实例数量
 *
 * TaskGroupQueue用于表示任务组槽位，当TaskGroupQueue的inQueue为YES时，
 * 表示该TaskGroupQueue正在被某个TaskInstance使用
 *
 * 当TaskInstance需要使用任务组时，应该使用acquireTaskGroupSlot方法获取任务组槽位，
 * 该方法不会阻塞并且总是成功获取，然后应该直接停止派发任务实例。
 * 当任务组槽位可用时，ITaskGroupCoordinator会唤醒等待的TaskInstance进行派发。
 *
 * 使用示例：
 * <pre>
 *     if(needAcquireTaskGroupSlot(taskInstance)) {
 *         taskGroupCoordinator.acquireTaskGroupSlot(taskInstance);
 *         return;
 *     }
 * </pre>
 *
 * 当TaskInstance完成时，应该使用releaseTaskGroupSlot方法释放任务组槽位。
 * <pre>
 *     if(needToReleaseTaskGroupSlot(taskInstance)) {
 *         taskGroupCoordinator.releaseTaskGroupSlot(taskInstance);
 *     }
 * </pre>
 */
public interface ITaskGroupCoordinator extends AutoCloseable {

    /**
     * 启动任务组协调器
     * 一旦启动，在关闭协调器之前不能再次调用此方法
     */
    void start();

    /**
     * 判断任务实例是否需要获取任务组槽位
     * 如果TaskInstance的getTaskGroupId() > 0，且TaskGroup标志为Flag.YES，则任务实例需要使用任务组
     *
     * @param taskInstance 任务实例
     * @return 如果TaskInstance需要获取任务组槽位则返回true
     */
    boolean needAcquireTaskGroupSlot(final TaskInstance taskInstance);

    /**
     * 为指定的TaskInstance获取任务组槽位
     *
     * 当taskInstance想要获取TaskGroup槽位时，应该调用此方法。
     * 如果获取成功，将在数据库中创建一个TaskGroupQueue，状态为WAIT_QUEUE并在队列中。
     * TaskInstance在有可用槽位并且taskGroupCoordinator通知之前不应该被派发。
     *
     * @param taskInstance 想要获取任务组槽位的任务实例
     * @throws IllegalArgumentException 如果taskInstance为null或使用的taskGroup不存在
     */
    void acquireTaskGroupSlot(TaskInstance taskInstance);

    /**
     * 判断任务实例是否需要释放任务组槽位
     * 如果TaskInstance正在使用TaskGroup，则需要释放TaskGroupSlot
     *
     * @param taskInstance 任务实例
     * @return 如果TaskInstance需要释放TaskGroupSlot则返回true
     */
    boolean needToReleaseTaskGroupSlot(TaskInstance taskInstance);

    /**
     * 为指定的TaskInstance释放任务组槽位
     *
     * 当taskInstance想要释放TaskGroup槽位时，应该调用此方法。释放方法会删除taskGroupQueue。
     * 此方法是幂等的，这意味着如果任务组槽位已经释放，此方法不会执行任何操作。
     *
     * @param taskInstance 想要释放任务组槽位的任务实例
     * @throws IllegalArgumentException 如果taskInstance为null或任务没有使用任务组
     */
    void releaseTaskGroupSlot(TaskInstance taskInstance);

    /**
     * 关闭任务组协调器
     * 一旦关闭，协调器将不工作，直到再次启动协调器
     */
    @Override
    void close();

}
