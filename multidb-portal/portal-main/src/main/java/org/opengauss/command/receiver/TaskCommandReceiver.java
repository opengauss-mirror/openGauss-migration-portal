/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.printer.TablePrinter;
import org.opengauss.domain.vo.TaskListVo;
import org.opengauss.migration.workspace.TaskWorkspaceManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Task command receiver
 *
 * @since 2025/3/29
 */
public class TaskCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(TaskCommandReceiver.class);

    private final TaskWorkspaceManager workspaceManager;

    public TaskCommandReceiver() {
        workspaceManager = new TaskWorkspaceManager();
    }

    /**
     * List all migration task
     */
    public void list() {
        List<TaskListVo> taskListVoList = workspaceManager.list();
        printTaskTable(taskListVoList);
    }

    /**
     * Create migration task
     *
     * @param taskId task id
     * @param sourceDbType source database type
     */
    public void create(String taskId, String sourceDbType) {
        workspaceManager.create(taskId, sourceDbType);
    }

    /**
     * Delete migration task
     *
     * @param taskId task id
     */
    public void delete(String taskId) {
        workspaceManager.delete(taskId);
    }

    private void printTaskTable(List<TaskListVo> taskListVoList) {
        List<String> header = new ArrayList<>();
        header.add("Task ID");
        header.add("Source Database Type");
        header.add("Is Running");

        List<TaskListVo> taskList = taskListVoList.stream()
                .sorted(Comparator.comparing(TaskListVo::getTaskId))
                .collect(Collectors.toList());

        List<List<String>> tableInfoList = new ArrayList<>();
        for (TaskListVo taskListVo : taskList) {
            List<String> row = new ArrayList<>();
            row.add(taskListVo.getTaskId());
            row.add(taskListVo.getSourceDbType());
            row.add(taskListVo.isRunning() ? "Y" : "N");
            tableInfoList.add(row);
        }

        String table = TablePrinter.printTable(header, tableInfoList);
        LOGGER.info("Task List:{}{}", System.lineSeparator(), table);
    }
}
