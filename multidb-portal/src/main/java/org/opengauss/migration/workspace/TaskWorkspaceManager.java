/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.workspace;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.config.Portal;
import org.opengauss.constants.TaskConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.domain.vo.TaskListVo;
import org.opengauss.enums.DatabaseType;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.exceptions.MigrationException;
import org.opengauss.exceptions.TaskException;
import org.opengauss.migration.config.ElasticsearchMigrationJobConfig;
import org.opengauss.migration.config.MilvusMigrationJobConfig;
import org.opengauss.migration.config.MysqlMigrationJobConfig;
import org.opengauss.migration.config.PgsqlMigrationJobConfig;
import org.opengauss.migration.monitor.MigrationAliveMonitor;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Task workspace manager
 *
 * @since 2025/3/28
 */
public class TaskWorkspaceManager {
    private static final Logger LOGGER = LogManager.getLogger(TaskWorkspaceManager.class);

    private final String workspaceDir;

    public TaskWorkspaceManager() {
        workspaceDir = Portal.getInstance().getPortalWorkspaceDirPath();
        createWorkspaceDir();
    }

    /**
     * List all migration tasks
     *
     * @return List of migration tasks
     */
    public List<TaskListVo> list() {
        LOGGER.info("List all migration tasks");
        List<String> taskIds = listAllTaskIds();

        List<TaskListVo> taskListVoList = new ArrayList<>();
        for (String taskId : taskIds) {
            TaskListVo taskListVo = new TaskListVo();
            taskListVo.setTaskId(taskId);

            TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
            taskListVo.setSourceDbType(readSourceDbType(taskWorkspace));
            taskListVo.setRunning(isTaskRunning(taskWorkspace));
            taskListVoList.add(taskListVo);
        }
        return taskListVoList;
    }

    /**
     * Create a migration task
     *
     * @param taskId       Task id
     * @param sourceDbType Source database type
     */
    public void create(String taskId, String sourceDbType) {
        LOGGER.info("Start to create a migration task");
        if (StringUtils.isNullOrBlank(taskId) || StringUtils.isNullOrBlank(sourceDbType)) {
            throw new TaskException("Task id and source database type cannot be empty");
        }

        DatabaseType databaseType = checkAndParseSourceDbType(sourceDbType);
        checkTaskId(taskId);

        if (checkTaskIdExists(taskId)) {
            throw new TaskException("Task id already exists: " + taskId);
        }

        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        try {
            taskWorkspace.create();
            prepareConfigFiles(taskWorkspace, databaseType);
            LOGGER.info("Create a migration task successfully");
        } catch (ConfigException | IllegalArgumentException e) {
            LOGGER.error("Failed to create a migration task", e);
            taskWorkspace.delete();
        }
    }

    /**
     * Delete a migration task
     *
     * @param taskId Task id
     */
    public void delete(String taskId) {
        LOGGER.info("Start to delete a migration task");
        if (StringUtils.isNullOrBlank(taskId)) {
            LOGGER.error("Task id cannot be empty");
            return;
        }

        if (!checkTaskIdExists(taskId)) {
            LOGGER.error("Task does not exist: {}", taskId);
            return;
        }

        TaskWorkspace taskWorkspace = new TaskWorkspace(taskId);
        if (isTaskRunning(taskWorkspace)) {
            LOGGER.error("Migration task is running, please stop the task first");
            return;
        }

        String workspaceHomeDir = taskWorkspace.getHomeDir();
        try {
            FileUtils.deletePath(taskWorkspace.getHomeDir());
            LOGGER.info("Delete a migration task successfully");
        } catch (IOException e) {
            throw new TaskException("Failed to delete task workspace directory: " + workspaceHomeDir, e);
        }
    }

    /**
     * Check if the task id exists
     *
     * @param taskId Task id
     * @return true if the task id exists, false otherwise
     */
    public boolean checkTaskIdExists(String taskId) {
        List<String> taskIds = listAllTaskIds();
        return taskIds.contains(taskId);
    }

    /**
     * Check if the task is running
     *
     * @param taskWorkspace Task workspace
     * @return true if the task is running, false otherwise
     */
    public boolean isTaskRunning(TaskWorkspace taskWorkspace) {
        File heartbeatFile = new File(MigrationAliveMonitor.getHeartbeatFilePath(taskWorkspace));
        if (!heartbeatFile.exists()) {
            return false;
        }

        long lastModified = heartbeatFile.lastModified();
        long currentTime = System.currentTimeMillis();
        long timeoutMillis = Duration.ofMinutes(3).toMillis();
        return (currentTime - lastModified) <= timeoutMillis;
    }

    private String readSourceDbType(TaskWorkspace taskWorkspace) {
        String taskId = taskWorkspace.getId();
        try {
            String sourceDbTypeFilePath = taskWorkspace.getSourceDbTypeFilePath();
            if (FileUtils.checkFileExists(sourceDbTypeFilePath)) {
                DatabaseType type = DatabaseType.valueOf(FileUtils.readFileContents(sourceDbTypeFilePath).trim());
                return type.getStandardName();
            }
        } catch (IOException e) {
            throw new TaskException("Failed to read source database type", e);
        } catch (IllegalArgumentException e) {
            throw new TaskException("The source database type of task " + taskId + " is abnormal. Please delete the "
                    + TaskConstants.TASK_WORKSPACE_DIR_SUFFIX + taskId + " directory manually");
        }
        throw new TaskException("The directory structure of task " + taskId + " is abnormal. Please delete the "
                + TaskConstants.TASK_WORKSPACE_DIR_SUFFIX + taskId + " directory manually");
    }

    private DatabaseType checkAndParseSourceDbType(String sourceDbType) {
        try {
            DatabaseType databaseType = DatabaseType.valueOf(sourceDbType.toUpperCase(Locale.ROOT));
            if (TaskConstants.SUPPORTED_SOURCE_DB_TYPES.contains(databaseType)) {
                return databaseType;
            } else {
                throw new TaskException("Unsupported source database type: " + sourceDbType);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported source database type: " + sourceDbType);
        }
    }

    private void prepareConfigFiles(TaskWorkspace taskWorkspace, DatabaseType sourceDbType) {
        if (DatabaseType.MYSQL.equals(sourceDbType)) {
            new MysqlMigrationJobConfig(taskWorkspace).generateToolsConfigFiles();
        } else if (DatabaseType.POSTGRESQL.equals(sourceDbType)) {
            new PgsqlMigrationJobConfig(taskWorkspace).generateToolsConfigFiles();
        } else if (DatabaseType.MILVUS.equals(sourceDbType)) {
            new MilvusMigrationJobConfig(taskWorkspace).generateToolsConfigFiles();
        } else if (DatabaseType.ELASTICSEARCH.equals(sourceDbType)) {
            new ElasticsearchMigrationJobConfig(taskWorkspace).generateToolsConfigFiles();
        } else {
            throw new IllegalArgumentException("Unsupported source database type: " + sourceDbType);
        }
        generateSourceDbTypeConfigFile(taskWorkspace, sourceDbType);
        copyLog4j2ConfigFile(taskWorkspace);
    }

    private void copyLog4j2ConfigFile(TaskWorkspace taskWorkspace) {
        try {
            String targetFilePath = String.format("%s/log4j2.xml", taskWorkspace.getConfigDirPath());
            FileUtils.copyFile("config/log4j2.xml", targetFilePath);
        } catch (IOException e) {
            throw new MigrationException("Failed to copy log4j2 config file to task workspace", e);
        }
    }

    private void generateSourceDbTypeConfigFile(TaskWorkspace taskWorkspace, DatabaseType sourceDbType) {
        try {
            String configFilePath = taskWorkspace.getSourceDbTypeFilePath();
            FileUtils.writeToFile(configFilePath, sourceDbType.name(), false);
            FileUtils.setFileReadOnly(configFilePath);
        } catch (IOException e) {
            throw new ConfigException("Failed to write source database type config file", e);
        }
    }

    private void checkTaskId(String taskId) {
        if (taskId.length() > TaskConstants.MAX_TASK_ID_LENGTH) {
            throw new TaskException("The length of the task id cannot exceed "
                    + TaskConstants.MAX_TASK_ID_LENGTH + " characters");
        }

        if (!taskId.matches(TaskConstants.TASK_ID_PATTERN)) {
            throw new TaskException("Invalid task id: " + taskId + ". "
                    + "Only letters(a-z and A-Z), numbers(0-9), underscores(_), and hyphens(-) are allowed");
        }
    }

    private List<String> listAllTaskIds() {
        String suffix = TaskConstants.TASK_WORKSPACE_DIR_SUFFIX;

        Path directory = Paths.get(workspaceDir);
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().startsWith(suffix))
                    .map(path -> path.getFileName().toString().substring(suffix.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new TaskException("Failed to list all task ids", e);
        }
    }

    private void createWorkspaceDir() {
        try {
            if (!FileUtils.checkDirExists(workspaceDir)) {
                FileUtils.createDirectory(workspaceDir);
            }
        } catch (IOException e) {
            throw new TaskException("Failed to create portal workspace directory", e);
        }
    }
}
