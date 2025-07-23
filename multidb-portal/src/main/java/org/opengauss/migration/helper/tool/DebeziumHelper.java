/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.tool;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.DebeziumConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.progress.model.tool.DebeziumSinkStatusEntry;
import org.opengauss.migration.progress.model.tool.DebeziumSourceStatusEntry;
import org.opengauss.migration.tools.Kafka;
import org.opengauss.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * debezium helper
 *
 * @since 2025/5/17
 */
public class DebeziumHelper {
    private static final Logger LOGGER = LogManager.getLogger(DebeziumHelper.class);

    private DebeziumHelper() {
    }

    /**
     * generate debezium process start command
     *
     * @param connectorConfig connector config
     * @param workerConfig worker config
     * @param log4jConfig log4j config
     * @param commandPrefix command prefix
     * @return process start command
     */
    public static String generateProcessStartCommand(
            ConfigFile connectorConfig, ConfigFile workerConfig, ConfigFile log4jConfig, String commandPrefix) {
        StringBuilder commandBuilder = new StringBuilder();
        commandBuilder.append(commandPrefix).append(" && ");
        commandBuilder.append("export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:");
        commandBuilder.append(log4jConfig.getFilePath()).append("\" && ");
        commandBuilder.append(Kafka.getInstance().getConnectStandalonePath()).append(" -daemon ");
        commandBuilder.append(workerConfig.getFilePath()).append(" ").append(connectorConfig.getFilePath());
        return commandBuilder.toString();
    }

    /**
     * generate debezium process check command
     *
     * @param connectorConfig connector config
     * @param workerConfig worker config
     * @return process check command
     */
    public static String generateProcessCheckCommand(ConfigFile connectorConfig, ConfigFile workerConfig) {
        return String.format("ConnectStandalone %s %s", workerConfig.getFilePath(), connectorConfig.getFilePath());
    }

    /**
     * get incremental source status file path
     *
     * @param taskWorkspace task workspace
     * @return incremental source status file path
     */
    public static String getIncrementalSourceStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusIncrementalDirPath();
        return getDebeziumLatestStatusFilePath(statusDirPath, DebeziumConstants.INCREMENTAL_SOURCE_STATUS_FILE_PREFIX);
    }

    /**
     * get incremental sink status file path
     *
     * @param taskWorkspace task workspace
     * @return incremental sink status file path
     */
    public static String getIncrementalSinkStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusIncrementalDirPath();
        return getDebeziumLatestStatusFilePath(statusDirPath, DebeziumConstants.INCREMENTAL_SINK_STATUS_FILE_PREFIX);
    }

    /**
     * get reverse source status file path
     *
     * @param taskWorkspace task workspace
     * @return reverse source status file path
     */
    public static String getReverseSourceStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusReverseDirPath();
        return getDebeziumLatestStatusFilePath(statusDirPath, DebeziumConstants.REVERSE_SOURCE_STATUS_FILE_PREFIX);
    }

    /**
     * get reverse sink status file path
     *
     * @param taskWorkspace task workspace
     * @return reverse sink status file path
     */
    public static String getReverseSinkStatusFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusReverseDirPath();
        return getDebeziumLatestStatusFilePath(statusDirPath, DebeziumConstants.REVERSE_SINK_STATUS_FILE_PREFIX);
    }

    /**
     * get debezium incremental fail sql file path
     *
     * @param taskWorkspace task workspace
     * @return debezium incremental fail sql file path
     */
    public static String getDebeziumIncrementalFailSqlFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusIncrementalDirPath();
        return String.format("%s/%s", statusDirPath, DebeziumConstants.FAIL_SQL_FILE_NAME);
    }

    /**
     * get debezium reverse fail sql file path
     *
     * @param taskWorkspace task workspace
     * @return debezium reverse fail sql file path
     */
    public static String getDebeziumReverseFailSqlFilePath(TaskWorkspace taskWorkspace) {
        String statusDirPath = taskWorkspace.getStatusReverseDirPath();
        return String.format("%s/%s", statusDirPath, DebeziumConstants.FAIL_SQL_FILE_NAME);
    }

    /**
     * parse debezium sink status file to debezium sink status entry
     *
     * @param filePath status file path
     * @return debezium sink status entry
     */
    public static Optional<DebeziumSinkStatusEntry> parseDebeziumSinkStatusFile(String filePath) {
        Path statusPath = Path.of(filePath);
        if (!Files.exists(statusPath)) {
            return Optional.empty();
        }

        try {
            String text = Files.readString(statusPath);
            if (!StringUtils.isNullOrBlank(text)) {
                return Optional.ofNullable(JSON.parseObject(text, DebeziumSinkStatusEntry.class,
                        JSONReader.Feature.IgnoreAutoTypeNotMatch));
            }
        } catch (IOException | JSONException e) {
            LOGGER.warn("Failed to read or parse debezium sink progress, error: {}", e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * parse debezium source status file to debezium source status entry
     *
     * @param filePath status file path
     * @return debezium source status entry
     */
    public static Optional<DebeziumSourceStatusEntry> parseDebeziumSourceStatusFile(String filePath) {
        Path statusPath = Path.of(filePath);
        if (!Files.exists(statusPath)) {
            return Optional.empty();
        }

        try {
            String text = Files.readString(statusPath);
            if (!StringUtils.isNullOrBlank(text)) {
                return Optional.ofNullable(JSON.parseObject(
                        text, DebeziumSourceStatusEntry.class, JSONReader.Feature.IgnoreAutoTypeNotMatch));
            }
        } catch (IOException | JSONException e) {
            LOGGER.warn("Failed to read or parse debezium source progress, error: {}", e.getMessage());
        }
        return Optional.empty();
    }

    private static String getDebeziumLatestStatusFilePath(String fileParentDir, String statusFilePrefix) {
        String result = "";

        File directory = new File(fileParentDir);
        if (directory.exists() && directory.isDirectory()) {
            File[] dirListFiles = directory.listFiles();
            result = Optional.ofNullable(dirListFiles)
                    .map(files -> getLastedFileName(files, statusFilePrefix))
                    .orElse("");
        }
        return result;
    }

    private static String getLastedFileName(File[] dirListFiles, String target) {
        File targetFile = null;
        for (File dirListFile : dirListFiles) {
            if (!dirListFile.getName().contains(target)) {
                continue;
            }

            if (targetFile == null) {
                targetFile = dirListFile;
                continue;
            }

            if (dirListFile.lastModified() > targetFile.lastModified()) {
                targetFile = dirListFile;
            }
        }

        try {
            if (targetFile != null) {
                return targetFile.getCanonicalPath();
            }
        } catch (IOException e) {
            LOGGER.trace("Failed to get latest file path, error: {}", e.getMessage());
        }
        return "";
    }
}
