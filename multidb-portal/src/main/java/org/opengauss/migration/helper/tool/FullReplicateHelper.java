/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.tool;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.FullReplicateConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.progress.model.tool.FullReplicateStatusEntry;
import org.opengauss.migration.tools.FullReplicateTool;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * oG_datasync_full_migration helper
 *
 * @since 2025/5/29
 */
@Slf4j
public class FullReplicateHelper {
    private static final Logger LOGGER = LogManager.getLogger(FullReplicateHelper.class);

    private FullReplicateHelper() {
    }

    /**
     * generate oG_datasync_full_migration process start command
     *
     * @param fullConfig full config file
     * @param sourceDbType source db type
     * @param fullMigrationToolOrder oG_datasync_full_migration order
     * @param jvmPrefixOptions jvm prefix options
     * @return process start command
     */
    public static String generateProcessStartCommand(
            ConfigFile fullConfig, String sourceDbType, String fullMigrationToolOrder, String jvmPrefixOptions) {
        StringBuilder commandBuilder = new StringBuilder();

        String jarPath = FullReplicateTool.getInstance().getJarPath();
        commandBuilder.append("java").append(" ")
                .append(jvmPrefixOptions).append(" ")
                .append("-jar").append(" ").append(jarPath).append(" ")
                .append("--start").append(" ").append(fullMigrationToolOrder).append(" ")
                .append("--source").append(" ").append(sourceDbType).append(" ")
                .append("--config").append(" ").append(fullConfig.getFilePath());

        return commandBuilder.toString();
    }

    /**
     * generate oG_datasync_full_migration process check command
     *
     * @param fullConfig full config file
     * @param sourceDbType source db type
     * @param fullMigrationToolOrder oG_datasync_full_migration order
     * @param jvmPrefixOptions jvm prefix options
     * @return process check command
     */
    public static String generateProcessCheckCommand(
            ConfigFile fullConfig, String sourceDbType, String fullMigrationToolOrder, String jvmPrefixOptions) {
        return generateProcessStartCommand(fullConfig, sourceDbType, fullMigrationToolOrder, jvmPrefixOptions);
    }

    /**
     * generate full migration log path
     *
     * @param taskWorkspace task workspace
     * @return log path
     */
    public static String generateFullMigrationLogPath(TaskWorkspace taskWorkspace) {
        return String.format("%s/%s", taskWorkspace.getLogsFullDirPath(), "full_migration.log");
    }

    /**
     * get oG_datasync_full_migration process stop sign
     *
     * @param fullMigrationToolOrder oG_datasync_full_migration order
     * @return process stop sign
     */
    public static String getProcessStopSign(String fullMigrationToolOrder) {
        if (FullReplicateConstants.ORDER_DROP_REPLICA_SCHEMA.equals(fullMigrationToolOrder)) {
            return "drop replica schema(sch_debezium) success.";
        }
        return fullMigrationToolOrder + " migration complete. full report thread is close.";
    }

    /**
     * generate oG_datasync_full_migration order status file path
     *
     * @param taskWorkspace task workspace
     * @param fullMigrationToolOrder oG_datasync_full_migration order
     * @return order status file path
     */
    public static String generateOrderStatusFilePath(TaskWorkspace taskWorkspace, String fullMigrationToolOrder) {
        return String.format("%s/%s.json", taskWorkspace.getStatusFullDirPath(), fullMigrationToolOrder);
    }

    /**
     * parse oG_datasync_full_migration status file to oG_datasync_full_migration status entry
     *
     * @param statusFilePath status file path
     * @return oG_datasync_full_migration status entry
     */
    public static Optional<FullReplicateStatusEntry> parseToolStatusFile(String statusFilePath) {
        Path statusPath = Path.of(statusFilePath);
        try {
            if (!Files.exists(statusPath)) {
                return Optional.empty();
            }

            String text = Files.readString(statusPath);
            if (!StringUtils.isNullOrBlank(text)) {
                return Optional.ofNullable(JSON.parseObject(text, FullReplicateStatusEntry.class,
                        JSONReader.Feature.IgnoreAutoTypeNotMatch));
            }
        } catch (IOException | JSONException e) {
            LOGGER.warn("Failed to read or parse oG_datasync_full_migration progress, error: {}", e.getMessage());
        }
        return Optional.empty();
    }
}
