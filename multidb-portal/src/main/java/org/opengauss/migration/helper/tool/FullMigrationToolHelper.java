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
import org.opengauss.constants.tool.FullMigrationToolConstants;
import org.opengauss.domain.model.ConfigFile;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.progress.model.tool.FullMigrationToolStatusEntry;
import org.opengauss.migration.tools.FullMigrationTool;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * full migration tool helper
 *
 * @since 2025/5/29
 */
@Slf4j
public class FullMigrationToolHelper {
    private static final Logger LOGGER = LogManager.getLogger(FullMigrationToolHelper.class);

    private FullMigrationToolHelper() {
    }

    /**
     * generate full migration tool process start command
     *
     * @param fullConfig full config file
     * @param sourceDbType source db type
     * @param fullMigrationToolOrder full migration tool order
     * @param jvmPrefixOptions jvm prefix options
     * @return process start command
     */
    public static String generateProcessStartCommand(
            ConfigFile fullConfig, String sourceDbType, String fullMigrationToolOrder, String jvmPrefixOptions) {
        StringBuilder commandBuilder = new StringBuilder();

        String jarPath = FullMigrationTool.getInstance().getJarPath();
        commandBuilder.append("java").append(" ")
                .append(jvmPrefixOptions).append(" ")
                .append("-jar").append(" ").append(jarPath).append(" ")
                .append("--start").append(" ").append(fullMigrationToolOrder).append(" ")
                .append("--source").append(" ").append(sourceDbType).append(" ")
                .append("--config").append(" ").append(fullConfig.getFilePath());

        return commandBuilder.toString();
    }

    /**
     * generate full migration tool process check command
     *
     * @param fullConfig full config file
     * @param sourceDbType source db type
     * @param fullMigrationToolOrder full migration tool order
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
     * get full migration tool process stop sign
     *
     * @param fullMigrationToolOrder full migration tool order
     * @return process stop sign
     */
    public static String getProcessStopSign(String fullMigrationToolOrder) {
        if (FullMigrationToolConstants.ORDER_DROP_REPLICA_SCHEMA.equals(fullMigrationToolOrder)) {
            return "drop replica schema(sch_debezium) success.";
        }
        return fullMigrationToolOrder + " migration complete. full report thread is close.";
    }

    /**
     * generate full migration tool order status file path
     *
     * @param taskWorkspace task workspace
     * @param fullMigrationToolOrder full migration tool order
     * @return order status file path
     */
    public static String generateOrderStatusFilePath(TaskWorkspace taskWorkspace, String fullMigrationToolOrder) {
        return String.format("%s/%s.json", taskWorkspace.getStatusFullDirPath(), fullMigrationToolOrder);
    }

    /**
     * parse full migration tool status file to full migration tool status entry
     *
     * @param statusFilePath status file path
     * @return full migration tool status entry
     */
    public static Optional<FullMigrationToolStatusEntry> parseToolStatusFile(String statusFilePath) {
        Path statusPath = Path.of(statusFilePath);
        try {
            if (!Files.exists(statusPath)) {
                return Optional.empty();
            }

            String text = Files.readString(statusPath);
            if (!StringUtils.isNullOrBlank(text)) {
                return Optional.ofNullable(JSON.parseObject(text, FullMigrationToolStatusEntry.class,
                        JSONReader.Feature.IgnoreAutoTypeNotMatch));
            }
        } catch (IOException | JSONException e) {
            LOGGER.warn("Failed to read or parse full migration tool progress, error: {}", e.getMessage());
        }
        return Optional.empty();
    }
}
