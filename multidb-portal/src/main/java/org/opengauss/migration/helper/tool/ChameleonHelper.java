/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.tool;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.ChameleonConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.migration.progress.model.tool.ChameleonStatusEntry;
import org.opengauss.migration.tools.Chameleon;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * chameleon helper
 *
 * @since 2025/5/14
 */
public class ChameleonHelper {
    private static final Logger LOGGER = LogManager.getLogger(ChameleonHelper.class);

    private ChameleonHelper() {
    }

    /**
     * parse chameleon status file to chameleon status entry
     *
     * @param statusFilePath status file path
     * @return chameleon status entry
     */
    public static Optional<ChameleonStatusEntry> parseChameleonStatusFile(String statusFilePath) {
        Path statusPath = Path.of(statusFilePath);
        try {
            if (!Files.exists(statusPath)) {
                return Optional.empty();
            }

            String text = Files.readString(statusPath);
            if (!StringUtils.isNullOrBlank(text)) {
                return Optional.ofNullable(JSON.parseObject(text, ChameleonStatusEntry.class));
            }
        } catch (IOException | JSONException e) {
            LOGGER.warn("Failed to read or parse chameleon progress, error: {}", e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * get all chameleon status file path list
     *
     * @param taskWorkspace task workspace
     * @return all status file path list
     */
    public static List<String> getAllStatusFilePathList(TaskWorkspace taskWorkspace) {
        ArrayList<String> result = new ArrayList<>();
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_DROP_REPLICA_SCHEMA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_CREATE_REPLICA_SCHEMA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_ADD_SOURCE));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_INIT_REPLICA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_START_TRIGGER_REPLICA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_START_VIEW_REPLICA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_START_FUNC_REPLICA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_START_PROC_REPLICA));
        result.add(generateOrderStatusFilePath(taskWorkspace, ChameleonConstants.ORDER_DETACH_REPLICA));
        return result;
    }

    /**
     * generate chameleon order status file path
     *
     * @param taskWorkspace task workspace
     * @param chameleonOrder chameleon order
     * @return chameleon order status file path
     */
    public static String generateOrderStatusFilePath(TaskWorkspace taskWorkspace, String chameleonOrder) {
        return String.format("%s/data_default_%s_%s.json", Chameleon.getInstance().getChameleonHomeDirPath(),
                taskWorkspace.getId(), chameleonOrder);
    }

    /**
     * generate chameleon full migration config file name
     *
     * @param taskWorkspace task workspace
     * @return chameleon full migration config file name
     */
    public static String generateFullMigrationConfigFileName(TaskWorkspace taskWorkspace) {
        String fullConfigNameModel = "default_%s.yml";
        return String.format(fullConfigNameModel, taskWorkspace.getId());
    }

    /**
     * generate chameleon full migration log path
     *
     * @param taskWorkspace task workspace
     * @return chameleon full migration log path
     */
    public static String generateFullMigrationLogPath(TaskWorkspace taskWorkspace) {
        return String.format("%s/%s", taskWorkspace.getLogsFullDirPath(), "full_migration.log");
    }

    /**
     * generate chameleon process start command
     *
     * @param taskWorkspace task workspace
     * @param chameleonOrder chameleon order
     * @return chameleon process start command
     */
    public static String generateProcessStartCommand(TaskWorkspace taskWorkspace, String chameleonOrder) {
        HashMap<String, String> orderParams = generateOrderParams(taskWorkspace, chameleonOrder);

        String chameleonPath = Chameleon.getInstance().getChameleonPath();
        StringBuilder commandBuilder = new StringBuilder(chameleonPath);
        commandBuilder.append(" ").append(chameleonOrder).append(" ");

        for (String key : orderParams.keySet()) {
            commandBuilder.append(key).append(" ").append(orderParams.get(key)).append(" ");
        }
        return commandBuilder.substring(0, commandBuilder.length() - 1);
    }

    private static HashMap<String, String> generateOrderParams(TaskWorkspace taskWorkspace, String chameleonOrder) {
        HashMap<String, String> orderParams = new HashMap<>();
        orderParams.put("--config", "default_" + taskWorkspace.getId());
        if (ChameleonConstants.ORDER_NEED_CONFIG_SOURCE_LIST.contains(chameleonOrder)) {
            orderParams.put("--source", "mysql");
        }
        return orderParams;
    }
}
