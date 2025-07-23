/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.tool;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.tool.DataCheckerConstants;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DataCheckerProcessType;
import org.opengauss.migration.tools.DataChecker;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Data-checker helper
 *
 * @since 2025/5/14
 */
public class DataCheckerHelper {
    private static final Logger LOGGER = LogManager.getLogger(DataCheckerHelper.class);

    private DataCheckerHelper() {
    }

    /**
     * Parse data-checker status file to json array
     *
     * @param statusFilePath status file path
     * @return JDONArray data-checker status
     */
    public static Optional<JSONArray> parseDataCheckerStatusFile(String statusFilePath) {
        Path statusPath = Path.of(statusFilePath);

        if (!Files.exists(statusPath)) {
            return Optional.empty();
        }

        try {
            String text = Files.readString(statusPath);
            if (!StringUtils.isNullOrBlank(text)) {
                text = "[" + text.substring(0, text.length() - 1) + "]";
                return Optional.ofNullable(JSONArray.parseArray(text));
            }
        } catch (IOException | JSONException e) {
            LOGGER.warn("Failed to read or parse data-checker progress, error: {}", e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Generate data check process start command
     *
     * @param processType process type
     * @param configFilePath config file path
     * @param jvmPrefixOptions jvm prefix options
     * @return process start command
     */
    public static String generateProcessStartCommand(
            DataCheckerProcessType processType, String configFilePath, String jvmPrefixOptions) {
        StringBuilder builder = new StringBuilder();
        builder.append("nohup java").append(" ")
                .append(jvmPrefixOptions).append(" ")
                .append("-Dloader.path=").append(DataChecker.getInstance().getLibDirPath()).append(" ")
                .append(generateProcessCheckCommand(processType, configFilePath)).append(" ")
                .append("> /dev/null &");

        return builder.toString();
    }

    /**
     * Generate data check process check command
     *
     * @param processType process type
     * @param configFilePath config file path
     * @return process check command
     */
    public static String generateProcessCheckCommand(DataCheckerProcessType processType, String configFilePath) {
        StringBuilder builder = new StringBuilder();
        builder.append("-Dspring.config.additional-location=").append(configFilePath).append(" ")
                .append("-jar").append(" ");

        DataChecker dataChecker = DataChecker.getInstance();
        if (DataCheckerProcessType.SINK.equals(processType)) {
            builder.append(dataChecker.getExtractJarPath()).append(" ")
                    .append("--").append(processType.getType());
        } else if (DataCheckerProcessType.SOURCE.equals(processType)) {
            builder.append(dataChecker.getExtractJarPath()).append(" ")
                    .append("--").append(processType.getType());
        } else {
            builder.append(dataChecker.getCheckJarPath());
        }

        return builder.toString();
    }

    /**
     * Get data-checker full process sign file path
     *
     * @param taskWorkspace task workspace
     * @return process sign file path
     */
    public static String getFullProcessSignFilePath(TaskWorkspace taskWorkspace) {
        String resultDirPath = getFullCheckResultDirPath(taskWorkspace);
        return String.format("%s/%s", resultDirPath, DataCheckerConstants.PROCESS_SIGN_FILE_NAME);
    }

    /**
     * Get data-checker incremental process sign file path
     *
     * @param taskWorkspace task workspace
     * @return process sign file path
     */
    public static String getIncrementalProcessSignFilePath(TaskWorkspace taskWorkspace) {
        String resultDirPath = getIncrementalCheckResultDirPath(taskWorkspace);
        return String.format("%s/%s", resultDirPath, DataCheckerConstants.PROCESS_SIGN_FILE_NAME);
    }

    /**
     * Generate full data check data path
     *
     * @param workspace task workspace
     * @return full data check data path
     */
    public static String generateFullDataCheckDataPath(TaskWorkspace workspace) {
        return workspace.getStatusFullDataCheckDirPath();
    }

    /**
     * Generate incremental data check data path
     *
     * @param workspace task workspace
     * @return incremental data check data path
     */
    public static String generateIncrementalDataCheckDataPath(TaskWorkspace workspace) {
        return workspace.getStatusIncrementalDataCheckDirPath();
    }

    /**
     * Generate full data check logs dir path
     *
     * @param workspace task workspace
     * @return log path
     */
    public static String generateFullDataCheckLogsDirPath(TaskWorkspace workspace) {
        return workspace.getLogsFullDataCheckDirPath();
    }

    /**
     * generate incremental data check logs dir path
     *
     * @param workspace task workspace
     * @return log path
     */
    public static String generateIncrementalDataCheckLogsDirPath(TaskWorkspace workspace) {
        return workspace.getLogsIncrementalDataCheckDirPath();
    }

    /**
     * Get data-checker process start sign
     *
     * @param processType process type
     * @return process start sign
     */
    public static String getProcessStartSign(DataCheckerProcessType processType) {
        if (DataCheckerProcessType.SOURCE.equals(processType)) {
            return DataCheckerConstants.SOURCE_PROCESS_START_SIGN;
        } else if (DataCheckerProcessType.SINK.equals(processType)) {
            return DataCheckerConstants.SINK_PROCESS_START_SIGN;
        } else {
            return DataCheckerConstants.CHECK_PROCESS_START_SIGN;
        }
    }

    /**
     * Get data-checker process stop sign
     *
     * @param processType process type
     * @return process stop sign
     */
    public static String getProcessStopSign(DataCheckerProcessType processType) {
        if (DataCheckerProcessType.SOURCE.equals(processType)) {
            return DataCheckerConstants.SOURCE_PROCESS_STOP_SIGN;
        } else if (DataCheckerProcessType.SINK.equals(processType)) {
            return DataCheckerConstants.SINK_PROCESS_STOP_SIGN;
        } else {
            return DataCheckerConstants.CHECK_PROCESS_STOP_SIGN;
        }
    }

    /**
     * Get data-checker full check result dir path
     *
     * @param taskWorkspace task workspace
     * @return check result dir path
     */
    public static String getFullCheckResultDirPath(TaskWorkspace taskWorkspace) {
        String statusPath = taskWorkspace.getStatusFullDataCheckDirPath();
        return String.format("%s/result", statusPath);
    }

    /**
     * Get data-checker incremental check result dir path
     *
     * @param taskWorkspace task workspace
     * @return check result dir path
     */
    public static String getIncrementalCheckResultDirPath(TaskWorkspace taskWorkspace) {
        String statusPath = taskWorkspace.getStatusIncrementalDataCheckDirPath();
        return String.format("%s/result", statusPath);
    }

    /**
     * Get data-checker full check success result file path
     *
     * @param taskWorkspace task workspace
     * @return full check success result file path
     */
    public static String getFullCheckResultSuccessFilePath(TaskWorkspace taskWorkspace) {
        String resultDirPath = getFullCheckResultDirPath(taskWorkspace);
        return String.format("%s/%s", resultDirPath, DataCheckerConstants.CHECK_RESULT_SUCCESS_FILE_NAME);
    }

    /**
     * get data-checker incremental check success result file path
     *
     * @param taskWorkspace task workspace
     * @return incremental check success result file path
     */
    public static String getIncrementalCheckResultSuccessFilePath(TaskWorkspace taskWorkspace) {
        String resultDirPath = getIncrementalCheckResultDirPath(taskWorkspace);
        return String.format("%s/%s", resultDirPath, DataCheckerConstants.CHECK_RESULT_SUCCESS_FILE_NAME);
    }

    /**
     * Get data-checker full check failed result file path
     *
     * @param taskWorkspace task workspace
     * @return full check failed result file path
     */
    public static String getFullCheckResultFailedFilePath(TaskWorkspace taskWorkspace) {
        String resultDirPath = getFullCheckResultDirPath(taskWorkspace);
        return String.format("%s/%s", resultDirPath, DataCheckerConstants.CHECK_RESULT_FAILED_FILE_NAME);
    }

    /**
     * get data-checker incremental check failed result file path
     *
     * @param taskWorkspace task workspace
     * @return incremental check failed result file path
     */
    public static String getIncrementalCheckResultFailedFilePath(TaskWorkspace taskWorkspace) {
        String resultDirPath = getIncrementalCheckResultDirPath(taskWorkspace);
        return String.format("%s/%s", resultDirPath, DataCheckerConstants.CHECK_RESULT_FAILED_FILE_NAME);
    }

    /**
     * generate data-checker full check result repair file path
     *
     * @param taskWorkspace task workspace
     * @param schemaName schema name
     * @param tableName table name
     * @return full check result repair file path
     */
    public static String generateFullCheckResultRepairFilePath(TaskWorkspace taskWorkspace, String schemaName,
                                                               String tableName) {
        String resultDirPath = getFullCheckResultDirPath(taskWorkspace);
        String repairFileName = String.format(DataCheckerConstants.CHECK_RESULT_REPAIR_FILE_NAME_MODEL,
                schemaName, tableName);
        return String.format("%s/%s", resultDirPath, repairFileName);
    }

    /**
     * Generate data-checker incremental check result repair file path
     *
     * @param taskWorkspace task workspace
     * @param schemaName schema name
     * @param tableName table name
     * @return incremental check result repair file path
     */
    public static String generateIncrementalCheckResultRepairFilePath(TaskWorkspace taskWorkspace, String schemaName,
                                                                      String tableName) {
        String resultDirPath = getIncrementalCheckResultDirPath(taskWorkspace);
        String repairFileName = String.format(DataCheckerConstants.CHECK_RESULT_REPAIR_FILE_NAME_MODEL,
                schemaName, tableName);
        return String.format("%s/%s", resultDirPath, repairFileName);
    }

    /**
     * Get full check log4j2 config map
     *
     * @param taskWorkspace task workspace
     * @return full check log4j2 config
     */
    public static Map<String, Object> getFullCheckLog4j2Config(TaskWorkspace taskWorkspace) {
        return getLog4j2Config(generateFullDataCheckLogsDirPath(taskWorkspace));
    }

    /**
     * Get incremental check log4j2 config map
     *
     * @param taskWorkspace task workspace
     * @return incremental check log4j2 config
     */
    public static Map<String, Object> getIncrementalCheckLog4j2Config(TaskWorkspace taskWorkspace) {
        return getLog4j2Config(generateIncrementalDataCheckLogsDirPath(taskWorkspace));
    }

    private static Map<String, Object> getLog4j2Config(String logDirPath) {
        Map<String, Object> changeParams = new HashMap<>();
        String configModel = "<Property name=\"LOG_HOME\">%s</Property>";
        String changeString = String.format(configModel, "logs");
        String newString = String.format(configModel, logDirPath);
        changeParams.put(changeString, newString);
        return changeParams;
    }
}
