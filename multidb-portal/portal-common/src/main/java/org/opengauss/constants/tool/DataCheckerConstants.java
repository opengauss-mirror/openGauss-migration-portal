/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

/**
 * data checker constants
 *
 * @since 2025/4/19
 */
public class DataCheckerConstants {
    /**
     * tool name
     */
    public static final String TOOL_NAME = "DataChecker";

    /**
     * install pkg dir name
     */
    public static final String INSTALL_PKG_DIR_NAME = "datachecker";

    /**
     * install pkg name model
     */
    public static final String INSTALL_PKG_NAME_MODEL = "openGauss-DataCheck-%s.tar.gz";

    /**
     * install dir name
     */
    public static final String INSTALL_DIR_NAME = "datachecker";

    /**
     * data checker home dir name model
     */
    public static final String DATA_CHECKER_HOME_DIR_NAME_MODEL = "openGauss-DataCheck-%s";

    /**
     * check jar name model
     */
    public static final String CHECK_JAR_NAME_MODEL = "datachecker-check-%s.jar";

    /**
     * extract jar name model
     */
    public static final String EXTRACT_JAR_NAME_MODEL = "datachecker-extract-%s.jar";

    /**
     * data checker lib dir name
     */
    public static final String DATA_CHECKER_LIB_DIR_NAME = "lib";

    /**
     * wait process start millis
     */
    public static final int WAIT_PROCESS_START_MILLIS = 5000;

    /**
     * check result success file name
     */
    public static final String CHECK_RESULT_SUCCESS_FILE_NAME = "success.log";

    /**
     * check result failed file name
     */
    public static final String CHECK_RESULT_FAILED_FILE_NAME = "failed.log";

    /**
     * check result repair file name model
     */
    public static final String CHECK_RESULT_REPAIR_FILE_NAME_MODEL = "repair_%s_%s_0_0.txt";

    /**
     * process sign file name
     */
    public static final String PROCESS_SIGN_FILE_NAME = "process.pid";

    /**
     * source process start sign
     */
    public static final String SOURCE_PROCESS_START_SIGN = "\"endpoint\":\"SOURCE\",\"event\":\"start\"";

    /**
     * sink process start sign
     */
    public static final String SINK_PROCESS_START_SIGN = "\"endpoint\":\"SINK\",\"event\":\"start\"";

    /**
     * check process start sign
     */
    public static final String CHECK_PROCESS_START_SIGN = "\"endpoint\":\"CHECK\",\"event\":\"start\"";

    /**
     * source process stop sign
     */
    public static final String SOURCE_PROCESS_STOP_SIGN = "\"endpoint\":\"SOURCE\",\"event\":\"stop\"";

    /**
     * sink process stop sign
     */
    public static final String SINK_PROCESS_STOP_SIGN = "\"endpoint\":\"SINK\",\"event\":\"stop\"";

    /**
     * check process stop sign
     */
    public static final String CHECK_PROCESS_STOP_SIGN = "\"endpoint\":\"CHECK\",\"event\":\"stop\"";

    private DataCheckerConstants() {
    }
}
