/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants.tool;

import java.util.List;

/**
 * chameleon constants
 *
 * @since 2025/4/19
 */
public class ChameleonConstants {
    /**
     * tool name
     */
    public static final String TOOL_NAME = "Chameleon";

    /**
     * pg chameleon dir path
     */
    public static final String PG_CHAMELEON_DIR_PATH = "~/.pg_chameleon";

    /**
     * pg chameleon config dir path
     */
    public static final String PG_CHAMELEON_CONFIG_DIR_PATH = PG_CHAMELEON_DIR_PATH + "/configuration";

    /**
     * install pkg dir name
     */
    public static final String INSTALL_PKG_DIR_NAME = "chameleon";

    /**
     * install pkg name model
     */
    public static final String INSTALL_PKG_NAME_MODEL = "chameleon-%s-%s.tar.gz";

    /**
     * install dir name
     */
    public static final String INSTALL_DIR_NAME = "chameleon";

    /**
     * chameleon dir home name model
     */
    public static final String CHAMELEON_DIR_HOME_NAME_MODEL = "chameleon-%s";

    /**
     * chameleon file relative path
     */
    public static final String CHAMELEON_FILE_RELATIVE_PATH = "venv/bin/chameleon";

    /**
     * wait chameleon process start millis
     */
    public static final int WAIT_PROCESS_START_MILLIS = 2000;

    /**
     * set configuration files order
     */
    public static final String ORDER_SET_CONFIGURATION_FILES = "set_configuration_files";

    /**
     * drop replica schema order
     */
    public static final String ORDER_DROP_REPLICA_SCHEMA = "drop_replica_schema";

    /**
     * create replica schema order
     */
    public static final String ORDER_CREATE_REPLICA_SCHEMA = "create_replica_schema";

    /**
     * add source order
     */
    public static final String ORDER_ADD_SOURCE = "add_source";

    /**
     * init replica order
     */
    public static final String ORDER_INIT_REPLICA = "init_replica";

    /**
     * start trigger replica order
     */
    public static final String ORDER_START_TRIGGER_REPLICA = "start_trigger_replica";

    /**
     * start view replica order
     */
    public static final String ORDER_START_VIEW_REPLICA = "start_view_replica";

    /**
     * start func replica order
     */
    public static final String ORDER_START_FUNC_REPLICA = "start_func_replica";

    /**
     * start proc replica order
     */
    public static final String ORDER_START_PROC_REPLICA = "start_proc_replica";

    /**
     * detach replica order
     */
    public static final String ORDER_DETACH_REPLICA = "detach_replica";

    /**
     * need config param order list
     */
    public static final List<String> ORDER_NEED_CONFIG_SOURCE_LIST = List.of(
            ORDER_ADD_SOURCE, ORDER_INIT_REPLICA, ORDER_START_TRIGGER_REPLICA, ORDER_START_VIEW_REPLICA,
            ORDER_START_FUNC_REPLICA, ORDER_START_PROC_REPLICA, ORDER_DETACH_REPLICA
    );

    private ChameleonConstants() {
    }
}
