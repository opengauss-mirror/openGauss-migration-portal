/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

/**
 * Portal constants
 *
 * @since 2025/4/14
 */
public class PortalConstants {
    /**
     * portal version
     */
    public static final String PORTAL_VERSION = "7.0.0rc3";

    /**
     * bin dir name
     */
    public static final String BIN_DIR_NANE = "bin";

    /**
     * config dir name
     */
    public static final String CONFIG_DIR_NANE = "config";

    /**
     * data dir name
     */
    public static final String DATA_DIR_NANE = "data";

    /**
     * logs dir name
     */
    public static final String LOGS_DIR_NANE = "logs";

    /**
     * pkg dir name
     */
    public static final String PKG_DIR_NANE = "pkg";

    /**
     * template dir name
     */
    public static final String TEMPLATE_DIR_NANE = "template";

    /**
     * tmp dir name
     */
    public static final String TMP_DIR_NANE = "tmp";

    /**
     * tools dir name
     */
    public static final String TOOLS_DIR_NANE = "tools";

    /**
     * workspace dir name
     */
    public static final String WORKSPACE_DIR_NANE = "workspace";

    /**
     * least space mb
     */
    public static final long LEAST_SPACE_MB = 900L;

    /**
     * command os
     */
    public static final String COMMAND_OS =
            "cat /etc/os-release | grep ID= | head -n 1 | awk -F '=' '{print $2}' | sed 's/\\\"//g'";

    /**
     * command os version
     */
    public static final String COMMAND_OS_VERSION =
            "cat /etc/os-release | grep VERSION_ID= | head -n 1|awk -F '=' '{print $2}' | sed 's/\\\"//g'";

    /**
     * dependencies install script dir relative path
     */
    public static final String DEPENDENCIES_INSTALL_SCRIPT_DIR_RELATIVE_PATH = "dependencies";

    /**
     * dependencies install script name
     */
    public static final String DEPENDENCIES_INSTALL_SCRIPT_NAME = "install_dependencies.sh";

    private PortalConstants() {
    }
}
