package org.opengauss.portalcontroller.constant;

public interface Parameter {
    String PATH = "path";
    String PKG_URL = "pkgUrl";
    String PKG_PATH = "pkgPath";
    String PKG_NAME = "pkgName";
    String INSTALL_PATH = "installPath";
    String PORTAL_NAME = "portalControl-1.0-SNAPSHOT-exec.jar";
    String MYSQL_CONNECTOR_SINK_NAME = "kafka mysql connector sink";
    String MYSQL_CONNECTOR_SOURCE_NAME = "kafka mysql connector source";
    String OPENGAUSS_CONNECTOR_SOURCE_NAME = "kafka opengauss connector source";
    String OPENGAUSS_CONNECTOR_SINK_NAME = "kafka opengauss connector sink";
    String INSTALL_ALL_MIGRATION_TOOLS = "Install all migration tools";
    String CHECK_FULL = "Full migration datacheck";
    String CHECK_INCREMENTAL = "Incremental migration datacheck";
    String CHECK_REVERSE = "Reverse migration datacheck";
    String ERROR_PATH = "error.path";
    String INPUT_ORDER_PATH = "input.order.path";
}
