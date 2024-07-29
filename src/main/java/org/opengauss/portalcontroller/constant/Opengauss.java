package org.opengauss.portalcontroller.constant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;

import java.util.HashMap;
import java.util.Map;

import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;

public interface Opengauss {
    String USER = "opengauss.user.name";
    String PASSWORD = "opengauss.user.password";
    String DATABASE_HOST = "opengauss.database.host";
    String DATABASE_PORT = "opengauss.database.port";
    String DATABASE_ISCLUSTER = "opengauss.database.iscluster";
    String DATABASE_STANDBY_HOSTS = "opengauss.database.standby.hostnames";
    String DATABASE_STANDBY_PORTS = "opengauss.database.standby.ports";
    String DATABASE_NAME = "opengauss.database.name";
    String DATABASE_SCHEMA = "opengauss.database.schema";

    /**
     * get logger
     *
     * @return Logger
     */
    private static Logger logger() {
        return LoggerFactory.getLogger(Opengauss.class);
    }

    /**
     * get opengauss.database.iscluster default value
     *
     * @return String
     */
    public static String getDatabaseIsClusterDefaultValue() {
        return "false";
    }

    /**
     * determine whether the cluster is available
     *
     * @return boolean
     */
    public static boolean isOpengaussClusterAvailable() {
        if (!isOpengaussCluster()) {
            return false;
        }
        return isStandbyInformationAvailable();
    }

    /**
     * get openGauss standby information map
     *
     * @return Map<String, String[]>
     */
    public static Map<String, String[]> getStandbyInformationMap() {
        HashMap<String, String[]> result = new HashMap<>();
        result.put(DATABASE_STANDBY_HOSTS, toolsMigrationParametersTable.get(DATABASE_STANDBY_HOSTS).split(","));
        result.put(DATABASE_STANDBY_PORTS, toolsMigrationParametersTable.get(DATABASE_STANDBY_PORTS).split(","));
        return result;
    }

    /**
     * is opengauss cluster
     *
     * @return boolean
     */
    private static boolean isOpengaussCluster() {
        String isCluster = toolsMigrationParametersTable.get(DATABASE_ISCLUSTER);
        if (isCluster == null || isCluster.equals("false")) {
            return false;
        }
        if (isCluster.equals("true")) {
            return true;
        }
        logger().warn("Invalid \"opengauss.database.iscluster\": {}", isCluster);
        return false;
    }

    /**
     * determine whether the cluster standby information is available
     *
     * @return boolean
     */
    private static boolean isStandbyInformationAvailable() {
        String standbyHosts = toolsMigrationParametersTable.get(DATABASE_STANDBY_HOSTS);
        String standbyPorts = toolsMigrationParametersTable.get(DATABASE_STANDBY_PORTS);
        if (ObjectUtils.isEmpty(standbyHosts) || ObjectUtils.isEmpty(standbyPorts)) {
            logger().warn("The \"opengauss.database.standby.hostnames\" "
                    + "or \"opengauss.database.standby.ports\" is empty.");
            return false;
        }
        if (standbyHosts.split(",").length != standbyPorts.split(",").length) {
            logger().warn("The number of hostname in \"opengauss.database.standby.hostnames\" "
                    + "does not match the number of port in \"opengauss.database.standby.ports\".");
            return false;
        }
        return true;
    }
}
