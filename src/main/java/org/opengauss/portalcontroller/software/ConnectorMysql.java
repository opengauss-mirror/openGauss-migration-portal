package org.opengauss.portalcontroller.software;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.RuntimeExecTools;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Connector mysql.
 */
public class ConnectorMysql implements Software {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorMysql.class);

    public ArrayList<String> initCriticalFileList() {
        String connectorPath = PortalControl.toolsConfigParametersTable.get(Debezium.Connector.PATH);
        ArrayList<String> connectorMysqlList = new ArrayList<>();
        String jarName = PortalControl.toolsConfigParametersTable.get(Debezium.Connector.MYSQL_JAR_NAME);
        connectorMysqlList.add(PathUtils.combainPath(true, connectorPath + "debezium-connector-mysql", jarName));
        return connectorMysqlList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Debezium.Connector.MYSQL_PATH);
        hashtable.put(Parameter.INSTALL_PATH, Debezium.Connector.PATH);
        hashtable.put(Parameter.PKG_PATH, Debezium.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Debezium.Connector.MYSQL_PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Debezium.Connector.MYSQL_PKG_NAME);
        return hashtable;
    }

    public void downloadPackage() {
        try {
            RuntimeExecTools.download(Debezium.Connector.MYSQL_PKG_URL, Debezium.Connector.MYSQL_PKG_NAME);
        } catch (PortalException e) {
            e.shutDownPortal(LOGGER);
        }
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new ConnectorMysql(), download);
    }
}
