package org.opengauss.portalcontroller.software;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.RuntimeExecTools;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Parameter;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Connector mysql.
 */
public class ConnectorMysql implements Software {
    public ArrayList<String> initCriticalFileList() {
        String connectorPath = PortalControl.toolsConfigParametersTable.get(Debezium.Connector.PATH);
        ArrayList<String> connectorMysqlList = new ArrayList<>();
        connectorMysqlList.add(connectorPath + "debezium-connector-mysql" + File.separator + "debezium-connector-mysql-1.8.1.Final.jar");
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
        RuntimeExecTools.download(Debezium.Connector.MYSQL_PKG_URL, Debezium.Connector.MYSQL_PKG_NAME);
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new ConnectorMysql(), download);
    }
}
