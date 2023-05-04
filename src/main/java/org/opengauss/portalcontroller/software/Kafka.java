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
 * The type Kafka.
 */
public class Kafka implements Software {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kafka.class);

    public ArrayList<String> initCriticalFileList() {
        String kafkaPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH);
        ArrayList<String> kafkaList = new ArrayList<>();
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "zookeeper-server-start.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "zookeeper-server-stop.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "config", "zookeeper.properties"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "kafka-server-start.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "kafka-server-stop.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "config", "server.properties"));
        return kafkaList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Debezium.Kafka.PATH);
        hashtable.put(Parameter.INSTALL_PATH, Debezium.PATH);
        hashtable.put(Parameter.PKG_PATH, Debezium.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Debezium.Kafka.PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Debezium.Kafka.PKG_NAME);
        return hashtable;
    }

    public void downloadPackage() {
        try {
            RuntimeExecTools.download(Debezium.Kafka.PKG_URL, Debezium.PKG_PATH);
        } catch (PortalException e) {
            e.shutDownPortal(LOGGER);
        }
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new Kafka(), download);
    }
}
