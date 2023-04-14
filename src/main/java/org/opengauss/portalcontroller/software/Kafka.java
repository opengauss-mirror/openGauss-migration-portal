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
 * The type Kafka.
 */
public class Kafka implements Software {

    public ArrayList<String> initCriticalFileList() {
        String kafkaPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH);
        ArrayList<String> kafkaList = new ArrayList<>();
        kafkaList.add(kafkaPath + "bin" + File.separator + "zookeeper-server-start.sh");
        kafkaList.add(kafkaPath + "bin" + File.separator + "zookeeper-server-stop.sh");
        kafkaList.add(kafkaPath + "config" + File.separator + "zookeeper.properties");
        kafkaList.add(kafkaPath + "bin" + File.separator + "kafka-server-start.sh");
        kafkaList.add(kafkaPath + "bin" + File.separator + "kafka-server-stop.sh");
        kafkaList.add(kafkaPath + "config" + File.separator + "server.properties");
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
        RuntimeExecTools.download(Debezium.Kafka.PKG_URL, Debezium.PKG_PATH);
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new Kafka(), download);
    }
}
