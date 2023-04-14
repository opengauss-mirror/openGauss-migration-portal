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
 * The type Confluent.
 */
public class Confluent implements Software {
    public ArrayList<String> initCriticalFileList() {
        ArrayList<String> confluentList = new ArrayList<>();
        String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        confluentList.add(confluentPath + "bin" + File.separator + "schema-registry-start");
        confluentList.add(confluentPath + "bin" + File.separator + "schema-registry-stop");
        confluentList.add(confluentPath + "etc" + File.separator + "schema-registry" + File.separator + "schema-registry.properties");
        confluentList.add(confluentPath + "bin" + File.separator + "connect-standalone");
        return confluentList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Debezium.Confluent.PATH);
        hashtable.put(Parameter.INSTALL_PATH, Debezium.PATH);
        hashtable.put(Parameter.PKG_PATH, Debezium.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Debezium.Confluent.PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Debezium.Confluent.PKG_NAME);
        return hashtable;
    }

    public void downloadPackage() {
        RuntimeExecTools.download(Debezium.Confluent.PKG_URL, Debezium.PKG_PATH);
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new Confluent(), download);
    }
}
