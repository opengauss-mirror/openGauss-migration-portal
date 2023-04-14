package org.opengauss.portalcontroller.software;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.RuntimeExecTools;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Parameter;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Datacheck.
 */
public class Datacheck implements Software {
    public ArrayList<String> initCriticalFileList() {
        ArrayList<String> datacheckList = new ArrayList<>();
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        datacheckList.add(datacheckPath + "datachecker-extract-0.0.1.jar");
        datacheckList.add(datacheckPath + "datachecker-check-0.0.1.jar");
        return datacheckList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Check.PATH);
        hashtable.put(Parameter.INSTALL_PATH, Check.INSTALL_PATH);
        hashtable.put(Parameter.PKG_PATH, Check.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Check.PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Check.PKG_NAME);
        return hashtable;
    }

    public void downloadPackage() {
        RuntimeExecTools.download(Check.PKG_URL, Check.PKG_PATH);
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new Datacheck(), download);
    }
}
