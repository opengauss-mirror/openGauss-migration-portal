package org.opengauss.portalcontroller.software;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.RuntimeExecTools;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Datacheck.
 */
public class Datacheck implements Software {
    private static final Logger LOGGER = LoggerFactory.getLogger(Datacheck.class);

    public ArrayList<String> initCriticalFileList() {
        ArrayList<String> datacheckList = new ArrayList<>();
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String datacheckExtractName = PortalControl.toolsConfigParametersTable.get(Check.EXTRACT_NAME);
        String datacheckCheckName = PortalControl.toolsConfigParametersTable.get(Check.CHECK_NAME);
        datacheckList.add(datacheckPath + datacheckExtractName);
        datacheckList.add(datacheckPath + datacheckCheckName);
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
        try {
            RuntimeExecTools.download(Check.PKG_URL, Check.PKG_PATH);
        } catch (PortalException e) {
            e.shutDownPortal(LOGGER);
        }
    }

    @Override
    public void install(boolean download) {
        InstallMigrationTools.installSingleMigrationTool(new Datacheck(), download);
    }
}
