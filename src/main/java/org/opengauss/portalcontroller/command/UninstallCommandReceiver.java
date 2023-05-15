package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;

public class UninstallCommandReceiver extends CommandReceiver {
    public void action(String order) {
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        if (Tools.containString(order, Command.ALL)) {
            installMigrationTools.uninstallAllMigrationTools();
        } else {
            installMigrationTools.uninstallMigrationTools(order);
        }
    }
}
