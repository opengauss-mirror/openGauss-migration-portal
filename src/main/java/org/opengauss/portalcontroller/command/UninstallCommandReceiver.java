package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;

/**
 * The type Uninstall command receiver.
 */
public class UninstallCommandReceiver extends CommandReceiver {
    public void action(String order) {
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        Tools.stopKafka();
        if (Tools.containString(order, Command.ALL)) {
            installMigrationTools.uninstallAllMigrationTools();
        } else {
            installMigrationTools.uninstallMigrationTools(order);
        }
    }
}
