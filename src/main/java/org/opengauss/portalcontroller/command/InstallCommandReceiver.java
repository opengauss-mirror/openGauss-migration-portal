package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;

/**
 * The type Install command receiver.
 */
public class InstallCommandReceiver extends CommandReceiver {
    public void action(String order) {
        InstallMigrationTools installMigrationTools = new InstallMigrationTools();
        if (Tools.containString(order, Command.ALL)) {
            installMigrationTools.runAllInstallOrder(order);
        } else {
            installMigrationTools.runInstallOrder(order);
        }
        Tools.sleepThread(1000, "unzip package");
        if (!Tools.containString(order, Command.FULL)) {
            Tools.prepareConfluent();
            Tools.startKafka();
        }
    }
}
