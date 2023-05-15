package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Show command receiver.
 */
public class ShowCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowCommandReceiver.class);
    public void action(String order) {
        switch (order) {
            case Command.Show.PLAN:
                PortalControl.showPlanList();
                break;
            case Command.Show.STATUS:
                PortalControl.showStatus();
                break;
            case Command.Show.INFORMATION:
                PortalControl.showMigrationParameters();
                break;
            case Command.Show.PARAMETERS:
                PortalControl.showParameters();
                break;
            default:
                LOGGER.error("Invalid command.");
                break;
        }
    }
}
