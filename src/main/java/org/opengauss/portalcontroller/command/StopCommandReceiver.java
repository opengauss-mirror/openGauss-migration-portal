package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.Tools;

/**
 * The type Stop command receiver.
 */
public class StopCommandReceiver extends CommandReceiver {
    public void action(String order) {
        Tools.writeInputOrder(order);
    }
}
