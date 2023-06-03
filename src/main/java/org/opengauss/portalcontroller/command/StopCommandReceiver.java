package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;

/**
 * The type Stop command receiver.
 */
public class StopCommandReceiver extends CommandReceiver {
    public void action(String order) {
        if (order.equals(Command.Stop.KAFKA)) {
            Tools.stopKafka();
        } else {
            Tools.writeInputOrder(order);
        }
    }
}
