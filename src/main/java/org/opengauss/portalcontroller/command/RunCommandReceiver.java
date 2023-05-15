package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.Tools;

/**
 * The type Run command receiver.
 */
public class RunCommandReceiver extends CommandReceiver{
    public void action(String order){
        Tools.writeInputOrder(order);
    }
}
