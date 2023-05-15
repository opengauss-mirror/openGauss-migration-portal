package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.PortalControl;

public class HelpCommandReceiver extends CommandReceiver{
    public void action(String order){
        PortalControl.help();
    }
}
