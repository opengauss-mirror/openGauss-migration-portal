package org.opengauss.portalcontroller.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandReceiver.class);
    public void action(String order){
        LOGGER.error("Invalid command.");
    }
}
