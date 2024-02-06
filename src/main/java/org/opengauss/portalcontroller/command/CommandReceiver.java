package org.opengauss.portalcontroller.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Command receiver.
 */
public class CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandReceiver.class);

    /**
     * Action.
     *
     * @param order the order
     */
    public void action(String order) {
        LOGGER.error("Invalid command.");
    }
}
