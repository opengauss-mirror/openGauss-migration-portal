package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Command;

/**
 * The type Concrete command.
 */
public class ConcreteCommand {
    /**
     * Execute.
     *
     * @param order the order
     */
    public void execute(String order) {
        generateReceiver(order).action(order);
    }

    /**
     * Generate receiver command receiver.
     *
     * @param order the order
     * @return the command receiver
     */
    public CommandReceiver generateReceiver(String order) {
        if (Tools.containString(order, Command.Type.UNINSTALL)) return new UninstallCommandReceiver();
        if (Tools.containString(order, Command.Type.INSTALL)) return new InstallCommandReceiver();
        if (Tools.containString(order, Command.Type.START)) return new StartCommandReceiver();
        if (Tools.containString(order, Command.Type.HELP)) return new HelpCommandReceiver();
        if (Tools.containString(order, Command.Type.SHOW)) return new ShowCommandReceiver();
        if (Tools.containString(order, Command.Type.RUN)) return new RunCommandReceiver();
        if (Tools.containString(order, Command.Type.STOP)) return new StopCommandReceiver();
        if (Tools.containString(order, Command.Type.STATUS)) {
            return new CheckPortalStatusCommandReceiver();
        }
        return new CommandReceiver();
    }
}
