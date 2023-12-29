package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.command.mysql.CheckPortalStatusCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.InstallCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.LoadToolsConfigCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.RunCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.StartCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.StopCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.UninstallCommandReceiver;
import org.opengauss.portalcontroller.command.mysql.VerifyCommandReceiver;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.utils.CommandUtils;

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
        if (CommandUtils.containString(order, Command.Type.UNINSTALL)) return new UninstallCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.INSTALL)) return new InstallCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.START)) return new StartCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.HELP)) return new HelpCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.SHOW)) return new ShowCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.RUN)) return new RunCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.STOP)) return new StopCommandReceiver();
        if (CommandUtils.containString(order, Command.Type.STATUS)) {
            return new CheckPortalStatusCommandReceiver();
        }
        if (CommandUtils.containString(order, Command.Type.LOAD)) {
            return new LoadToolsConfigCommandReceiver();
        }
        if (CommandUtils.containString(order, Command.Type.VERIFY)) {
            return new VerifyCommandReceiver();
        }
        return new CommandReceiver();
    }
}
