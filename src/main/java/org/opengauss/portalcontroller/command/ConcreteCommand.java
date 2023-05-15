package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.constant.Command;

public class ConcreteCommand {
    public void execute(String order) {
        generateReceiver(order).action(order);
    }

    public CommandReceiver generateReceiver(String order) {
        if (order.contains(Command.Type.UNINSTALL)) return new UninstallCommandReceiver();
        if (order.contains(Command.Type.INSTALL)) return new InstallCommandReceiver();
        if (order.contains(Command.Type.START)) return new StartCommandReceiver();
        if (order.contains(Command.Type.HELP)) return new HelpCommandReceiver();
        if (order.contains(Command.Type.SHOW)) return new ShowCommandReceiver();
        if (order.contains(Command.Type.RUN)) return new RunCommandReceiver();
        if (order.contains(Command.Type.STOP)) return new StopCommandReceiver();
        return new CommandReceiver();
    }
}
