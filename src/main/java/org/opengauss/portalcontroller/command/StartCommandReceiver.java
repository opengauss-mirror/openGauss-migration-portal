package org.opengauss.portalcontroller.command;

import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StartCommandReceiver extends CommandReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartCommandReceiver.class);
    public void action(String order) {
        PortalControl.startPlan(generateTaskList(order));
    }

    public List<String> generateTaskList(String order) {
        if (order.equals(Command.Start.Plan.CURRENT)) {
            String path = PathUtils.combainPath(true, PortalControl.portalControlPath + "config", "currentPlan");
            return PortalControl.initTasklist(path);
        }
        if (order.contains(Command.MYSQL)) {
            return new ArrayList<>() {{
                add(order);
            }};
        }
        String plan = order.replaceFirst(Command.Type.START, "").trim();
        if (PortalControl.planList.containsKey(plan)) {
            return PortalControl.planList.get(plan);
        }else{
            LOGGER.error("Invalid command.");
            return new ArrayList<>();
        }
    }
}
