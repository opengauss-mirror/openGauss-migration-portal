package org.opengauss.portalcontroller.context;

import org.opengauss.portalcontroller.InstallMigrationTools;
import org.opengauss.portalcontroller.check.CheckTask;
import org.opengauss.portalcontroller.check.CheckTaskMysqlFullMigration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PortalContext {
    static Map<Object, CheckTask> taskInstanceMap = new ConcurrentHashMap<>();

    static {
        taskInstanceMap.put("", new CheckTaskMysqlFullMigration());
    }


    public static CheckTask getIns(Object parm) {
        return taskInstanceMap.get(parm);
    }
    public static CheckTask getAIns(Object parm) {
        return taskInstanceMap.get(parm);
    }
    public static InstallMigrationTools getAIns(){
        return new InstallMigrationTools();
    }
}
