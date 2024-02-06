/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.tools.mysql;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.workspaceId;

/**
 * ReverseDatacheckTool
 *
 * @date :2023/11/3 15:22
 * @description: ReverseDatacheckTool
 * @version: 1.1
 * @since 1.1
 */
public class ReverseDatacheckTool extends FullDatacheckTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReverseDatacheckTool.class);

    /**
     * Change datacheck parameters.
     */
    @Override
    public void initWorkSpaceParams(String workspaceId) {
        checkConfigParams.put("data.check.data-path", toolsConfigParametersTable.get(Check.Result.REVERSE));
        checkSourceParams.put("spring.extract.debezium-enable", true);
        checkSinkParams.put("spring.extract.debezium-enable", true);
        String sourceTopic = PropertitesUtils.getSinglePropertiesParameter("transforms.route.replacement",
                toolsConfigParametersTable.get(Debezium.Source.REVERSE_CONFIG_PATH));
        checkSourceParams.put("spring.extract.debezium-topic", sourceTopic);
        String sinkTopic = PropertitesUtils.getSinglePropertiesParameter("transforms.route.replacement",
                toolsConfigParametersTable.get(Debezium.Sink.REVERSE_CONFIG_PATH));
        checkSinkParams.put("spring.extract.debezium-topic", sinkTopic);
    }

    /**
     * init
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean init(String workspaceId) {
        LOGGER.info("reverse datacheck tool start init");
        super.initConfigChangeParamsMap();
        setAllParams(workspaceId);
        changeAllConfig();
        deleteParamsConifg();
        return true;
    }

    /**
     * start
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean start(String workspaceId) {
        fileCheck.startCheck();
        Task.startDataCheck(fileCheck.getCheckResultListener());
        stop();
        return true;
    }

    /**
     * stop
     *
     * @return boolean
     */
    public boolean stop() {
        while (!Plan.stopPlan && !Plan.stopReverseMigration) {
            LOGGER.info("Reverse migration is running...");
            LogViewUtils.outputInformation(checkStatus(workspaceId),
                    Parameter.CHECK_REVERSE + " is running.", Parameter.CHECK_FULL + " has error.");
            ProcessUtils.sleepThread(1000, "running reverse migraiton datacheck");
        }
        if (Plan.stopReverseMigration) {
            if (PortalControl.status != Status.ERROR) {
                PortalControl.status = Status.REVERSE_MIGRATION_FINISHED;
                Plan.pause = true;
                ProcessUtils.sleepThread(50, "pausing the plan");
            }
            Task.stopTaskMethod(Method.Run.CHECK);
            Task.stopTaskMethod(Method.Run.CHECK_SINK);
            Task.stopTaskMethod(Method.Run.CHECK_SOURCE);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SINK);
            Task.stopTaskMethod(Method.Run.REVERSE_CONNECT_SOURCE);
            LOGGER.info("Reverse migration stopped.");
            fileCheck.stopListener();
        }
        return true;
    }

    /**
     * uninstall
     *
     * @return boolean
     */
    public boolean uninstall() {
        String errorPath = PortalControl.portalErrorPath;
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Confluent.PATH));
        filePaths.add(hashtable.get(Debezium.Connector.MYSQL_PATH));
        filePaths.add(hashtable.get(Debezium.Connector.OPENGAUSS_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        InstallMigrationUtils.removeSingleMigrationToolFiles(filePaths, errorPath);
        return true;
    }
}
