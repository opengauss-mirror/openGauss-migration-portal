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
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.workspaceId;

/**
 * IncrementalDatacheckTool
 *
 * @date :2023/11/3 15:22
 * @description: IncrementalDatacheckTool
 * @version: 1.1
 * @since 1.1
 */
public class IncrementalDatacheckTool extends FullDatacheckTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalDatacheckTool.class);

    private final IncrementalMigrationTool incrementalMigrationTool = new IncrementalMigrationTool();

    /**
     * init
     *
     * @param workspaceId workspaceId
     * @return boolean
     */
    @Override
    public boolean init(String workspaceId) {
        LOGGER.info("increment data check tool start init");
        super.initConfigChangeParamsMap();
        setAllParams(workspaceId);
        changeAllConfig();
        deleteParamsConifg();
        return true;
    }

    /**
     * initWorkSpaceParams
     *
     * @param workspaceId workspaceId
     */
    @Override
    public void initWorkSpaceParams(String workspaceId) {
        checkConfigParams.put("data.check.data-path", toolsConfigParametersTable.get(Check.Result.INCREMENTAL));
        checkSourceParams.put("spring.extract.debezium-enable", true);
        checkSinkParams.put("spring.extract.debezium-enable", true);
        String sourceTopic = PropertitesUtils.getSinglePropertiesParameter("transforms.route.replacement",
                toolsConfigParametersTable.get(Debezium.Source.INCREMENTAL_CONFIG_PATH));
        checkSourceParams.put("spring.extract.debezium-topic", sourceTopic);
        String sinkTopic = PropertitesUtils.getSinglePropertiesParameter("transforms.route.replacement",
                toolsConfigParametersTable.get(Debezium.Sink.INCREMENTAL_CONFIG_PATH));
        checkSinkParams.put("spring.extract.debezium-topic", sinkTopic);
        debeziumConfigParams.put("offset.storage.file.filename", PathUtils.combainPath(true,
                PortalControl.portalControlPath + "tmp", "connect.offsets"));
    }

    @Override
    public boolean start(String workspaceId) {
        fileCheck.startCheck();
        Task.startDataCheck(fileCheck.getCheckResultListener());
        incrementMigrationResumeBrokenTransfer();
        stop();
        return true;
    }

    private void incrementMigrationResumeBrokenTransfer() {
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration) {
            LOGGER.info("Incremental migration is running...");
            if (StringUtils.hasLength(Plan.runIncrementalMigrationEndpoint)) {
                LOGGER.info("resume broken transfer of incremental migration endpoint: {}",
                    Plan.runIncrementalMigrationEndpoint);
                startConnectMigrationEndpoint(Plan.runIncrementalMigrationEndpoint);
                Plan.runIncrementalMigrationEndpoint = "";
                Plan.pause = false;
                break;
            }
            ProcessUtils.sleepThread(1000, "running incremental migration");
        }
    }

    private void startConnectMigrationEndpoint(String connectMigrationEndpoint) {
        LOGGER.info("incrementMigrationResumeBrokenTransfer start task  {}", connectMigrationEndpoint);
        Task.startTaskMethod(connectMigrationEndpoint, 5000, "", new LogFileListener());
        PortalControl.status = Status.RUNNING_INCREMENTAL_MIGRATION;
        Plan.pause = false;
    }

    @Override
    public boolean stop() {
        while (!Plan.stopPlan && !Plan.stopIncrementalMigration) {
            LOGGER.info("Incremental migration is running...");
            LogViewUtils.outputInformation(checkStatus(workspaceId),
                    Parameter.CHECK_INCREMENTAL + " is running.", Parameter.CHECK_FULL + " has error.");
            ProcessUtils.sleepThread(1000, "running incremental migraiton datacheck");
        }
        List<String> taskThreadList = List.of(Method.Run.CHECK, Method.Run.CHECK_SINK, Method.Run.CHECK_SOURCE,
                Method.Run.CONNECT_SINK, Method.Run.CONNECT_SOURCE);
        if (Plan.stopIncrementalMigration) {
            incrementalMigrationTool.beforeStop(taskThreadList);
        }
        fileCheck.stopListener();
        return true;
    }

    public boolean uninstall() {
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
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
