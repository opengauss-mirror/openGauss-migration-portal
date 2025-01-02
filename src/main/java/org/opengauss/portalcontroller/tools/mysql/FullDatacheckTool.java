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

import com.alibaba.fastjson.JSON;
import org.apache.logging.log4j.util.Strings;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.LogParseConstants;
import org.opengauss.portalcontroller.constant.Method;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.StartPort;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.enums.ToolsConfigEnum;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.DataCheckLogFileCheck;
import org.opengauss.portalcontroller.software.Confluent;
import org.opengauss.portalcontroller.software.Datacheck;
import org.opengauss.portalcontroller.software.Software;
import org.opengauss.portalcontroller.thread.ThreadStatusController;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.task.Task;
import org.opengauss.portalcontroller.tools.Tool;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.InstallMigrationUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.ParamsUtils;
import org.opengauss.portalcontroller.utils.ProcessUtils;
import org.opengauss.portalcontroller.utils.YmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;
import static org.opengauss.portalcontroller.PortalControl.toolsMigrationParametersTable;
import static org.opengauss.portalcontroller.PortalControl.workspaceId;
import static org.opengauss.portalcontroller.status.ChangeStatusTools.getdataCheckTableStatus;

/**
 * FullDatacheckTool
 *
 * @date :2023/11/3 15:22
 * @description: FullDatacheckTool
 * @version: 1.1
 * @since 1.1
 */
public class FullDatacheckTool extends ParamsConfig implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(FullDatacheckTool.class);

    /**
     *  DataCheckLogFileCheck
     */
    protected DataCheckLogFileCheck fileCheck = new DataCheckLogFileCheck();

    Map<String, Object> checkSourceParams = null;
    Map<String, Object> checkSinkParams = null;
    Map<String, Object> checkConfigParams = null;
    Map<String, Object> debeziumConfigParams = null;


    @Override
    public void initConfigChangeParamsMap() {
        checkSourceParams = new HashMap<>();
        checkSinkParams = new HashMap<>();
        checkConfigParams = new HashMap<>();
        debeziumConfigParams = new HashMap<>();
        this.configYmlChangeParamsMap.put(Check.Source.CONFIG_PATH, checkSourceParams);
        this.configYmlChangeParamsMap.put(Check.Sink.CONFIG_PATH, checkSinkParams);
        this.configYmlChangeParamsMap.put(Check.CONFIG_PATH, checkConfigParams);
        this.configPropsChangeParamsMap.put(Debezium.Connector.CONFIG_PATH, debeziumConfigParams);
    }


    @Override
    public boolean install(boolean isDownload) {
        ArrayList<Software> softwareArrayList = new ArrayList<>();
        softwareArrayList.add(new Confluent());
        softwareArrayList.add(new Datacheck());
        InstallMigrationUtils installMigrationUtils = new InstallMigrationUtils();
        for (Software software : softwareArrayList) {
            try {
                installMigrationUtils.installSingleMigrationSoftware(software, isDownload);
            } catch (PortalException e) {
                LOGGER.error("install failed", e);
                return false;
            }
        }
        LogViewUtils.outputResult(true, Command.Install.Mysql.Check.DEFAULT);
        return true;
    }

    @Override
    public boolean init(String workspaceId) {
        LOGGER.info("full data check tool start init");
        // 初始化参数文件和修改参数集合 修改参数集合填充 修改到配置文件
        initConfigChangeParamsMap();
        setAllParams(workspaceId);
        changeAllConfig();
        deleteParamsConifg();
        // 删除参数
        return true;
    }

    @Override
    public void initDataBaseParams() {
        int checkPort = StartPort.CHECK + PortalControl.portId * 10;
        checkSourceParams.put("spring.extract.debezium-enable", false);
        String mysqlDatabaseName = toolsMigrationParametersTable.get(Mysql.DATABASE_NAME);
        checkSourceParams.put(Check.Parameters.SCHEMA, mysqlDatabaseName);
        String mysqlDatabasePort = toolsMigrationParametersTable.get(Mysql.DATABASE_PORT);
        String mysqlDatabaseHost = toolsMigrationParametersTable.get(Mysql.DATABASE_HOST);
        String mysqlDatacheckUrl =
                "jdbc:mysql://" + mysqlDatabaseHost + ":" + mysqlDatabasePort + "/" + mysqlDatabaseName + "?useSSL"
                        + "=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&allowPublicKeyRetrieval"
                        + "=true";
        checkSourceParams.put(Check.Parameters.URL, mysqlDatacheckUrl);
        String mysqlUserName = toolsMigrationParametersTable.get(Mysql.USER);
        checkSourceParams.put(Check.Parameters.USER_NAME, mysqlUserName);
        String mysqlUserPassword = toolsMigrationParametersTable.get(Mysql.PASSWORD);
        checkSourceParams.put(Check.Parameters.PASSWORD, mysqlUserPassword);
        ArrayList<Integer> portList = ParamsUtils.getAvailablePorts(checkPort, 3, 1000);
        int servicePort = portList.get(2);
        checkSourceParams.put("spring.check.server-uri", "http://127.0.0.1:" + servicePort);
        int sourcePort = portList.get(0);
        checkSourceParams.put("server.port", sourcePort);
        checkSourceParams.put("logging.config", toolsConfigParametersTable.get(Check.LOG_PATTERN_PATH));
        String opengaussDatabaseSchema = toolsMigrationParametersTable.get(Opengauss.DATABASE_SCHEMA);
        checkSinkParams.put(Check.Parameters.SCHEMA, opengaussDatabaseSchema);
        String opengaussDatabaseHost = toolsMigrationParametersTable.get(Opengauss.DATABASE_HOST);
        String opengaussDatabasePort = toolsMigrationParametersTable.get(Opengauss.DATABASE_PORT);
        String opengaussDatabaseName = toolsMigrationParametersTable.get(Opengauss.DATABASE_NAME);
        String opengaussDatacheckUrl = "jdbc:opengauss://" + opengaussDatabaseHost + ":" + opengaussDatabasePort
                + "/" + opengaussDatabaseName + "?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone"
                + "=UTC";
        checkSinkParams.put("spring.extract.debezium-enable", false);
        checkSinkParams.put(Check.Parameters.URL, opengaussDatacheckUrl);
        String opengaussUserName = toolsMigrationParametersTable.get(Opengauss.USER);
        checkSinkParams.put(Check.Parameters.USER_NAME, opengaussUserName);
        String opengaussUserPassword = toolsMigrationParametersTable.get(Opengauss.PASSWORD);
        checkSinkParams.put(Check.Parameters.PASSWORD, opengaussUserPassword);
        checkSinkParams.put("spring.check.server-uri", "http://127.0.0.1:" + servicePort);
        int sinkPort = portList.get(1);
        checkSinkParams.put("server.port", sinkPort);
        checkSinkParams.put("logging.config", toolsConfigParametersTable.get(Check.LOG_PATTERN_PATH));
        checkConfigParams.put("data.check.source-uri", "http://127.0.0.1:" + sourcePort);
        checkConfigParams.put("data.check.sink-uri", "http://127.0.0.1:" + sinkPort);
        checkConfigParams.put("server.port", servicePort);
        checkConfigParams.put("data.check.data-path", toolsConfigParametersTable.get(Check.Result.FULL));
        checkConfigParams.put("logging.config", toolsConfigParametersTable.get(Check.LOG_PATTERN_PATH));
    }

    @Override
    void initWorkSpaceParams(String workspaceId) {

    }

    @Override
    void initInteractionParams() {

    }


    /**
     * delete The Parameter Configuration Of Datacheck
     */
    @Override
    public void initParmasFromEnvForDelete() {
        String deleteAppConifgKeys = System.getProperty(ToolsConfigEnum.DATA_CHECK_APPLICATION.getConfigName());
        if (Strings.isNotBlank(deleteAppConifgKeys)) {
            configDeleteParamsMap.put(Check.CONFIG_PATH, List.of(deleteAppConifgKeys.split(",")));
        }
        String deleteSinkConfigKeys = System.getProperty(ToolsConfigEnum.DATA_CHECK_APPLICATION_SINK.getConfigName());
        if (Strings.isNotBlank(deleteAppConifgKeys)) {
            configDeleteParamsMap.put(Check.Sink.CONFIG_PATH, List.of(deleteSinkConfigKeys.split(",")));
        }
        String deleteSourceConfigKeys =
                System.getProperty(ToolsConfigEnum.DATA_CHECK_APPLICATION_SOURCE.getConfigName());
        if (Strings.isNotBlank(deleteAppConifgKeys)) {
            configDeleteParamsMap.put(Check.Source.CONFIG_PATH, List.of(deleteSourceConfigKeys.split(",")));
        }
    }

    /**
     *
     * initParmasFromEnvForAddAndChange
     */
    @Override
    public void initParmasFromEnvForAddAndChange() {
        checkConfigParams.putAll(YmlUtils.getChangeToolsYmlParameters(ToolsConfigEnum.DATA_CHECK_APPLICATION));
        checkSinkParams.putAll(YmlUtils.getChangeToolsYmlParameters(ToolsConfigEnum.DATA_CHECK_APPLICATION_SINK));
        checkSourceParams.putAll(YmlUtils.getChangeToolsYmlParameters(ToolsConfigEnum.DATA_CHECK_APPLICATION_SOURCE));
    }

    /**
     *
     * initKafkaParams
     */
    @Override
    public void initKafkaParams() {
        MigrationConfluentInstanceConfig portalConfig = MigrationConfluentInstanceConfig.getInstanceFromPortalConfig();
        String schemaRegistryPrefix = "http://";
        checkSourceParams.put("spring.extract.debezium-avro-registry",
                schemaRegistryPrefix + portalConfig.getSchemaRegistryIpPort());
        checkSourceParams.put("spring.kafka.bootstrap-servers", portalConfig.getKafkaIpPort());
        checkSinkParams.put("spring.extract.debezium-avro-registry",
                schemaRegistryPrefix + portalConfig.getSchemaRegistryIpPort());
        checkSinkParams.put("spring.kafka.bootstrap-servers", portalConfig.getKafkaIpPort());
        // application.yml文件修改
        checkConfigParams.put("spring.kafka.bootstrap-servers", portalConfig.getKafkaIpPort());
    }

    /**
     * start
     *
     * @param workspaceId  workspaceId
     * @return boolean
     */
    @Override
    public boolean start(String workspaceId) {
        fileCheck.startCheck();
        DataCheckLogFileCheck.setDataCheckFinish(false);
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.START_FULL_MIGRATION_CHECK;
        }
        Task.startDataCheck(fileCheck.getCheckResultListener());
        if (PortalControl.status != Status.ERROR) {
            PortalControl.status = Status.RUNNING_FULL_MIGRATION_CHECK;
        }
        stop();
        return true;
    }

    /**
     * stop
     *
     * @return boolean
     */
    public boolean stop() {
        while (!Plan.stopPlan) {
            fileCheck.checkFullDataCheckStop();
            if (ProcessUtils.getCommandPid(Task.getTaskProcessMap().get(Method.Run.CHECK)) == -1
                    && DataCheckLogFileCheck.isDataCheckFinish()) {
                if (PortalControl.status != Status.ERROR) {
                    LOGGER.info("Full migration datacheck is finished.");
                    PortalControl.status = Status.FULL_MIGRATION_CHECK_FINISHED;
                    fileCheck.stopListener();
                }
                break;
            }
            LogViewUtils.outputInformation(checkStatus(workspaceId),
                    Parameter.CHECK_FULL + " is running.", Parameter.CHECK_FULL + " has error.");
            ProcessUtils.sleepThread(LogParseConstants.PERIOD_WATCH_LOG, "running full migration datacheck");
        }
        return true;
    }

    /**
     * uninstall
     *
     * @return boolean
     */
    @Override
    public boolean uninstall() {
        Hashtable<String, String> hashtable = toolsConfigParametersTable;
        String errorPath = PortalControl.portalErrorPath;
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add(hashtable.get(Debezium.Kafka.TMP_PATH));
        filePaths.add(hashtable.get(Debezium.Zookeeper.TMP_PATH));
        filePaths.add(hashtable.get(Check.PATH));
        InstallMigrationUtils.removeSingleMigrationToolFiles(filePaths, errorPath);
        return true;
    }

    /**
     * checkStatus
     *
     * @param workspaceId  workspaceId
     * @return boolean
     */
    @Override
    public boolean checkStatus(String workspaceId) {
        return fileCheck.getErrResult();
    }

    /**
     * reportProgress
     *
     * @param workspaceId  workspaceId
     * @return boolean
     */
    @Override
    public boolean reportProgress(String workspaceId) {
        ThreadStatusController.getFullMigrationStatus().setTable(
                getdataCheckTableStatus(ThreadStatusController.getFullMigrationStatus().getTable()));
        String fullMigrationStatusString = JSON.toJSONString(ThreadStatusController.getFullMigrationStatus());
        FileUtils.writeFile(fullMigrationStatusString,
                PortalControl.toolsConfigParametersTable.get(Status.FULL_PATH), false);
        return true;
    }
}
