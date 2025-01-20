/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.status;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.alert.ErrorCode;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.entity.ObjectEntry;
import org.opengauss.portalcontroller.entity.RecordVo;
import org.opengauss.portalcontroller.entity.Total;
import org.opengauss.portalcontroller.task.Plan;
import org.opengauss.portalcontroller.thread.ThreadStatusController;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.opengauss.portalcontroller.utils.LogViewUtils;
import org.opengauss.portalcontroller.utils.PathUtils;
import org.opengauss.portalcontroller.verify.DiskSpaceVerifyChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The type Change status tools.
 */
public class ChangeStatusTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStatusTools.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static Map<String, ObjectEntry> tableStatusMap = new ConcurrentHashMap<>();
    private static int lastStatus = 1;


    /**
     * Gets chameleon table status.
     *
     * @return the chameleon table status
     */
    public static ArrayList<TableStatus> getChameleonTableStatus() {
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        String path = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_init_replica.json";
        return getChameleonTableStatus(path);
    }

    /**
     * Gets chameleon table status.
     *
     * @param path the path
     * @return the chameleon table status
     */
    public static ArrayList<TableStatus> getChameleonTableStatus(String path) {
        ArrayList<TableStatus> tableStatusList = new ArrayList<>();
        String tableChameleonStatus;
        if (!("".equals(tableChameleonStatus = LogViewUtils.getFullLog(path)))) {
            JSONObject root = parseJsonStr(tableChameleonStatus);
            if (root == null) {
                return new ArrayList<>();
            }
            JSONArray table = root.getJSONArray("table");
            Iterator<Object> iterator = table.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String name = table.getJSONObject(index).getString("name");
                double percent = table.getJSONObject(index).getDouble("percent");
                int status = table.getJSONObject(index).getInteger("status");
                TableStatus tableStatus;
                if (status == Status.Object.ERROR) {
                    String errorMsg = table.getJSONObject(index).getString("error");
                    tableStatus = new TableStatus(name, status, percent, errorMsg);
                } else {
                    tableStatus = new TableStatus(name, status, percent);
                }
                tableStatusList.add(tableStatus);
                index++;
                iterator.next();
            }
        }
        return tableStatusList;
    }

    /**
     * getdataCheckTableStatus
     *
     * @param tableStatusList tableStatusList
     * @return ArrayList<TableStatus>
     */
    public static ArrayList<TableStatus> getdataCheckTableStatus(ArrayList<TableStatus> tableStatusList) {
        boolean isFullCheck = PortalControl.status >= Status.START_FULL_MIGRATION_CHECK;
        String checkResultPath = PortalControl.toolsConfigParametersTable.get(Check.Result.FULL_CURRENT);
        if (new File(checkResultPath).exists() && isFullCheck) {
            tableStatusList = getDatacheckTableStatus(tableStatusList);
            String progressPath = checkResultPath + "progress.log";
            String destinationPath = PortalControl.toolsConfigParametersTable.get(Status.FULL_CHECK_PATH);
            changeDatacheckSpeedStatus(progressPath, destinationPath);
        }
        return tableStatusList;
    }

    /**
     * Gets datacheck table status.
     *
     * @param tableStatusArrayList the table status array list
     * @return the datacheck table status
     */
    public static ArrayList<TableStatus> getDatacheckTableStatus(ArrayList<TableStatus> tableStatusArrayList) {
        String successPath = PortalControl.toolsConfigParametersTable.get(Check.Result.FULL_CURRENT) + "success.log";
        String failPath = PortalControl.toolsConfigParametersTable.get(Check.Result.FULL_CURRENT) + "failed.log";
        return getDatacheckTableStatus(successPath, tableStatusArrayList, failPath);
    }

    /**
     * Gets chameleon object status.
     *
     * @param name  the name
     * @param order the order
     * @return the chameleon object status
     */
    public static ArrayList<ObjectStatus> getChameleonObjectStatus(String name, String order) {
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        String path = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_" + order + ".json";
        if (!new File(path).exists()) {
            path = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_init_replica.json";
        }
        ArrayList<ObjectStatus> objectStatusList = new ArrayList<>();
        String chameleonStr = LogViewUtils.getFullLogNoSeparator(path);

        if (!("".equals(chameleonStr))) {
            JSONObject root = parseJsonStr(chameleonStr);
            if (root == null) {
                return new ArrayList<>();
            }
            if (root.getJSONArray(name) != null) {
                JSONArray objects = root.getJSONArray(name);
                Iterator iterator = objects.iterator();
                int index = 0;
                while (iterator.hasNext()) {
                    String objectName = objects.getJSONObject(index).getString("name");
                    int status = objects.getJSONObject(index).getInteger("status");
                    ObjectStatus objectStatus;
                    if (status == Status.Object.ERROR) {
                        String errorMsg = objects.getJSONObject(index).getString("error");
                        objectStatus = new ObjectStatus(objectName, status, errorMsg);
                    } else {
                        objectStatus = new ObjectStatus(objectName, status);
                    }
                    objectStatusList.add(objectStatus);
                    index++;
                    iterator.next();
                }
            }
        }
        return objectStatusList;
    }


    /**
     * Gets all chameleon status.
     *
     * @return the all chameleon status
     */
    public static FullMigrationStatus getAllChameleonStatus() throws IOException {
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        RecordVo recordVo = new RecordVo();
        parseChameleonStatus(chameleonVenvPath, recordVo);
        List<TableStatus> tableStatusArrayList = translateMigrationStatusObject(recordVo.getTable(), translateMigrationTableStatusObject());
        List<ObjectStatus> viewStatusArrayList = translateMigrationStatusObject(recordVo.getView(), translateMigrationStatusObject());
        List<ObjectStatus> functionStatusArrayList = translateMigrationStatusObject(recordVo.getFunction(), translateMigrationStatusObject());
        List<ObjectStatus> procedureStatusArrayList = translateMigrationStatusObject(recordVo.getProcedure(), translateMigrationStatusObject());
        List<ObjectStatus> triggerStatusArrayList = translateMigrationStatusObject(recordVo.getTrigger(), translateMigrationStatusObject());
        return new FullMigrationStatus(recordVo.getTotal(), new ArrayList<>(tableStatusArrayList), new ArrayList<>(viewStatusArrayList), new ArrayList<>(functionStatusArrayList),
                new ArrayList<>(triggerStatusArrayList), new ArrayList<>(procedureStatusArrayList));
    }

    private static <T> List<T> translateMigrationStatusObject(List<ObjectEntry> list, Function<ObjectEntry, T> function) {
        return list.stream().map(function).collect(Collectors.toList());
    }

    private static Function<ObjectEntry, ObjectStatus> translateMigrationStatusObject() {
        return entry -> new ObjectStatus(entry.getName(), entry.getStatus(), entry.getError());
    }

    private static Function<ObjectEntry, TableStatus> translateMigrationTableStatusObject() {
        return tab -> new TableStatus(tab.getName(), tab.getStatus(), tab.getPercent(), tab.getError());
    }


    private static void parseChameleonStatus(String chameleonVenvPath, RecordVo recordVo) throws IOException {
        String fileName = chameleonVenvPath + "data_default_" + Plan.workspaceId;
        Path tablePath = Path.of(fileName + "_init_replica.json");
        Path viewPath = Path.of(fileName + "_start_view_replica.json");
        Path funcPath = Path.of(fileName + "_start_func_replica.json");
        Path triggerPath = Path.of(fileName + "_start_trigger_replica.json");
        Path procPath = Path.of(fileName + "_start_proc_replica.json");

        if (!Files.exists(tablePath) && !Files.exists(viewPath) && !Files.exists(funcPath)
                && !Files.exists(triggerPath) && !Files.exists(procPath)) {
            initRecordVo(recordVo);
            return;
        }

        RecordVo table = parseRecord(tablePath);
        RecordVo view = parseRecord(viewPath);
        RecordVo func = parseRecord(funcPath);
        RecordVo trigger = parseRecord(triggerPath);
        RecordVo proc = parseRecord(procPath);

        initFunctionRecordVo(recordVo, func);
        initViewRecordVo(recordVo, view);
        initTableRecordVo(recordVo, table);
        initTriggerRecordVo(recordVo, trigger);
        initProcedureRecordVo(recordVo, proc);
    }

    private static void initRecordVo(RecordVo recordVo) {
        recordVo.setTrigger(List.of());
        recordVo.setProcedure(List.of());
        recordVo.setFunction(List.of());
        recordVo.setView(List.of());
        recordVo.setTable(List.of());
        recordVo.setTotal(new Total());
    }

    private static void initFunctionRecordVo(RecordVo recordVo, RecordVo func) {
        if (func != null) {
            recordVo.setFunction(func.getFunction());
        } else {
            recordVo.setFunction(List.of());
        }
    }

    private static void initViewRecordVo(RecordVo recordVo, RecordVo view) {
        if (view != null) {
            recordVo.setView(view.getView());
        } else {
            recordVo.setView(List.of());
        }
    }

    private static void initTableRecordVo(RecordVo recordVo, RecordVo table) {
        if (table != null) {
            recordVo.setTotal(table.getTotal());
            recordVo.setTable(table.getTable());
        } else {
            recordVo.setTotal(new Total());
            recordVo.setTable(List.of());
        }
    }

    private static void initTriggerRecordVo(RecordVo recordVo, RecordVo trigger) {
        if (trigger != null) {
            recordVo.setTrigger(trigger.getTrigger());
        } else {
            recordVo.setTrigger(List.of());
        }
    }

    private static void initProcedureRecordVo(RecordVo recordVo, RecordVo proc) {
        if (proc != null) {
            recordVo.setProcedure(proc.getProcedure());
        } else {
            recordVo.setProcedure(List.of());
        }
    }

    private static RecordVo parseRecord(Path filePath) {
        String text = "";
        try {
            if (Files.exists(filePath)) {
                text = Files.readString(filePath);
            }
            if (StringUtils.isEmpty(text)) {
                return null;
            }
            return objectMapper.readValue(text, RecordVo.class);
        } catch (IOException e) {
            LOGGER.warn("read or parse status ", e.getMessage());
            return null;
        }
    }
    /**
     * Change full status.
     */
    public static void changeFullStatus() {
        FullMigrationStatus tempFullMigrationStatus;
        try {
            tempFullMigrationStatus = getAllChameleonStatus();
        } catch (JSONException | IOException e) {
            LOGGER.error("{}", ErrorCode.IO_EXCEPTION, e);
            tempFullMigrationStatus = ThreadStatusController.getFullMigrationStatus();
        }
        ThreadStatusController.setFullMigrationStatus(tempFullMigrationStatus);

        String fullMigrationStatusString = JSON.toJSONString(ThreadStatusController.getFullMigrationStatus());
        FileUtils.writeFile(fullMigrationStatusString,
                PortalControl.toolsConfigParametersTable.get(Status.FULL_PATH), false);
    }

    /**
     * Change incremental status int.
     *
     * @param sourcePath           the source migration status path
     * @param sinkPath             the sink migration status path
     * @param incrementalPath      the incremental migration status path
     * @param incrementalOrReverse the incremental or reverse
     */
    public static void changeIncrementalStatus(String sourcePath, String sinkPath, String incrementalPath,
                                               boolean incrementalOrReverse) {
        try {
            String sourceLog = LogViewUtils.getFullLogNoSeparator(sourcePath);
            String sinkLog = LogViewUtils.getFullLogNoSeparator(sinkPath);
            if (!sourceLog.equals("") && !sinkLog.equals("")) {
                IncrementalMigrationStatus incrementalMigrationStatus = new IncrementalMigrationStatus();
                JSONObject sourceObject = JSONObject.parseObject(sourceLog);
                JSONObject sinkObject = JSONObject.parseObject(sinkLog);
                incrementalMigrationStatus.setCount(sinkObject.getInteger(Parameter.IncrementalStatus.REPLAYED_COUNT)
                        + sinkObject.getInteger(Parameter.IncrementalStatus.OVER_ALL_PIPE));
                incrementalMigrationStatus.setSourceSpeed(sourceObject.getInteger(Parameter.IncrementalStatus.SPEED));
                incrementalMigrationStatus.setSinkSpeed(sinkObject.getInteger(Parameter.IncrementalStatus.SPEED));
                incrementalMigrationStatus.setRest(sinkObject.getInteger(Parameter.IncrementalStatus.OVER_ALL_PIPE));
                incrementalMigrationStatus.setFailCount(sinkObject.getInteger(Parameter.IncrementalStatus.FAIL));
                incrementalMigrationStatus.setSuccessCount(sinkObject.getInteger(Parameter.IncrementalStatus.SUCCESS));
                incrementalMigrationStatus.setReplayedCount(sinkObject.getInteger(
                        Parameter.IncrementalStatus.REPLAYED_COUNT));
                String failSqlPath;
                if (incrementalOrReverse) {
                    incrementalMigrationStatus.setSkippedCount(sinkObject.getInteger(Parameter.IncrementalStatus.SKIP)
                            + sinkObject.getInteger(Parameter.IncrementalStatus.SKIPPED_EXCLUDE_EVENT_COUNT));
                    String incrementFolder = PortalControl.toolsConfigParametersTable.get(Status.INCREMENTAL_FOLDER);
                    failSqlPath = PathUtils.combainPath(true, incrementFolder, "fail-sql.txt");
                } else {
                    incrementalMigrationStatus.setSkippedCount(sourceObject.getInteger(
                            Parameter.IncrementalStatus.SKIPPED_EXCLUDE_COUNT));
                    String reverseFolder = PortalControl.toolsConfigParametersTable.get(Status.REVERSE_FOLDER);
                    failSqlPath = PathUtils.combainPath(true, reverseFolder, "fail-sql.txt");
                }
                int status = Status.Incremental.RUNNING;
                if (PortalControl.status == Status.ERROR || !LogViewUtils.getFullLog(failSqlPath).equals("")) {
                    status = Status.Incremental.ERROR;
                    String msg = "Please read " + failSqlPath + " to get fail sqls.";
                    incrementalMigrationStatus.setMsg(msg);
                }
                incrementalMigrationStatus.setStatus(status);
                FileUtils.writeFile(JSON.toJSONString(incrementalMigrationStatus), incrementalPath, false);
            }
        } catch (Exception ignored) {
        }
    }


    /**
     * Write portal status.
     */
    public static void writePortalStatus() {
        if (ThreadStatusController.isEqualLastPortalStatus(PortalControl.status)) {
            return;
        } else {
            PortalStatusWriter portalStatusWriter;
            if (PortalControl.status == Status.ERROR) {
                portalStatusWriter = new PortalStatusWriter(PortalControl.status, System.currentTimeMillis(),
                    PortalControl.errorMsg);
                ThreadStatusController.addPortalStatusWriterList(portalStatusWriter);
            } else {
                portalStatusWriter = new PortalStatusWriter(PortalControl.status, System.currentTimeMillis());
                ThreadStatusController.addPortalStatusWriterList(portalStatusWriter);
            }
        }
        String str = JSON.toJSONString(ThreadStatusController.getPortalStatusWriterList());
        FileUtils.writeFile(str, PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH), false);
    }

    /**
     * Write portal status.
     */
    public static void reduceDiskSpace() {
        LOGGER.info("isReduced:{},Plan.stopPlan:{},PortalControl.status:{}", ThreadStatusController.isReduced(),
                Plan.stopPlan, PortalControl.status);
        if (Plan.stopPlan || PortalControl.status >= Status.FULL_MIGRATION_CHECK_FINISHED) {
            if (ThreadStatusController.isReduced()) {
                return;
            }
            if (!PortalControl.taskList.contains(Command.Start.Mysql.FULL)) {
                return;
            }
            Connection mysqlConnection = null;
            try {
                mysqlConnection = JdbcUtils.getMysqlConnection();
                DiskSpaceVerifyChain.readAndWrite(
                        DiskSpaceVerifyChain.getMaxTableSpace(mysqlConnection, false).multiply(BigDecimal.valueOf(-1)),
                        new HashMap<>(), true);
                ThreadStatusController.setIsReduced(true);
            } finally {
                try {
                    if (mysqlConnection != null) {
                        mysqlConnection.close();
                    }
                } catch (SQLException e) {
                    LOGGER.error("{}close PgConnection fail.", ErrorCode.SQL_EXCEPTION);
                }
            }
        }
    }

    /**
     * Output chameleon table status.
     */
    public static void outputChameleonTableStatus() {
        LOGGER.info("Table:");
        String path = PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH);
        ArrayList<TableStatus> tableStatusArrayList = getChameleonTableStatus(path);
        for (TableStatus tableStatus : tableStatusArrayList) {
            LOGGER.info("Name: " + tableStatus.getName() + ", percent: " + tableStatus.getPercent() + ", status: " + Status.Object.HASHTABLE.get(tableStatus.getStatus()));
        }
    }

    /**
     * Output chameleon object status.
     *
     * @param name the name
     */
    public static void outputChameleonObjectStatus(String name) {
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        LOGGER.info("{}:", name);
        String path = PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH);
        ArrayList<ObjectStatus> tableStatusArrayList = getChameleonObjectStatus(name, path);
        for (ObjectStatus objectStatus : tableStatusArrayList) {
            LOGGER.info("Name: {}, status: {}", objectStatus.getName(),
                    Status.Object.HASHTABLE.get(objectStatus.getStatus()));
        }
    }

    /**
     * Output chameleon status.
     */
    public static void outputChameleonStatus() {
        outputChameleonTableStatus();
        outputChameleonObjectStatus("view");
        outputChameleonObjectStatus("function");
        outputChameleonObjectStatus("trigger");
        outputChameleonObjectStatus("procedure");
    }

    /**
     * Output incremental status.
     *
     * @param path the path
     */
    public static void outputIncrementalStatus(String path) {
        String tempStr = LogViewUtils.getFullLogNoSeparator(path);
        if (!tempStr.isEmpty()) {
            JSONObject jsonObject = JSONObject.parseObject(tempStr);
            int status = jsonObject.getInteger("status");
            int count = jsonObject.getInteger("count");
            int sourceSpeed = jsonObject.getInteger("sourceSpeed");
            int sinkSpeed = jsonObject.getInteger("sinkSpeed");
            String msg = jsonObject.getString("msg");
            if (status == Status.Incremental.RUNNING && PortalControl.status == Status.RUNNING_INCREMENTAL_MIGRATION) {
                LOGGER.info("Incremental migration status: running");
            } else if (status == Status.Incremental.RUNNING
                    && PortalControl.status == Status.INCREMENTAL_MIGRATION_FINISHED) {
                LOGGER.info("Incremental migration status: finished");
            } else {
                PortalControl.status = Status.ERROR;
                PortalControl.errorMsg = msg;
                LOGGER.info("Incremental migration status: error, message: " + msg);
            }
            int rest = jsonObject.getInteger("rest");
            LOGGER.info("Count: " + count + ", sourceSpeed: " + sourceSpeed + ", sinkSpeed: " + sinkSpeed + ", rest: "
                    + rest);
        }
    }

    /**
     * Gets portal status.
     *
     * @return the portal status
     */
    public static int getPortalStatus() {
        int status = 0;
        String str =
                LogViewUtils.getFullLogNoSeparator(PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH));
        JSONArray array = JSONArray.parseArray(str);
        Iterator iterator = array.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            status = array.getJSONObject(index).getInteger("status");
            index++;
            iterator.next();
        }
        return status;
    }

    /**
     * Gets datacheck table status.
     *
     * @param successPath          the success path
     * @param tableStatusArrayList the table status array list
     * @param failPath             the fail path
     * @return the datacheck table status
     */
    public static ArrayList<TableStatus> getDatacheckTableStatus(String successPath,
                                                                 ArrayList<TableStatus> tableStatusArrayList,
                                                                 String failPath) {
        String successStr = LogViewUtils.getFullLog(successPath);
        if (!successStr.equals("")) {
            successStr = "[" + successStr.substring(0, successStr.length() - 1) + "]";
            JSONArray array = JSONArray.parseArray(successStr);
            Iterator iterator = array.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String tableName = array.getJSONObject(index).getString("table");
                for (TableStatus tableStatus : tableStatusArrayList) {
                    if (tableStatus.getName().equals(tableName)) {
                        tableStatus.setPercent(1.0);
                        tableStatus.setStatus(Status.Object.FULL_MIGRATION_CHECK_FINISHED);
                        break;
                    }
                }
                index++;
                iterator.next();
            }
        }
        String failStr = LogViewUtils.getFullLog(failPath);
        if (!failStr.equals("")) {
            failStr = "[" + failStr.substring(0, failStr.length() - 1) + "]";
            JSONArray array = JSONArray.parseArray(failStr);
            Iterator iterator = array.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String tableName = array.getJSONObject(index).getString("table");
                for (TableStatus tableStatus : tableStatusArrayList) {
                    if (tableStatus.getName().equals(tableName)) {
                        tableStatus.setStatus(Status.Object.CHECK_FAILED);
                        String errorMsg = array.getJSONObject(index).getString("message");
                        errorMsg += "If you want to repair data.please read the following files:";
                        String repairFileName =
                                "repair_" + PortalControl.toolsMigrationParametersTable.get(Mysql.DATABASE_NAME) + "_"
                                        + tableName + "_0_0.txt";
                        String repairPath = PathUtils.combainPath(true,
                                PortalControl.toolsConfigParametersTable.get(Check.Result.FULL) + "result",
                                repairFileName);
                        errorMsg += repairPath;
                        tableStatus.setErrorMsg(errorMsg);
                        break;
                    }
                }
                index++;
                iterator.next();
            }
        }
        return tableStatusArrayList;
    }

    /**
     * Get chameleon total status object.
     *
     * @return the object
     */
    public static Object getChameleonTotalStatus() {
        String chameleonVenvPath = PortalControl.toolsConfigParametersTable.get(Chameleon.VENV_PATH);
        String path = chameleonVenvPath + "data_default_" + Plan.workspaceId + "_init_replica.json";
        String tableChameleonStatus = LogViewUtils.getFullLogNoSeparator(path);
        if (!("".equals(tableChameleonStatus))) {
            JSONObject root = parseJsonStr(tableChameleonStatus);
            if (root == null) {
                return "";
            }
            return JSONObject.parseObject(root.getString("total"));
        }
        return "";
    }

    private static JSONObject parseJsonStr(String jsonStr) {
        JSONObject root = null;
        try {
            root = JSONObject.parseObject(jsonStr);
        } catch (JSONException exp) {
            LOGGER.warn("not read a complete json string, continue.");
        }
        return root;
    }

    /**
     * Change datacheck speed status.
     *
     * @param progressPath the progress path
     * @param statusPath   the status path
     */
    public static void changeDatacheckSpeedStatus(String progressPath, String statusPath) {
        if (new File(progressPath).exists()) {
            String progressStr = LogViewUtils.lastLine(progressPath);
            FileUtils.writeFile(progressStr, statusPath, false);
        } else {
            LOGGER.info("Get datacheck progess failed.Use old progress.");
        }
    }
}
