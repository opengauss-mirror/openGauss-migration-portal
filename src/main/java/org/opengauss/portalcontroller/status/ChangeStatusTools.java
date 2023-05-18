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
import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.Plan;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Mysql;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.constant.Status;
import org.opengauss.portalcontroller.exception.PortalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The type Change status tools.
 */
public class ChangeStatusTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStatusTools.class);

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
        File file = new File(path);
        String tableChameleonStatus;
        if (!(tableChameleonStatus = Tools.readFile(file)).equals("")) {
            JSONObject root = JSONObject.parseObject(tableChameleonStatus);
            JSONArray table = root.getJSONArray("table");
            Iterator<Object> iterator = table.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String name = table.getJSONObject(index).getString("name");
                double percent = table.getJSONObject(index).getDouble("percent");
                int status = table.getJSONObject(index).getInteger("status");
                TableStatus tableStatus = new TableStatus(name, status, percent);
                tableStatusList.add(tableStatus);
                index++;
                iterator.next();
            }
            boolean isFullCheck = PortalControl.status >= Status.START_FULL_MIGRATION_CHECK;
            String checkResultPath = PortalControl.toolsConfigParametersTable.get(Check.Result.FULL_CURRENT);
            if (new File(checkResultPath).exists() && isFullCheck) {
                tableStatusList = getDatacheckTableStatus(tableStatusList);
                String progressPath = checkResultPath + "progress.log";
                String destinationPath = PortalControl.toolsConfigParametersTable.get(Status.FULL_CHECK_PATH);
                Tools.changeDatacheckSpeedStatus(progressPath, destinationPath);
            }
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
        File file = new File(path);
        return getChameleonObjectStatus(name, file);
    }

    /**
     * Gets chameleon object status.
     *
     * @param name the name
     * @param file the file
     * @return the chameleon object status
     */
    public static ArrayList<ObjectStatus> getChameleonObjectStatus(String name, File file) {
        ArrayList<ObjectStatus> objectStatusList = new ArrayList<>();
        String chameleonStr;
        if (file.exists()) {
            chameleonStr = Tools.readFile(file);
            if (!chameleonStr.equals("")) {
                JSONObject root = JSONObject.parseObject(chameleonStr);
                if (root.getJSONArray(name) != null) {
                    JSONArray objects = root.getJSONArray(name);
                    Iterator iterator = objects.iterator();
                    int index = 0;
                    while (iterator.hasNext()) {
                        String objectName = objects.getJSONObject(index).getString("name");
                        int status = objects.getJSONObject(index).getInteger("status");
                        ObjectStatus objectStatus = new ObjectStatus(objectName, status);
                        objectStatusList.add(objectStatus);
                        index++;
                        iterator.next();
                    }
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
    public static FullMigrationStatus getAllChameleonStatus() {
        Object total = getChameleonTotalStatus();
        ArrayList<TableStatus> tableStatusArrayList = getChameleonTableStatus();
        ArrayList<ObjectStatus> viewStatusArrayList = getChameleonObjectStatus("view", "start_view_replica");
        ArrayList<ObjectStatus> functionStatusArrayList = getChameleonObjectStatus("function", "start_func_replica");
        ArrayList<ObjectStatus> triggerStatusArrayList = getChameleonObjectStatus("trigger", "start_trigger_replica");
        ArrayList<ObjectStatus> procedureStatusArrayList = getChameleonObjectStatus("procedure", "start_proc_replica");
        return new FullMigrationStatus(total, tableStatusArrayList, viewStatusArrayList, functionStatusArrayList, triggerStatusArrayList, procedureStatusArrayList);
    }

    /**
     * Change full status.
     */
    public static void changeFullStatus() {
        FullMigrationStatus tempFullMigrationStatus;
        String fullMigrationStatusString;
        try {
            tempFullMigrationStatus = getAllChameleonStatus();
        } catch (JSONException e) {
            tempFullMigrationStatus = ThreadStatusController.fullMigrationStatus;
        }
        ThreadStatusController.fullMigrationStatus = tempFullMigrationStatus;
        fullMigrationStatusString = JSON.toJSONString(ThreadStatusController.fullMigrationStatus);
        Tools.writeFile(fullMigrationStatusString, new File(PortalControl.toolsConfigParametersTable.get(Status.FULL_PATH)), false);
    }

    /**
     * Change incremental status int.
     *
     * @param sourcePath           the source migration status path
     * @param sinkPath             the sink migration status path
     * @param incrementalPath      the incremental migration status path
     * @param incrementalOrReverse the incremental or reverse
     */
    public static void changeIncrementalStatus(String sourcePath, String sinkPath, String incrementalPath, boolean incrementalOrReverse) {
        JSONObject sourceObject = JSONObject.parseObject(Tools.readFile(new File(sourcePath)));
        JSONObject sinkObject = JSONObject.parseObject(Tools.readFile(new File(sinkPath)));
        IncrementalMigrationStatus incrementalMigrationStatus = new IncrementalMigrationStatus();
        incrementalMigrationStatus.setCount(sinkObject.getInteger(Parameter.IncrementalStatus.REPLAYED_COUNT) + sinkObject.getInteger(Parameter.IncrementalStatus.OVER_ALL_PIPE));
        incrementalMigrationStatus.setSourceSpeed(sourceObject.getInteger(Parameter.IncrementalStatus.SPEED));
        incrementalMigrationStatus.setSinkSpeed(sinkObject.getInteger(Parameter.IncrementalStatus.SPEED));
        incrementalMigrationStatus.setRest(sinkObject.getInteger(Parameter.IncrementalStatus.OVER_ALL_PIPE));
        incrementalMigrationStatus.setFailCount(sinkObject.getInteger(Parameter.IncrementalStatus.FAIL));
        incrementalMigrationStatus.setSuccessCount(sinkObject.getInteger(Parameter.IncrementalStatus.SUCCESS));
        incrementalMigrationStatus.setReplayedCount(sinkObject.getInteger(Parameter.IncrementalStatus.REPLAYED_COUNT));
        String failSqlPath;
        if (incrementalOrReverse) {
            incrementalMigrationStatus.setSkippedCount(sinkObject.getInteger(Parameter.IncrementalStatus.SKIPPED) + sinkObject.getInteger(Parameter.IncrementalStatus.SKIPPED_EXCLUDE_EVENT_COUNT));
            failSqlPath = PathUtils.combainPath(true, PortalControl.toolsConfigParametersTable.get(Status.INCREMENTAL_FOLDER), "fail-sql.txt");
        } else {
            incrementalMigrationStatus.setSkippedCount(sourceObject.getInteger(Parameter.IncrementalStatus.SKIPPED_EXCLUDE_EVENT_COUNT));
            failSqlPath = PathUtils.combainPath(true, PortalControl.toolsConfigParametersTable.get(Status.REVERSE_FOLDER), "fail-sql.txt");
        }
        int status = Status.Incremental.RUNNING;
        if (PortalControl.status == Status.ERROR || !Tools.readFile(new File(failSqlPath)).equals("")) {
            status = Status.Incremental.ERROR;
            String msg = "Please read " + failSqlPath + " to get fail sqls.";
            incrementalMigrationStatus.setMsg(msg);
        }
        incrementalMigrationStatus.setStatus(status);
        Tools.writeFile(JSON.toJSONString(incrementalMigrationStatus), new File(incrementalPath), false);
    }


    /**
     * Write portal status.
     */
    public static void writePortalStatus() {
        try {
            FileWriter fw = new FileWriter(PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH));
            PortalStatusWriter portalStatusWriter;
            if (PortalControl.status == Status.ERROR) {
                portalStatusWriter = new PortalStatusWriter(PortalControl.status, System.currentTimeMillis(), PortalControl.errorMsg);
            } else {
                portalStatusWriter = new PortalStatusWriter(PortalControl.status, System.currentTimeMillis());
            }
            ThreadStatusController.portalStatusWriterArrayList.add(portalStatusWriter);
            String str = JSON.toJSONString(ThreadStatusController.portalStatusWriterArrayList);
            fw.write(str);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "writing portal status", e.getMessage());
            portalException.setRequestInformation("Write portal status failed");
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(e.toString());
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
        LOGGER.info(name + ":");
        String path = PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH);
        File file = new File(path);
        ArrayList<ObjectStatus> tableStatusArrayList = getChameleonObjectStatus(name, file);
        for (ObjectStatus objectStatus : tableStatusArrayList) {
            LOGGER.info("Name: " + objectStatus.getName() + ", status: " + Status.Object.HASHTABLE.get(objectStatus.getStatus()));
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
        String tempStr = Tools.readFile(new File(path));
        if (!tempStr.equals("")) {
            JSONObject root = JSONObject.parseObject(tempStr);
            int status = root.getInteger("status");
            int count = root.getInteger("count");
            int sourceSpeed = root.getInteger("sourceSpeed");
            int sinkSpeed = root.getInteger("sinkSpeed");
            int rest = root.getInteger("rest");
            String msg = root.getString("msg");
            if (status == Status.Incremental.RUNNING && PortalControl.status == Status.RUNNING_INCREMENTAL_MIGRATION) {
                LOGGER.info("Incremental migration status: running");
            } else if (status == Status.Incremental.RUNNING && PortalControl.status == Status.INCREMENTAL_MIGRATION_FINISHED) {
                LOGGER.info("Incremental migration status: finished");
            } else {
                PortalControl.status = Status.ERROR;
                PortalControl.errorMsg = msg;
                LOGGER.info("Incremental migration status: error, message: " + msg);
            }
            LOGGER.info("Count: " + count + ", sourceSpeed: " + sourceSpeed + ", sinkSpeed: " + sinkSpeed + ", rest: " + rest);
        }
    }

    /**
     * Gets portal status.
     *
     * @return the portal status
     */
    public static int getPortalStatus() {
        int status = 0;
        String str = Tools.readFile(new File(PortalControl.toolsConfigParametersTable.get(Status.PORTAL_PATH)));
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
    public static ArrayList<TableStatus> getDatacheckTableStatus(String successPath, ArrayList<TableStatus> tableStatusArrayList, String failPath) {
        String successStr = Tools.readFile(new File(successPath));
        if (!successStr.equals("")) {
            successStr = "[" + successStr.substring(0, successStr.length() - 1) + "]";
            JSONArray array = JSONArray.parseArray(successStr);
            Iterator iterator = array.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String tableName = array.getJSONObject(index).getString("tableName");
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
        String failStr = Tools.readFile(new File(failPath));
        if (!failStr.equals("")) {
            failStr = "[" + failStr.substring(0, failStr.length() - 1) + "]";
            JSONArray array = JSONArray.parseArray(failStr);
            Iterator iterator = array.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String tableName = array.getJSONObject(index).getString("tableName");
                for (TableStatus tableStatus : tableStatusArrayList) {
                    if (tableStatus.getName().equals(tableName)) {
                        tableStatus.setStatus(Status.Object.CHECK_FAILED);
                        String errorMsg = array.getJSONObject(index).getString("message");
                        errorMsg += "If you want to repair data.please read the following files:";
                        String repairFileName = "repair_" + PortalControl.toolsMigrationParametersTable.get(Mysql.DATABASE_NAME) + "_" + tableName + "_0_0.txt";
                        String repairPath = PathUtils.combainPath(true, PortalControl.toolsConfigParametersTable.get(Check.Result.FULL) + "result", repairFileName);
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
        File file = new File(path);
        String tableChameleonStatus;
        if (!(tableChameleonStatus = Tools.readFile(file)).equals("")) {
            JSONObject root = JSONObject.parseObject(tableChameleonStatus);
            return JSONObject.parseObject(root.getString("total"));
        }
        return "";
    }
}
