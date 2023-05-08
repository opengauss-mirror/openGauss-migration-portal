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

import java.util.ArrayList;

/**
 * The type Full migration status.
 */
public class FullMigrationStatus {
    private ArrayList<TableStatus> table = new ArrayList<>();
    private ArrayList<ObjectStatus> view = new ArrayList<>();
    private ArrayList<ObjectStatus> function = new ArrayList<>();
    private ArrayList<ObjectStatus> trigger = new ArrayList<>();
    private ArrayList<ObjectStatus> procedure = new ArrayList<>();

    /**
     * Gets table.
     *
     * @return the table
     */
    public ArrayList<TableStatus> getTable() {
        return table;
    }

    /**
     * Sets table.
     *
     * @param table the table
     */
    public void setTable(ArrayList<TableStatus> table) {
        this.table = table;
    }

    /**
     * Gets view.
     *
     * @return the view
     */
    public ArrayList<ObjectStatus> getView() {
        return view;
    }

    /**
     * Sets view.
     *
     * @param view the view
     */
    public void setView(ArrayList<ObjectStatus> view) {
        this.view = view;
    }

    /**
     * Gets function.
     *
     * @return the function
     */
    public ArrayList<ObjectStatus> getFunction() {
        return function;
    }

    /**
     * Sets function.
     *
     * @param function the function
     */
    public void setFunction(ArrayList<ObjectStatus> function) {
        this.function = function;
    }

    /**
     * Gets trigger.
     *
     * @return the trigger
     */
    public ArrayList<ObjectStatus> getTrigger() {
        return trigger;
    }

    /**
     * Sets trigger.
     *
     * @param trigger the trigger
     */
    public void setTrigger(ArrayList<ObjectStatus> trigger) {
        this.trigger = trigger;
    }

    /**
     * Gets procedure.
     *
     * @return the procedure
     */
    public ArrayList<ObjectStatus> getProcedure() {
        return procedure;
    }

    /**
     * Sets procedure.
     *
     * @param procedure the procedure
     */
    public void setProcedure(ArrayList<ObjectStatus> procedure) {
        this.procedure = procedure;
    }

    /**
     * Instantiates a new Full migration status.
     */
    public FullMigrationStatus() {
    }

    /**
     * Instantiates a new Full migration status.
     *
     * @param table     the table
     * @param view      the view
     * @param function  the function
     * @param trigger   the trigger
     * @param procedure the procedure
     */
    public FullMigrationStatus(ArrayList<TableStatus> table, ArrayList<ObjectStatus> view, ArrayList<ObjectStatus> function, ArrayList<ObjectStatus> trigger, ArrayList<ObjectStatus> procedure) {
        this.table = table;
        this.view = view;
        this.function = function;
        this.trigger = trigger;
        this.procedure = procedure;
    }
}
