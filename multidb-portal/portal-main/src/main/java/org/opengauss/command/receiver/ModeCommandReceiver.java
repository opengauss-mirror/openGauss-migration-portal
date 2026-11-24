/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.printer.TablePrinter;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.domain.model.MigrationMode;
import org.opengauss.migration.mode.ModeManager;

import java.util.ArrayList;
import java.util.List;

/**
 * mode command receiver
 *
 * @since 2025/3/29
 */
public class ModeCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(ModeCommandReceiver.class);

    private final ModeManager modeManager;

    public ModeCommandReceiver() {
        modeManager = new ModeManager();
    }

    /**
     * list all migration modes
     */
    public void list() {
        List<MigrationMode> modeList = modeManager.list();
        MigrationPhase[] allPhases = MigrationPhase.values();

        List<String> header = new ArrayList<>();
        header.add("Mode Name");
        for (MigrationPhase phase : allPhases) {
            header.add(phase.getPhaseName());
        }

        List<List<String>> tableInfoList = new ArrayList<>();
        for (MigrationMode mode : modeList) {
            List<String> row = new ArrayList<>();
            row.add(mode.getModeName());
            for (MigrationPhase phase : allPhases) {
                if (mode.hasPhase(phase)) {
                    row.add("Y");
                } else {
                    row.add("");
                }
            }
            tableInfoList.add(row);
        }

        String table = TablePrinter.printTable(header, tableInfoList);
        LOGGER.info("Migration Modes:{}{}", System.lineSeparator(), table);
    }

    /**
     * add a migration mode
     *
     * @param modeFilePath mode file path
     */
    public void add(String modeFilePath) {
        modeManager.add(modeFilePath);
    }

    /**
     * update a migration mode
     *
     * @param modeFilePath mode file path
     */
    public void update(String modeFilePath) {
        modeManager.update(modeFilePath);
    }

    /**
     * delete a migration mode
     *
     * @param modeName mode name
     */
    public void delete(String modeName) {
        modeManager.delete(modeName);
    }

    /**
     * get a migration mode define template file
     */
    public void template() {
        modeManager.template();
    }
}
