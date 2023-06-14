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

package org.opengauss.portalcontroller.constant;

import java.util.Hashtable;

/**
 * The interface Status.
 *
 * @author ：liutong
 * @date ：Created in 2023/06/15
 * @since ：11
 */
public interface Status {
    /**
     * The constant START_FULL_MIGRATION.
     */
    int START_FULL_MIGRATION = 1;

    /**
     * The constant RUNNING_FULL_MIGRATION.
     */
    int RUNNING_FULL_MIGRATION = 2;

    /**
     * The constant FULL_MIGRATION_FINISHED.
     */
    int FULL_MIGRATION_FINISHED = 3;

    /**
     * The constant START_FULL_MIGRATION_CHECK.
     */
    int START_FULL_MIGRATION_CHECK = 4;

    /**
     * The constant RUNNING_FULL_MIGRATION_CHECK.
     */
    int RUNNING_FULL_MIGRATION_CHECK = 5;

    /**
     * The constant FULL_MIGRATION_CHECK_FINISHED.
     */
    int FULL_MIGRATION_CHECK_FINISHED = 6;

    /**
     * The constant START_INCREMENTAL_MIGRATION.
     */
    int START_INCREMENTAL_MIGRATION = 7;

    /**
     * The constant RUNNING_INCREMENTAL_MIGRATION.
     */
    int RUNNING_INCREMENTAL_MIGRATION = 8;

    /**
     * The constant INCREMENTAL_MIGRATION_FINISHED.
     */
    int INCREMENTAL_MIGRATION_FINISHED = 9;

    /**
     * The constant START_REVERSE_MIGRATION.
     */
    int START_REVERSE_MIGRATION = 10;

    /**
     * The constant RUNNING_REVERSE_MIGRATION.
     */
    int RUNNING_REVERSE_MIGRATION = 11;

    /**
     * The constant REVERSE_MIGRATION_FINISHED.
     */
    int REVERSE_MIGRATION_FINISHED = 12;

    /**
     * The constant ERROR.
     */
    int ERROR = 500;

    /**
     * The constant FOLDER.
     */
    String FOLDER = "status.folder";

    /**
     * The constant PORTAL_PATH.
     */
    String PORTAL_PATH = "status.portal.path";

    /**
     * The constant FULL_PATH.
     */
    String FULL_PATH = "status.full.path";

    /**
     * The constant FULL_CHECK_PATH.
     */
    String FULL_CHECK_PATH = "status.full.check.path";

    /**
     * The constant INCREMENTAL_FOLDER.
     */
    String INCREMENTAL_FOLDER = "status.incremental.folder";

    /**
     * The constant INCREMENTAL_PATH.
     */
    String INCREMENTAL_PATH = "status.incremental.path";

    /**
     * The constant REVERSE_FOLDER.
     */
    String REVERSE_FOLDER = "status.reverse.folder";

    /**
     * The constant REVERSE_PATH.
     */
    String REVERSE_PATH = "status.reverse.path";

    /**
     * The constant XLOG_PATH.
     */
    String XLOG_PATH = "status.xlog.path";

    /**
     * The constant HASHTABLE.
     */
    Hashtable<Integer, String> HASHTABLE = new Hashtable<>() {
        {
            put(START_FULL_MIGRATION, Information.START_FULL_MIGRATION);
            put(RUNNING_FULL_MIGRATION, Information.RUNNING_FULL_MIGRATION);
            put(FULL_MIGRATION_FINISHED, Information.FULL_MIGRATION_FINISHED);
            put(START_FULL_MIGRATION_CHECK, Information.START_FULL_MIGRATION_CHECK);
            put(RUNNING_FULL_MIGRATION_CHECK, Information.RUNNING_FULL_MIGRATION_CHECK);
            put(FULL_MIGRATION_CHECK_FINISHED, Information.FULL_MIGRATION_CHECK_FINISHED);
            put(START_INCREMENTAL_MIGRATION, Information.START_INCREMENTAL_MIGRATION);
            put(RUNNING_INCREMENTAL_MIGRATION, Information.RUNNING_INCREMENTAL_MIGRATION);
            put(INCREMENTAL_MIGRATION_FINISHED, Information.INCREMENTAL_MIGRATION_FINISHED);
            put(START_REVERSE_MIGRATION, Information.START_REVERSE_MIGRATION);
            put(RUNNING_REVERSE_MIGRATION, Information.RUNNING_REVERSE_MIGRATION);
            put(REVERSE_MIGRATION_FINISHED, Information.REVERSE_MIGRATION_FINISHED);
            put(ERROR, Object.Information.ERROR);
        }
    };

    /**
     * The interface Information.
     */
    interface Information {
        /**
         * The constant START_FULL_MIGRATION.
         */
        String START_FULL_MIGRATION = "start full migration";

        /**
         * The constant RUNNING_FULL_MIGRATION.
         */
        String RUNNING_FULL_MIGRATION = "full migration running";

        /**
         * The constant FULL_MIGRATION_FINISHED.
         */
        String FULL_MIGRATION_FINISHED = "full migration finished";

        /**
         * The constant START_FULL_MIGRATION_CHECK.
         */
        String START_FULL_MIGRATION_CHECK = "start full migration datacheck";

        /**
         * The constant RUNNING_FULL_MIGRATION_CHECK.
         */
        String RUNNING_FULL_MIGRATION_CHECK = "full migration datacheck running";

        /**
         * The constant FULL_MIGRATION_CHECK_FINISHED.
         */
        String FULL_MIGRATION_CHECK_FINISHED = "full migration datacheck finished";

        /**
         * The constant START_INCREMENTAL_MIGRATION.
         */
        String START_INCREMENTAL_MIGRATION = "start incremental migration";

        /**
         * The constant RUNNING_INCREMENTAL_MIGRATION.
         */
        String RUNNING_INCREMENTAL_MIGRATION = "incremental migration running";

        /**
         * The constant INCREMENTAL_MIGRATION_FINISHED.
         */
        String INCREMENTAL_MIGRATION_FINISHED = "incremental migration finished";

        /**
         * The constant START_REVERSE_MIGRATION.
         */
        String START_REVERSE_MIGRATION = "start reverse migration";

        /**
         * The constant RUNNING_REVERSE_MIGRATION.
         */
        String RUNNING_REVERSE_MIGRATION = "reverse migration running";

        /**
         * The constant REVERSE_MIGRATION_FINISHED.
         */
        String REVERSE_MIGRATION_FINISHED = "reverse migration finished";

        /**
         * The constant ERROR.
         */
        String ERROR = "error";
    }

    /**
     * The interface Object.
     */
    interface Object {
        /**
         * The constant START_FULL_MIGRATION.
         */
        int START_FULL_MIGRATION = 1;

        /**
         * The constant RUNNING_FULL_MIGRATION.
         */
        int RUNNING_FULL_MIGRATION = 2;

        /**
         * The constant FULL_MIGRATION_FINISHED.
         */
        int FULL_MIGRATION_FINISHED = 3;

        /**
         * The constant RUNNING_FULL_MIGRATION_CHECK.
         */
        int RUNNING_FULL_MIGRATION_CHECK = 4;

        /**
         * The constant FULL_MIGRATION_CHECK_FINISHED.
         */
        int FULL_MIGRATION_CHECK_FINISHED = 5;

        /**
         * The constant ERROR.
         */
        int ERROR = 6;

        /**
         * The constant CHECK_FAILED.
         */
        int CHECK_FAILED = 7;

        /**
         * The constant HASHTABLE.
         */
        Hashtable<Integer, String> HASHTABLE = new Hashtable<>() {
            {
                put(START_FULL_MIGRATION, Information.START_FULL_MIGRATION);
                put(RUNNING_FULL_MIGRATION, Information.RUNNING_FULL_MIGRATION);
                put(FULL_MIGRATION_FINISHED, Information.FULL_MIGRATION_FINISHED);
                put(RUNNING_FULL_MIGRATION_CHECK, Information.RUNNING_FULL_MIGRATION_CHECK);
                put(FULL_MIGRATION_CHECK_FINISHED, Information.FULL_MIGRATION_CHECK_FINISHED);
                put(ERROR, Information.ERROR);
            }
        };

        /**
         * The interface Information.
         */
        interface Information {
            /**
             * The constant START_FULL_MIGRATION.
             */
            String START_FULL_MIGRATION = "waiting";

            /**
             * The constant RUNNING_FULL_MIGRATION.
             */
            String RUNNING_FULL_MIGRATION = "running";

            /**
             * The constant FULL_MIGRATION_FINISHED.
             */
            String FULL_MIGRATION_FINISHED = "finished";

            /**
             * The constant RUNNING_FULL_MIGRATION_CHECK.
             */
            String RUNNING_FULL_MIGRATION_CHECK = "checking";

            /**
             * The constant FULL_MIGRATION_CHECK_FINISHED.
             */
            String FULL_MIGRATION_CHECK_FINISHED = "checked";

            /**
             * The constant ERROR.
             */
            String ERROR = "failed";
        }
    }

    /**
     * The interface Incremental.
     */
    interface Incremental {
        /**
         * The constant RUNNING.
         */
        int RUNNING = 1;

        /**
         * The constant ERROR.
         */
        int ERROR = 2;
    }
}
