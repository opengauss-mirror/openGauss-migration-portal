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

/**
 * Command constants
 *
 * @author ：liutong
 * @date ：Created in 2023/06/03
 * @since ：11
 */
public interface Command {
    /**
     * The constant HELP.
     */
    String HELP = "help";

    /**
     * The constant PREPARE.
     */
    String PREPARE = "prepare";

    /**
     * The constant MYSQL.
     */
    String MYSQL = "mysql";

    /**
     * The constant MYSQL.
     */
    String FULL = "full";

    /**
     * The constant ALL.
     */
    String ALL = "all";

    /**
     * The interface Install.
     */
    interface Install {
        /**
         * The interface Mysql.
         */
        interface Mysql {
            /**
             * The interface Full migration.
             */
            interface FullMigration {
                /**
                 * The constant ONLINE.
                 */
                String ONLINE = "install mysql full migration tools online";

                /**
                 * The constant OFFLINE.
                 */
                String OFFLINE = "install mysql full migration tools offline";

                /**
                 * The constant DEFAULT.
                 */
                String DEFAULT = "install mysql full migration tools";
            }

            /**
             * The interface Incremental migration.
             */
            interface IncrementalMigration {
                /**
                 * The constant ONLINE.
                 */
                String ONLINE = "install mysql incremental migration tools online";

                /**
                 * The constant OFFLINE.
                 */
                String OFFLINE = "install mysql incremental migration tools offline";

                /**
                 * The constant DEFAULT.
                 */
                String DEFAULT = "install mysql incremental migration tools";
            }

            /**
             * The interface Check.
             */
            interface Check {
                /**
                 * The constant ONLINE.
                 */
                String ONLINE = "install mysql datacheck tools online";

                /**
                 * The constant OFFLINE.
                 */
                String OFFLINE = "install mysql datacheck tools offline";

                /**
                 * The constant DEFAULT.
                 */
                String DEFAULT = "install mysql datacheck tools";
            }

            /**
             * The interface Reverse migration.
             */
            interface ReverseMigration {
                /**
                 * The constant ONLINE.
                 */
                String ONLINE = "install mysql reverse migration tools online";

                /**
                 * The constant OFFLINE.
                 */
                String OFFLINE = "install mysql reverse migration tools offline";

                /**
                 * The constant DEFAULT.
                 */
                String DEFAULT = "install mysql reverse migration tools";
            }

            /**
             * The interface All.
             */
            interface All {
                /**
                 * The constant ONLINE.
                 */
                String ONLINE = "install mysql all migration tools online";

                /**
                 * The constant DEFAULT.
                 */
                String DEFAULT = "install mysql all migration tools";

                /**
                 * The constant OFFLINE.
                 */
                String OFFLINE = "install mysql all migration tools offline";
            }
        }
    }

    /**
     * The interface Uninstall.
     */
    interface Uninstall {
        /**
         * The interface Mysql.
         */
        interface Mysql {
            /**
             * The constant FULL.
             */
            String FULL = "uninstall mysql full migration tools";

            /**
             * The constant INCREMENTAL.
             */
            String INCREMENTAL = "uninstall mysql incremental migration tools";

            /**
             * The constant CHECK.
             */
            String CHECK = "uninstall mysql datacheck tools";

            /**
             * The constant REVERSE.
             */
            String REVERSE = "uninstall mysql reverse migration tools";

            /**
             * The constant ALL.
             */
            String ALL = "uninstall mysql all migration tools";
        }
    }

    /**
     * The interface Start.
     */
    interface Start {
        /**
         * The constant KAFKA.
         */
        String KAFKA = "start kafka";

        /**
         * The interface Mysql.
         */
        interface Mysql {
            /**
             * The constant FULL.
             */
            String FULL = "start mysql full migration";

            /**
             * The constant INCREMENTAL.
             */
            String INCREMENTAL = "start mysql incremental migration";

            /**
             * The constant REVERSE.
             */
            String REVERSE = "start mysql reverse migration";

            /**
             * The constant FULL_CHECK.
             */
            String FULL_CHECK = "start mysql full migration datacheck";

            /**
             * The constant INCREMENTAL_CHECK.
             */
            String INCREMENTAL_CHECK = "start mysql incremental migration datacheck";

            /**
             * The constant REVERSE_CHECK.
             */
            String REVERSE_CHECK = "start mysql reverse migration datacheck";
        }

        /**
         * The interface Plan.
         */
        interface Plan {
            /**
             * The constant PLAN1.
             */
            String PLAN1 = "start plan1";

            /**
             * The constant PLAN2.
             */
            String PLAN2 = "start plan2";

            /**
             * The constant PLAN3.
             */
            String PLAN3 = "start plan3";

            /**
             * The constant CURRENT.
             */
            String CURRENT = "start current plan";
        }
    }

    /**
     * The interface Show.
     */
    interface Show {
        /**
         * The constant PLAN.
         */
        String PLAN = "show plans";

        /**
         * The constant STATUS.
         */
        String STATUS = "show status";

        /**
         * The constant INFORMATION.
         */
        String INFORMATION = "show information";

        /**
         * The constant PARAMETERS.
         */
        String PARAMETERS = "show parameters";
    }

    /**
     * The interface Stop.
     */
    interface Stop {
        /**
         * The constant PLAN.
         */
        String PLAN = "stop plan";

        /**
         * The constant INCREMENTAL_MIGRATION.
         */
        String INCREMENTAL_MIGRATION = "stop incremental migration";

        /**
         * The constant REVERSE_MIGRATION.
         */
        String REVERSE_MIGRATION = "stop reverse migration";

        /**
         * The constant KAFKA.
         */
        String KAFKA = "stop kafka";
    }

    /**
     * The interface Parameters.
     */
    interface Parameters {
        /**
         * The constant ID.
         */
        String ID = "workspace.id";

        /**
         * The constant PATH.
         */
        String PATH = "path";

        /**
         * The constant CHECK.
         */
        String CHECK = "check";

        /**
         * The constant ORDER.
         */
        String ORDER = "order";
    }

    /**
     * The interface Run.
     */
    interface Run {
        /**
         * The constant INCREMENTAL_MIGRATION.
         */
        String INCREMENTAL_MIGRATION = "run incremental migration";

        /**
         * The constant REVERSE_MIGRATION.
         */
        String REVERSE_MIGRATION = "run reverse migration";
    }

    /**
     * The interface Type.
     */
    interface Type {
        /**
         * The constant INSTALL.
         */
        String INSTALL = "install";

        /**
         * The constant RUN.
         */
        String RUN = "run";

        /**
         * The constant START.
         */
        String START = "start";

        /**
         * The constant UNINSTALL.
         */
        String UNINSTALL = "uninstall";

        /**
         * The constant STOP.
         */
        String STOP = "stop";

        /**
         * The constant SHOW.
         */
        String SHOW = "show";

        /**
         * The constant HELP.
         */
        String HELP = "help";

        /**
         * The constant status.
         */
        String STATUS = "status";

        /**
         * The constant load config.
         */
        String LOAD = "load";

        /**
         * The constant verify.
         */
        String VERIFY = "verify";
    }

    /**
     * check portal status command
     */
    interface CheckPortalStatus {
        /**
         * check portal status
         */
        String CHECK_POTAL_STATUS = "check portal status";
    }

    /**
     * LoadToolsConfig
     *
     * @author: www
     * @date: 2023/11/28 12:08
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    interface LoadToolsConfig {
        /**
         * load tools config
         */
        String LOAD_TOOLS_CONFIG = "load tools config";
    }

    /**
     * check before migration
     */
    interface Verify {
        /**
         * check all
         */
        String VERIFY_PRE_MIGRATION = "verify pre migration";

        /**
         * check reverse
         */
        String VERIFY_REVERSE_MIGRATION = "verify reverse migration";
    }
}
