package org.opengauss.portalcontroller.constant;

import java.util.List;

public interface MigrationParameters {
    String SNAPSHOT_OBJECT = "snapshot.object";

    interface Install {
        String FULL_MIGRATION = "default.install.mysql.full.migration.tools.way";
        String INCREMENTAL_MIGRATION = "default.install.mysql.incremental.migration.tools.way";
        String DATACHECK = "default.install.mysql.datacheck.tools.way";
        String REVERSE_MIGRATION = "default.install.mysql.reverse.migration.tools.way";
    }

    interface Type {
        String FULL = "full";
        String INCREMENTAL = "incremental";
        String REVERSE = "reverse";
        String CHECK = "datacheck";
        List<String> ALL = List.of(FULL, INCREMENTAL, REVERSE, CHECK);
    }

    /**
     * log constant
     *
     * @author: www
     * @date: 2023/11/28 12:03
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    interface Log {
        /**
         * GLOBAL_LOG_LEVEL
         */
        String GLOBAL_LOG_LEVEL = "global.log.level";
    }
}
