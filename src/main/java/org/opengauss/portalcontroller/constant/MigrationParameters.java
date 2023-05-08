package org.opengauss.portalcontroller.constant;

public interface MigrationParameters {
    String SNAPSHOT_OBJECT = "snapshot.object";
    interface Install {
        String FULL_MIGRATION = "default.install.mysql.full.migration.tools.way";
        String INCREMENTAL_MIGRATION = "default.install.mysql.incremental.migration.tools.way";
        String DATACHECK = "default.install.mysql.datacheck.tools.way";
        String REVERSE_MIGRATION = "default.install.mysql.reverse.migration.tools.way";
    }
    interface Type{
        String FULL = "full";
        String INCREMENTAL = "incremental";
        String REVERSE = "reverse";
        String CHECK = "datacheck";
    }
}
