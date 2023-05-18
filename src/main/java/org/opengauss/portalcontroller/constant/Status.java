package org.opengauss.portalcontroller.constant;

import java.util.Hashtable;

public interface Status {
    int START_FULL_MIGRATION = 1;
    int RUNNING_FULL_MIGRATION = 2;
    int FULL_MIGRATION_FINISHED = 3;
    int START_FULL_MIGRATION_CHECK = 4;
    int RUNNING_FULL_MIGRATION_CHECK = 5;
    int FULL_MIGRATION_CHECK_FINISHED = 6;
    int START_INCREMENTAL_MIGRATION = 7;
    int RUNNING_INCREMENTAL_MIGRATION = 8;
    int INCREMENTAL_MIGRATION_FINISHED = 9;
    int START_REVERSE_MIGRATION = 10;
    int RUNNING_REVERSE_MIGRATION = 11;
    int REVERSE_MIGRATION_FINISHED = 12;
    int ERROR = 500;

    interface Information {
        String START_FULL_MIGRATION = "start full migration";
        String RUNNING_FULL_MIGRATION = "full migration running";
        String FULL_MIGRATION_FINISHED = "full migration finished";
        String START_FULL_MIGRATION_CHECK = "start full migration datacheck";
        String RUNNING_FULL_MIGRATION_CHECK = "full migration datacheck running";
        String FULL_MIGRATION_CHECK_FINISHED = "full migration datacheck finished";
        String START_INCREMENTAL_MIGRATION = "start incremental migration";
        String RUNNING_INCREMENTAL_MIGRATION = "incremental migration running";
        String INCREMENTAL_MIGRATION_FINISHED = "incremental migration finished";
        String START_REVERSE_MIGRATION = "start reverse migration";
        String RUNNING_REVERSE_MIGRATION = "reverse migration running";
        String REVERSE_MIGRATION_FINISHED = "reverse migration finished";
        String ERROR = "error";
    }

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

    interface Object {
        int START_FULL_MIGRATION = 1;
        int RUNNING_FULL_MIGRATION = 2;
        int FULL_MIGRATION_FINISHED = 3;
        int RUNNING_FULL_MIGRATION_CHECK = 4;
        int FULL_MIGRATION_CHECK_FINISHED = 5;
        int ERROR = 6;
        int CHECK_FAILED = 7;

        interface Information {
            String START_FULL_MIGRATION = "waiting";
            String RUNNING_FULL_MIGRATION = "running";
            String FULL_MIGRATION_FINISHED = "finished";
            String RUNNING_FULL_MIGRATION_CHECK = "checking";
            String FULL_MIGRATION_CHECK_FINISHED = "checked";
            String ERROR = "failed";
        }

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
    }

    interface Incremental {
        int RUNNING = 1;
        int ERROR = 2;
    }

    String FOLDER = "status.folder";
    String PORTAL_PATH =  "status.portal.path";
    String FULL_PATH = "status.full.path";
    String FULL_CHECK_PATH = "status.full.check.path";
    String INCREMENTAL_FOLDER = "status.incremental.folder";
    String INCREMENTAL_PATH = "status.incremental.path";
    String REVERSE_FOLDER = "status.reverse.folder";
    String REVERSE_PATH = "status.reverse.path";
    String XLOG_PATH = "status.xlog.path";
}
