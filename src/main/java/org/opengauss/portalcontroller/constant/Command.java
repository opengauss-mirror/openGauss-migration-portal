package org.opengauss.portalcontroller.constant;

public interface Command {
    interface Install{
        interface Mysql{
            interface FullMigration{
                String ONLINE = "install mysql full migration tools online";
                String OFFLINE = "install mysql full migration tools offline";
                String DEFAULT = "install mysql full migration tools";
            }
            interface IncrementalMigration{
                String ONLINE = "install mysql incremental migration tools online";
                String OFFLINE = "install mysql incremental migration tools offline";
                String DEFAULT = "install mysql incremental migration tools";
            }
            interface Check{
                String ONLINE = "install mysql datacheck tools online";
                String OFFLINE = "install mysql datacheck tools offline";
                String DEFAULT = "install mysql datacheck tools";
            }

            interface ReverseMigration{
                String ONLINE = "install mysql reverse migration tools online";
                String OFFLINE = "install mysql reverse migration tools offline";
                String DEFAULT = "install mysql reverse migration tools";
            }
            interface All{
                String ONLINE = "install mysql all migration tools online";
                String DEFAULT = "install mysql all migration tools";
                String OFFLINE = "install mysql all migration tools offline";
            }
        }
    }
    interface Uninstall{
        interface Mysql{
            String FULL = "uninstall mysql full migration tools";
            String INCREMENTAL = "uninstall mysql incremental migration tools";
            String CHECK = "uninstall mysql datacheck tools";
            String REVERSE = "uninstall mysql reverse migration tools";
            String ALL = "uninstall mysql all migration tools";
        }
    }
    interface Start{
        interface Mysql{
            String FULL = "start mysql full migration";
            String INCREMENTAL = "start mysql incremental migration";
            String REVERSE = "start mysql reverse migration";
            String FULL_CHECK = "start mysql full migration datacheck";
            String INCREMENTAL_CHECK = "start mysql incremental migration datacheck";
            String REVERSE_CHECK = "start mysql reverse migration datacheck";
        }
        interface Plan{
            String PLAN1 = "start plan1";
            String PLAN2 = "start plan2";
            String PLAN3 = "start plan3";
            String CURRENT = "start current plan";
        }
    }
    interface Show{
        String PLAN = "show plans";
        String STATUS = "show status";
        String INFORMATION = "show information";
        String PARAMETERS = "show parameters";
    }
    interface Stop{
        String PLAN = "stop plan";
        String INCREMENTAL_MIGRATION = "stop incremental migration";
        String REVERSE_MIGRATION = "stop reverse migration";
    }
    interface Parameters{
        String ID = "workspace.id";
        String PATH = "path";
        String CHECK = "check";
        String ORDER = "order";
    }
    String HELP = "help";
    interface Run{
        String INCREMENTAL_MIGRATION = "run incremental migration";
        String REVERSE_MIGRATION = "run reverse migration";
    }
    interface Type{
        String INSTALL = "install";
        String RUN = "run";
        String START = "start";
        String UNINSTALL = "uninstall";
        String STOP = "stop";
        String SHOW = "show";
        String HELP = "help";
    }
    String MYSQL = "mysql";
    String ALL = "all";
}
