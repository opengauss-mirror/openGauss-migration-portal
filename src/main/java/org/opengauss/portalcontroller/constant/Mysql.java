package org.opengauss.portalcontroller.constant;

public interface Mysql {
    String USER = "mysql.user.name";
    String PASSWORD = "mysql.user.password";
    String DATABASE_HOST = "mysql.database.host";
    String DATABASE_PORT = "mysql.database.port";
    String DATABASE_NAME = "mysql.database.name";
    String DATABASE_TABLE= "mysql.database.table";

    interface Default {
        String USER = "";
        String PASSWORD = "";
        String DATABASE_HOST = "127.0.0.1";
        String DATABASE_PORT = "3306";
        String DATABASE_NAME = "";
    }

    interface Incremental {
        String CONNECTOR_PATH = "mysql.incremental.connector.path";
    }

    interface Reverse {
        String CONNECTOR_PATH = "mysql.reverse.connector.path";
    }
}
