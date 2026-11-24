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
