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

package org.opengauss.portalcontroller.software;

import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Parameter;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Confluent.
 */
public class Confluent implements Software {
    public ArrayList<String> initCriticalFileList() {
        ArrayList<String> confluentList = new ArrayList<>();
        String confluentPath = PortalControl.toolsConfigParametersTable.get(Debezium.Confluent.PATH);
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "schema-registry-start"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "schema-registry-stop"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "etc", "schema-registry", "schema-registry.properties"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "connect-standalone"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "zookeeper-server-start"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "zookeeper-server-stop"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "etc", "kafka", "zookeeper.properties"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "kafka-server-start"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "bin", "kafka-server-stop"));
        confluentList.add(PathUtils.combainPath(true, confluentPath + "etc", "kafka", "server.properties"));
        return confluentList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Debezium.Confluent.PATH);
        hashtable.put(Parameter.INSTALL_PATH, Debezium.PATH);
        hashtable.put(Parameter.PKG_PATH, Debezium.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Debezium.Confluent.PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Debezium.Confluent.PKG_NAME);
        return hashtable;
    }
}
