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
 * The type Kafka.
 */
public class Kafka implements Software {
    public ArrayList<String> initCriticalFileList() {
        String kafkaPath = PortalControl.toolsConfigParametersTable.get(Debezium.Kafka.PATH);
        ArrayList<String> kafkaList = new ArrayList<>();
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "zookeeper-server-start.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "zookeeper-server-stop.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "config", "zookeeper.properties"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "kafka-server-start.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "bin", "kafka-server-stop.sh"));
        kafkaList.add(PathUtils.combainPath(true, kafkaPath + "config", "server.properties"));
        return kafkaList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Debezium.Kafka.PATH);
        hashtable.put(Parameter.INSTALL_PATH, Debezium.PATH);
        hashtable.put(Parameter.PKG_PATH, Debezium.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Debezium.Kafka.PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Debezium.Kafka.PKG_NAME);
        return hashtable;
    }
}
