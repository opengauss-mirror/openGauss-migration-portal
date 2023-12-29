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

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Debezium;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.utils.PathUtils;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Connector opengauss.
 */
public class ConnectorOpengauss implements Software {
    public ArrayList<String> initCriticalFileList() {
        String connectorPath = PortalControl.toolsConfigParametersTable.get(Debezium.Connector.PATH);
        ArrayList<String> connectorOpengaussList = new ArrayList<>();
        String jarName = Debezium.Connector.OPENGAUSS_JAR_NAME;
        connectorOpengaussList.add(PathUtils.combainPath(true, connectorPath + "debezium-connector-opengauss",
                jarName));
        return connectorOpengaussList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Debezium.Connector.OPENGAUSS_PATH);
        hashtable.put(Parameter.INSTALL_PATH, Debezium.Connector.PATH);
        hashtable.put(Parameter.PKG_PATH, Debezium.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Debezium.Connector.OPENGAUSS_PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Debezium.Connector.OPENGAUSS_PKG_NAME);
        hashtable.put(Parameter.PKG_UNZIP_SPACE, Debezium.Connector.PKG_UNZIP_SPACE);
        return hashtable;
    }
}
