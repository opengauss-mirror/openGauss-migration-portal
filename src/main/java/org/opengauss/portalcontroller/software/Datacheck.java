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
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.constant.Parameter;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The type Datacheck.
 */
public class Datacheck implements Software {
    public ArrayList<String> initCriticalFileList() {
        ArrayList<String> datacheckList = new ArrayList<>();
        String datacheckPath = PortalControl.toolsConfigParametersTable.get(Check.PATH);
        String datacheckExtractName = PortalControl.toolsConfigParametersTable.get(Check.EXTRACT_NAME);
        String datacheckCheckName = PortalControl.toolsConfigParametersTable.get(Check.CHECK_NAME);
        datacheckList.add(datacheckPath + datacheckExtractName);
        datacheckList.add(datacheckPath + datacheckCheckName);
        return datacheckList;
    }

    public Hashtable<String, String> initParameterHashtable() {
        Hashtable<String, String> hashtable = new Hashtable<>();
        hashtable.put(Parameter.PATH, Check.PATH);
        hashtable.put(Parameter.INSTALL_PATH, Check.INSTALL_PATH);
        hashtable.put(Parameter.PKG_PATH, Check.PKG_PATH);
        hashtable.put(Parameter.PKG_URL, Check.PKG_URL);
        hashtable.put(Parameter.PKG_NAME, Check.PKG_NAME);
        hashtable.put(Parameter.PKG_UNZIP_SPACE, Check.PKG_UNZIP_SPACE);
        return hashtable;
    }
}
