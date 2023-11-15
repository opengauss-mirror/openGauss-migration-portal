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

package org.opengauss.portalcontroller.verify;

import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.Tools;

import java.sql.Connection;
import java.util.Map;

/**
 * CommonServiceVerifyChain
 *
 * @since 1.1
 * @date :2023/11/3 15:22
 * @description: CommonServiceVerifyChain
 * @version: 1.1
 */
public class CommonServiceVerifyChain extends AbstractPreMigrationVerifyChain {
    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        // kafka availability
        boolean isUsed = Tools.checkKafkaProcess();
        resultMap.put("service_availability", isUsed ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE);
        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG, isUsed ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE);
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }
}