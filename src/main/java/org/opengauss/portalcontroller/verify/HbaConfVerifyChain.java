/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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
import org.opengauss.portalcontroller.utils.JdbcUtils;
import org.opengauss.portalcontroller.utils.MigrationParamUtils;
import org.opengauss.portalcontroller.verify.model.HbaConfModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * pg_hba.conf verify
 *
 * @since 2024-10-11
 */
public class HbaConfVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaConfVerifyChain.class);
    private static final Pattern GS_VERSION_PATTERN = Pattern.compile("\\(openGauss\\s(\\d+\\.\\d+\\.\\d+[^\\s]*)");

    private final HashMap<Object, Object> paramMap = new HashMap<>();
    private final StringBuilder errorMessage = new StringBuilder();

    private boolean isValid = false;

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        String verifyParamKey = "hba_conf";

        if (pgConnection == null) {
            resultMap.put(verifyParamKey, paramMap);
            paramMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        } else if (isDbVersionSupport(pgConnection)) {
            resultMap.put(verifyParamKey, paramMap);
            doVerify(pgConnection);
        } else {
            isValid = true;
            LOGGER.info("Skip hba conf verify.");
        }

        resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString())
                        | (isValid ? Constants.KEY_FLAG_TRUE : Constants.KEY_FLAG_FALSE));
        super.transfer(resultMap, mysqlConnection, pgConnection);
    }

    private boolean isDbVersionSupport(PgConnection pgConnection) {
        try {
            String selectSql = "select version();";
            String sqlResult = JdbcUtils.selectStringValue(pgConnection, selectSql, "version");

            String version = extractVersion(sqlResult);
            LOGGER.info("openGauss version is {}.", version);

            String signVersion = "6.0.0";
            String exceptVersion = "6.0.0-RC1";
            if (!version.equals(exceptVersion) && version.compareTo(signVersion) >= 0) {
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to query openGauss version. Error: ", e);
        }
        return false;
    }

    private void doVerify(PgConnection pgConnection) {
        List<HbaConfModel> hbaConfModels = getHbaConfModels(pgConnection);

        List<HbaConfModel> replicationHbaConfModels = hbaConfModels.stream()
                .filter(HbaConfModel::hasReplicationPrivileges).collect(Collectors.toList());
        isValid = replicationHbaConfModels.stream().anyMatch(HbaConfModel::checkHbaConf);
        if (isValid) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_TRUE);
            return;
        }

        paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_FALSE);
        if (!replicationHbaConfModels.isEmpty()) {
            int detailCode = replicationHbaConfModels.get(0).getCheckStatus().getCode();
            paramMap.put("detail_code", detailCode);

            if (detailCode == HbaConfModel.CheckStatus.ADDRESS_NOT_SUPPORT.getCode()) {
                paramMap.put("kafka_ip", MigrationParamUtils.getKafkaIp());
            }
        } else if (errorMessage.length() > 0) {
            paramMap.put(Constants.KEY_RESULT, Constants.KEY_FLAG_ERROR);
            paramMap.put("error_message", errorMessage.toString());
        } else {
            paramMap.put("detail_code", HbaConfModel.CheckStatus.NO_PRIVILEGES.getCode());
        }
    }

    private String extractVersion(String input) {
        String version = "";
        Matcher matcher = GS_VERSION_PATTERN.matcher(input);

        if (matcher.find()) {
            version = matcher.group(1);
        }
        return version;
    }

    private List<HbaConfModel> getHbaConfModels(PgConnection pgConnection) {
        List<HbaConfModel> result = new ArrayList<>();
        String selectSql = "select * from gs_get_hba_conf();";
        try (Statement statement = pgConnection.createStatement();
             ResultSet resultSet = statement.executeQuery(selectSql)) {
            while (resultSet.next()) {
                HbaConfModel hbaConfModel = new HbaConfModel();
                hbaConfModel.setType(resultSet.getString("type"));
                hbaConfModel.setDatabase(resultSet.getString("database"));
                hbaConfModel.setUser(resultSet.getString("user"));
                hbaConfModel.setAddress(resultSet.getString("address"));
                hbaConfModel.setMethod(resultSet.getString("method"));
                result.add(hbaConfModel);
            }
        } catch (SQLException e) {
            errorMessage.append(e.getMessage());
            LOGGER.error("Failed to get hba config. Error: ", e);
        }
        return result;
    }
}
