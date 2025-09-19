/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.helper.config;

import org.opengauss.constants.config.ChameleonConfig;
import org.opengauss.domain.dto.MysqlMigrationConfigDto;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.utils.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * chameleon mysql migration config helper
 *
 * @since 2025/5/7
 */
public class ChameleonMysqlMigrationConfigHelper {
    private ChameleonMysqlMigrationConfigHelper() {
    }

    /**
     * get mysql full migration config map
     *
     * @param dto mysql migration config dto
     * @param workspace task workspace
     * @return mysql full migration config
     */
    public static Map<String, Object> mysqlFullMigrationConfig(MysqlMigrationConfigDto dto, TaskWorkspace workspace) {
        HashMap<String, Object> changeParams = new HashMap<>();
        changeParams.put(ChameleonConfig.MYSQL_DATABASE_IP, dto.getMysqlDatabaseIp());
        changeParams.put(ChameleonConfig.MYSQL_DATABASE_PORT, dto.getMysqlDatabasePort());
        changeParams.put(ChameleonConfig.MYSQL_DATABASE_USER, dto.getMysqlDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(ChameleonConfig.MYSQL_DATABASE_PASSWORD, dto.getMysqlDatabasePassword());
        }
        String mysqlDbName = dto.getMysqlDatabaseName();
        changeParams.put(ChameleonConfig.MYSQL_DATABASE_NAME, mysqlDbName);

        String schemaMappingKey = String.format("%s.%s", ChameleonConfig.MYSQL_SCHEMA_MAPPINGS, mysqlDbName);
        String schemaMappingValue = mysqlDbName;
        if (!StringUtils.isNullOrBlank(dto.getOpengaussDatabaseSchema())) {
            schemaMappingValue = dto.getOpengaussDatabaseSchema();
        }
        changeParams.put(schemaMappingKey, schemaMappingValue);

        if (!StringUtils.isNullOrBlank(dto.getMysqlDatabaseTables())) {
            List<String> limitTables = Arrays.asList(dto.getMysqlDatabaseTables().split(","));
            changeParams.put(ChameleonConfig.MYSQL_LIMIT_TABLES, limitTables);
        }

        changeParams.put(ChameleonConfig.PG_DATABASE_IP, dto.getOpengaussDatabaseIp());
        changeParams.put(ChameleonConfig.PG_DATABASE_PORT, dto.getOpengaussDatabasePort());
        changeParams.put(ChameleonConfig.PG_DATABASE_USER, dto.getOpengaussDatabaseUsername());
        if (!dto.isUseInteractivePassword()) {
            changeParams.put(ChameleonConfig.PG_DATABASE_PASSWORD, dto.getOpengaussDatabasePassword());
        }
        changeParams.put(ChameleonConfig.PG_DATABASE_NAME, dto.getOpengaussDatabaseName());

        String csvDir = generateCsvDir(workspace);
        changeParams.put(ChameleonConfig.MYSQL_CSV_DIR, csvDir);
        changeParams.put(ChameleonConfig.MYSQL_OUT_DIR, csvDir);
        changeParams.put(ChameleonConfig.PID_DIR, generatePidDir(workspace));
        changeParams.put(ChameleonConfig.DUMP_JSON, "yes");
        return changeParams;
    }

    /**
     * get mysql pid dir
     *
     * @param taskWorkspace task workspace
     * @return mysql pid dir
     */
    public static String generatePidDir(TaskWorkspace taskWorkspace) {
        return String.format("%s/%s", taskWorkspace.getTmpDirPath(), "pid");
    }

    /**
     * get mysql csv dir
     *
     * @param taskWorkspace task workspace
     * @return mysql csv dir
     */
    public static String generateCsvDir(TaskWorkspace taskWorkspace) {
        return taskWorkspace.getTmpDirPath();
    }
}
