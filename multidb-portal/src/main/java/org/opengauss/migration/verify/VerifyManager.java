/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.domain.dto.MysqlMigrationConfigDto;
import org.opengauss.domain.dto.PgsqlMigrationConfigDto;
import org.opengauss.domain.model.OpenGaussDatabaseConnectInfo;
import org.opengauss.domain.model.TaskWorkspace;
import org.opengauss.enums.DatabaseType;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.exceptions.VerifyException;
import org.opengauss.migration.verify.constants.VerifyConstants;
import org.opengauss.migration.verify.model.VerifyDto;
import org.opengauss.migration.verify.model.VerifyResult;
import org.opengauss.utils.FileUtils;

import java.io.IOException;
import java.util.List;

/**
 * Verify manager
 *
 * @since 2025/5/9
 */
public class VerifyManager {
    private static final Logger LOGGER = LogManager.getLogger(VerifyManager.class);

    /**
     * Verify before start MySQL migration
     *
     * @param migrationPhaseList migration phase list
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean mysqlMigrationVerify(
            List<MigrationPhase> migrationPhaseList, MysqlMigrationConfigDto migrationConfigDto,
            TaskWorkspace taskWorkspace) {
        VerifyResult mysqlVerifyResult = new VerifyResult();
        VerifyDto verifyDto = genrateMysqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getMysqlMigrationVerifyChain(migrationPhaseList).verify(verifyDto, mysqlVerifyResult);
        verifyDto.closeConnection();
        outputVerifyResult(mysqlVerifyResult, taskWorkspace);
        return mysqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start MySQL reverse phase
     *
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean mysqlReversePhaseVerify(
            MysqlMigrationConfigDto migrationConfigDto, TaskWorkspace taskWorkspace) {
        VerifyResult mysqlVerifyResult = new VerifyResult();
        VerifyDto verifyDto = genrateMysqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getMysqlReversePhaseVerifyChain().verify(verifyDto, mysqlVerifyResult);
        verifyDto.closeConnection();
        outputVerifyResult(mysqlVerifyResult, taskWorkspace);
        return mysqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start PostgreSQL migration
     *
     * @param migrationPhaseList migration phase list
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean pgsqlMigrationVerify(
            List<MigrationPhase> migrationPhaseList, PgsqlMigrationConfigDto migrationConfigDto,
            TaskWorkspace taskWorkspace) {
        VerifyResult pgsqlVerifyResult = new VerifyResult();
        VerifyDto verifyDto = genratePgsqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getPgsqlMigrationVerifyChain(migrationPhaseList).verify(verifyDto, pgsqlVerifyResult);
        verifyDto.closeConnection();
        outputVerifyResult(pgsqlVerifyResult, taskWorkspace);
        return pgsqlVerifyResult.isSuccess();
    }

    /**
     * Verify before start PostgreSQL reverse phase
     *
     * @param migrationConfigDto migration config dto
     * @param taskWorkspace task workspace
     * @return true if verify is successful, false otherwise
     */
    public static boolean pgsqlReversePhaseVerify(
            PgsqlMigrationConfigDto migrationConfigDto, TaskWorkspace taskWorkspace) {
        VerifyResult pgsqlVerifyResult = new VerifyResult();
        VerifyDto verifyDto = genratePgsqlVerifyDto(migrationConfigDto);

        VerifyChainBuilder.getPgsqlReversePhaseVerifyChain().verify(verifyDto, pgsqlVerifyResult);
        verifyDto.closeConnection();
        outputVerifyResult(pgsqlVerifyResult, taskWorkspace);
        return pgsqlVerifyResult.isSuccess();
    }

    private static void outputVerifyResult(VerifyResult verifyResult, TaskWorkspace taskWorkspace) {
        String resultFilePath = String.format("%s/%s", taskWorkspace.getStatusDirPath(),
                VerifyConstants.VERIFY_RESULT_FILE_NAME);
        String result = verifyResult.getResult();

        try {
            if (verifyResult.isSuccess()) {
                LOGGER.info("Verify before migration is successful");
            } else {
                LOGGER.error("Verify before migration is failed, following is the detail: {}{}",
                        System.lineSeparator(), result);
            }
            FileUtils.writeToFile(resultFilePath, result, false);
        } catch (IOException e) {
            throw new VerifyException("Failed to write verify result to file: " + resultFilePath, e);
        }
    }

    private static VerifyDto genrateMysqlVerifyDto(MysqlMigrationConfigDto migrationConfigDto) {
        VerifyDto verifyDto = new VerifyDto();
        verifyDto.setSourceDbType(DatabaseType.MYSQL);
        verifyDto.setSourceIp(migrationConfigDto.getMysqlDatabaseIp());
        verifyDto.setSourcePort(migrationConfigDto.getMysqlDatabasePort());
        verifyDto.setSourceUsername(migrationConfigDto.getMysqlDatabaseUsername());
        verifyDto.setSourcePassword(migrationConfigDto.getMysqlDatabasePassword());
        verifyDto.setSourceDatabase(migrationConfigDto.getMysqlDatabaseName());

        setVerifyDtoOpenGaussParams(migrationConfigDto.getOpenGaussConnectInfo(), verifyDto);
        return verifyDto;
    }

    private static VerifyDto genratePgsqlVerifyDto(PgsqlMigrationConfigDto migrationConfigDto) {
        VerifyDto verifyDto = new VerifyDto();
        verifyDto.setSourceDbType(DatabaseType.POSTGRESQL);
        verifyDto.setSourceIp(migrationConfigDto.getPgsqlDatabaseIp());
        verifyDto.setSourcePort(migrationConfigDto.getPgsqlDatabasePort());
        verifyDto.setSourceUsername(migrationConfigDto.getPgsqlDatabaseUsername());
        verifyDto.setSourcePassword(migrationConfigDto.getPgsqlDatabasePassword());
        verifyDto.setSourceDatabase(migrationConfigDto.getPgsqlDatabaseName());

        setVerifyDtoOpenGaussParams(migrationConfigDto.getOpenGaussConnectInfo(), verifyDto);
        return verifyDto;
    }

    private static void setVerifyDtoOpenGaussParams(OpenGaussDatabaseConnectInfo connectInfo, VerifyDto verifyDto) {
        verifyDto.setTargetIp(connectInfo.getIp());
        verifyDto.setTargetPort(connectInfo.getPort());
        verifyDto.setTargetUsername(connectInfo.getUsername());
        verifyDto.setTargetPassword(connectInfo.getPassword());
        verifyDto.setTargetDatabase(connectInfo.getDatabaseName());
        verifyDto.setTargetCluster(connectInfo.isClusterAvailable());
        verifyDto.setTargetStandbyHosts(connectInfo.getStandbyHosts());
        verifyDto.setTargetStandbyPorts(connectInfo.getStandbyPorts());
    }
}
