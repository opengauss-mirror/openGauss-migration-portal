/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.ConfigDescCommandReceiver;
import org.opengauss.constants.TaskConstants;
import org.opengauss.enums.DatabaseType;

import java.util.Locale;

/**
 * config description command
 *
 * @since 2025/6/24
 */
public class ConfigDescCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(ConfigDescCommand.class);

    private final String databaseType;

    public ConfigDescCommand(String databaseType) {
        if (databaseType == null) {
            throw new IllegalArgumentException("Missing argument: databaseType");
        }
        this.databaseType = databaseType;
    }

    @Override
    public void execute() {
        DatabaseType type = parseDatabaseType();
        ConfigDescCommandReceiver commandReceiver = new ConfigDescCommandReceiver();

        switch (type) {
            case MYSQL:
                LOGGER.info("Start command to get MySQL migration configuration description");
                commandReceiver.mysqlConfigDesc();
                break;
            case POSTGRESQL:
                LOGGER.info("Start command to get PostgreSQL migration configuration description");
                commandReceiver.pgsqlConfigDesc();
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
    }

    private DatabaseType parseDatabaseType() {
        try {
            DatabaseType type = DatabaseType.valueOf(databaseType.toUpperCase(Locale.ROOT));
            if (TaskConstants.SUPPORTED_SOURCE_DB_TYPES.contains(type)) {
                return type;
            } else {
                throw new IllegalArgumentException("Unsupported database type: " + databaseType);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
    }
}
