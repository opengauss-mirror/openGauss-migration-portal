/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.migration.provider;

import org.opengauss.enums.DatabaseType;
import org.opengauss.migration.exceptions.MigrationException;

import java.util.EnumMap;
import java.util.Map;

/**
 * Migration provider registry, holds all registered database migration providers
 *
 * @since 2026/10/15
 */
public final class MigrationProviderRegistry {
    private static final Map<DatabaseType, DatabaseMigrationProvider> PROVIDERS = new EnumMap<>(DatabaseType.class);

    static {
        register(new MysqlMigrationProvider());
        register(new PgsqlMigrationProvider());
        register(new MilvusMigrationProvider());
        register(new ElasticsearchMigrationProvider());
    }

    private MigrationProviderRegistry() {
    }

    /**
     * Get migration provider by database type
     *
     * @param databaseType database type
     * @return database migration provider
     */
    public static DatabaseMigrationProvider getProvider(DatabaseType databaseType) {
        DatabaseMigrationProvider provider = PROVIDERS.get(databaseType);
        if (provider == null) {
            throw new MigrationException("Unsupported source database type: " + databaseType);
        }
        return provider;
    }

    private static void register(DatabaseMigrationProvider provider) {
        PROVIDERS.put(provider.databaseType(), provider);
    }
}
