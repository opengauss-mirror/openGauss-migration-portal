/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
