/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify;

import org.opengauss.enums.MigrationPhase;
import org.opengauss.migration.verify.mysql.MysqlAuthPluginVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlBinLogVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlConnectVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlFullPermissionVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlGtidSetVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlIncrementalPermissionVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlLowerCaseVerifyChain;
import org.opengauss.migration.verify.mysql.MysqlReversePermissionVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussConnectVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussEnableSlotLogVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussFullPermissionVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussIncrementalPermissionVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussReplicationConnectionVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussReplicationNumberVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussReversePermissionVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussSqlCompatibilityVerifyChain;
import org.opengauss.migration.verify.opengauss.OpenGaussWalLevelVerifyChain;
import org.opengauss.migration.verify.pgsql.PgsqlConnectVerifyChain;
import org.opengauss.migration.verify.pgsql.PgsqlReplicationConnectionVerifyChain;
import org.opengauss.migration.verify.pgsql.PgsqlReplicationNumberVerifyChain;

import java.util.List;

/**
 * Verify chain builder
 *
 * @since 2025/5/8
 */
public class VerifyChainBuilder {
    private AbstractVerifyChain head;
    private AbstractVerifyChain tail;

    private VerifyChainBuilder() {
    }

    /**
     * Get MySQL migration verify chain
     *
     * @param migrationPhaseList migration phase list
     * @return verify chain
     */
    public static AbstractVerifyChain getMysqlMigrationVerifyChain(List<MigrationPhase> migrationPhaseList) {
        VerifyChainBuilder builder = new VerifyChainBuilder();
        builder.addVerifyChain(new MysqlConnectVerifyChain())
                .addVerifyChain(new OpenGaussConnectVerifyChain())
                .addVerifyChain(new MysqlLowerCaseVerifyChain())
                .addVerifyChain(new OpenGaussSqlCompatibilityVerifyChain());

        if (migrationPhaseList.contains(MigrationPhase.FULL_MIGRATION)) {
            builder.addVerifyChain(new MysqlFullPermissionVerifyChain())
                    .addVerifyChain(new OpenGaussFullPermissionVerifyChain())
                    .addVerifyChain(new MysqlAuthPluginVerifyChain());
        }

        if (migrationPhaseList.contains(MigrationPhase.INCREMENTAL_MIGRATION)) {
            builder.addVerifyChain(new MysqlIncrementalPermissionVerifyChain())
                    .addVerifyChain(new OpenGaussIncrementalPermissionVerifyChain())
                    .addVerifyChain(new MysqlBinLogVerifyChain())
                    .addVerifyChain(new MysqlGtidSetVerifyChain());
        }

        if (migrationPhaseList.contains(MigrationPhase.REVERSE_MIGRATION)) {
            builder.addVerifyChain(new MysqlReversePermissionVerifyChain())
                    .addVerifyChain(new OpenGaussReversePermissionVerifyChain())
                    .addVerifyChain(new OpenGaussWalLevelVerifyChain())
                    .addVerifyChain(new OpenGaussReplicationConnectionVerifyChain())
                    .addVerifyChain(new OpenGaussReplicationNumberVerifyChain())
                    .addVerifyChain(new OpenGaussEnableSlotLogVerifyChain());
        }
        return builder.build();
    }

    /**
     * Get MySQL reverse phase verify chain
     *
     * @return verify chain
     */
    public static AbstractVerifyChain getMysqlReversePhaseVerifyChain() {
        VerifyChainBuilder builder = new VerifyChainBuilder();
        builder.addVerifyChain(new MysqlConnectVerifyChain())
                .addVerifyChain(new OpenGaussConnectVerifyChain())
                .addVerifyChain(new MysqlReversePermissionVerifyChain())
                .addVerifyChain(new OpenGaussReversePermissionVerifyChain())
                .addVerifyChain(new OpenGaussWalLevelVerifyChain())
                .addVerifyChain(new OpenGaussReplicationConnectionVerifyChain())
                .addVerifyChain(new OpenGaussEnableSlotLogVerifyChain());
        return builder.build();
    }

    /**
     * Get PostgreSQL migration verify chain
     *
     * @param migrationPhaseList migration phase list
     * @return verify chain
     */
    public static AbstractVerifyChain getPgsqlMigrationVerifyChain(List<MigrationPhase> migrationPhaseList) {
        VerifyChainBuilder builder = new VerifyChainBuilder();
        builder.addVerifyChain(new PgsqlConnectVerifyChain())
                .addVerifyChain(new OpenGaussConnectVerifyChain())
                .addVerifyChain(new OpenGaussSqlCompatibilityVerifyChain());

        if (migrationPhaseList.contains(MigrationPhase.FULL_MIGRATION)) {
            builder.addVerifyChain(new OpenGaussFullPermissionVerifyChain());
        }

        if (migrationPhaseList.contains(MigrationPhase.INCREMENTAL_MIGRATION)) {
            builder.addVerifyChain(new OpenGaussIncrementalPermissionVerifyChain())
                    .addVerifyChain(new PgsqlReplicationConnectionVerifyChain())
                    .addVerifyChain(new PgsqlReplicationNumberVerifyChain());
        }

        if (migrationPhaseList.contains(MigrationPhase.REVERSE_MIGRATION)) {
            builder.addVerifyChain(new OpenGaussReversePermissionVerifyChain())
                    .addVerifyChain(new OpenGaussWalLevelVerifyChain())
                    .addVerifyChain(new OpenGaussReplicationConnectionVerifyChain())
                    .addVerifyChain(new OpenGaussReplicationNumberVerifyChain())
                    .addVerifyChain(new OpenGaussEnableSlotLogVerifyChain());
        }
        return builder.build();
    }

    /**
     * Get PostgreSQL reverse phase verify chain
     *
     * @return verify chain
     */
    public static AbstractVerifyChain getPgsqlReversePhaseVerifyChain() {
        VerifyChainBuilder builder = new VerifyChainBuilder();
        builder.addVerifyChain(new PgsqlConnectVerifyChain())
                .addVerifyChain(new OpenGaussConnectVerifyChain())
                .addVerifyChain(new OpenGaussReversePermissionVerifyChain())
                .addVerifyChain(new OpenGaussWalLevelVerifyChain())
                .addVerifyChain(new OpenGaussReplicationConnectionVerifyChain())
                .addVerifyChain(new OpenGaussEnableSlotLogVerifyChain());
        return builder.build();
    }

    private VerifyChainBuilder addVerifyChain(AbstractVerifyChain verifyChain) {
        if (head == null) {
            head = verifyChain;
        } else {
            tail.next = verifyChain;
        }
        tail = verifyChain;
        return this;
    }

    private AbstractVerifyChain build() {
        return head;
    }
}
