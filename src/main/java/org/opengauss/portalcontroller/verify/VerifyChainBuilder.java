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

/**
 * VerifyChainBuilder
 *
 * @date :2023/11/3 15:22
 * @description: VerifyChainBuilder
 * @version: 1.1
 * @since 1.1
 */
public class VerifyChainBuilder {
    private AbstractPreMigrationVerifyChain head;

    private AbstractPreMigrationVerifyChain tail;

    private VerifyChainBuilder() {

    }

    private static VerifyChainBuilder getChainBuilder() {
        return new VerifyChainBuilder();
    }

    /**
     * construct online
     *
     * @return AbstractPreMigrationVerifyChain
     */
    public static AbstractPreMigrationVerifyChain getOnlineVerifyChain() {
        return getChainBuilder().addChain(new CommonServiceVerifyChain())
            .addChain(new DatabaseConnectVerifyChain())
            .addChain(new FullPermissionVerifyChain())
            .addChain(new IncrementPermissionVerifyChain())
            .addChain(new IncrementParameterVerifyChain())
            .addChain(new ReversePermissionVerifyChain())
            .addChain(new ReverseParameterVerifyChain())
            .addChain(new LowerParameterVerifyChain())
            .addChain(new BdatabaseVerifyChain())
            .addChain(new DatabaseEncryptionVerifyChain())
            .addChain(new ReplicationNumberVerifyChain())
            .build();
    }

    /**
     * construct offline
     *
     * @return AbstractPreMigrationVerifyChain
     */
    public static AbstractPreMigrationVerifyChain getOfflineVerifyChain() {
        return getChainBuilder().addChain(new CommonServiceVerifyChain())
            .addChain(new DatabaseConnectVerifyChain())
            .addChain(new FullPermissionVerifyChain())
            .addChain(new LowerParameterVerifyChain())
            .addChain(new BdatabaseVerifyChain())
            .addChain(new DatabaseEncryptionVerifyChain())
            .build();
    }

    /**
     * construct reverse
     *
     * @return AbstractPreMigrationVerifyChain
     */
    public static AbstractPreMigrationVerifyChain getReverseVerifyChain() {
        return getChainBuilder().addChain(new CommonServiceVerifyChain())
            .addChain(new DatabaseConnectVerifyChain())
            .addChain(new ReversePermissionVerifyChain())
            .addChain(new ReverseParameterVerifyChain())
            .addChain(new LowerParameterVerifyChain())
            .addChain(new BdatabaseVerifyChain())
            .addChain(new ReplicationNumberVerifyChain())
            .build();
    }

    private VerifyChainBuilder addChain(AbstractPreMigrationVerifyChain chain) {
        if (this.head == null) {
            this.head = chain;
            this.tail = this.head;
            return this;
        }

        this.tail.next = chain;
        this.tail = chain;
        return this;
    }

    private AbstractPreMigrationVerifyChain build() {
        return this.head;
    }
}
