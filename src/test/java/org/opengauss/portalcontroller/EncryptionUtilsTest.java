/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller;

import org.junit.jupiter.api.Test;
import org.opengauss.portalcontroller.utils.EncryptionUtils;

/**
 * For test decrypt the password
 *
 * @date :2024/10/12 16:30
 * @description: EncryptionUtilsTest
 * @version: 1.1
 * @since 1.1
 */
public class EncryptionUtilsTest {
    @Test
    public void DecryptTest() {
        String mysqlCipherText = "as2hh06ZtPzo1FTjGIEEOohz/Sg0NEhEYQXO";
        String opengaussCipherText = "Sc2/hFWThqnr1Uj2zjQFG8KdJ+8ydqmKJ5Q=";
        String str1 = EncryptionUtils.decrypt(mysqlCipherText, PortalControl.ASE_SECRET_KEY);
        assert str1.equals("password123");
        String str2 = EncryptionUtils.decrypt(opengaussCipherText, PortalControl.ASE_SECRET_KEY);
        assert str2.equals("Sample@123");
    }

    @Test
    public void Decrypt4NoEncryptTest() {
        assert "password".equals(PortalControl.decryptUsingAES("password"));
    }
}
