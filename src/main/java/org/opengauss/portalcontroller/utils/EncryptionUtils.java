/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
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

package org.opengauss.portalcontroller.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.util.StringUtils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * For decrypt the password
 *
 * @date :2024/10/12 16:30
 * @description: EncryptionUtils
 * @version: 1.1
 * @since 1.1
 */
@Slf4j
public final class EncryptionUtils {
    /**
     * Define the AES/GCM/PKCS5Padding algorithm for encryption/decryption
     */
    private static String gcm256Algorithm = "AES/GCM/PKCS5Padding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;
    private static byte[] iV = new byte[GCM_IV_LENGTH];

    /**
     * For decrypt the password
     *
     * @param cipherText cipherText
     * @param keyStr AES sercertKey
     * @return String return text after decrypt
     */
    public static String decrypt(String cipherText, String keyStr) {
        log.info("decrypt, cipherText:{}", cipherText);
        if (StringUtils.isEmpty(cipherText) || StringUtils.isEmpty(keyStr)) {
            throw new IllegalArgumentException("cipherText or key is empty");
        }
        try {
            // Create SecretKeySpec
            SecretKeySpec keySpec = new SecretKeySpec(Base64.decodeBase64(keyStr), "AES");
            // Create GCMParameterSpec
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iV);
            // Get Cipher Instance
            Cipher cipher = Cipher.getInstance(gcm256Algorithm);
            // Initialize Cipher for DECRYPT_MODE
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmParameterSpec);
            // Perform Decryption
            byte[] decryptedText = cipher.doFinal(Base64.decodeBase64(cipherText));
            return new String(decryptedText, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException | InvalidAlgorithmParameterException e) {
            log.error("Invalid parameter", e);
            throw new IllegalArgumentException("Invalid parameter");
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalBlockSizeException
                    |BadPaddingException | InvalidKeyException e) {
            log.error("decrypt fail", e);
            throw new UnsupportedOperationException("decrypt fail");
        }
    }
}
