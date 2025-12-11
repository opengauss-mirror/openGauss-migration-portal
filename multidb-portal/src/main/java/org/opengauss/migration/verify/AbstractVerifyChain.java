/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.verify;

import org.opengauss.migration.verify.model.AbstractVerifyDto;
import org.opengauss.migration.verify.model.ChainResult;
import org.opengauss.migration.verify.model.VerifyResult;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract verify chain
 *
 * @since 2025/5/8
 */
public abstract class AbstractVerifyChain {
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?");

    /**
     * Chain result
     */
    protected final ChainResult chainResult = new ChainResult();

    /**
     * Next verify chain
     */
    protected AbstractVerifyChain next;

    /**
     * Verify
     *
     * @param verifyDto verify dto
     * @param verifyResult verify result
     */
    public abstract void verify(AbstractVerifyDto verifyDto, VerifyResult verifyResult);

    /**
     * Transfer to next verify chain
     *
     * @param verifyDto verify dto
     * @param verifyResult verify result
     */
    protected final void transfer(AbstractVerifyDto verifyDto, VerifyResult verifyResult) {
        if (this.next != null) {
            this.next.verify(verifyDto, verifyResult);
        }
    }

    /**
     * Add current chain result to verify result
     *
     * @param verifyResult verify result
     */
    protected final void addCurrentChainResult(VerifyResult verifyResult) {
        verifyResult.addChainResult(this.chainResult);
    }

    /**
     * Compare two versions
     *
     * @param version1 version 1
     * @param version2 version 2
     * @return the comparison result
     */
    protected int compareVersions(String version1, String version2) {
        int[] v1Parts = parseVersion(version1);
        int[] v2Parts = parseVersion(version2);

        int length = Math.max(v1Parts.length, v2Parts.length);
        for (int i = 0; i < length; i++) {
            int v1 = (i < v1Parts.length) ? v1Parts[i] : 0;
            int v2 = (i < v2Parts.length) ? v2Parts[i] : 0;

            if (v1 != v2) {
                return v1 - v2;
            }
        }
        return 0;
    }

    private int[] parseVersion(String versionString) {
        if (versionString == null || versionString.trim().isEmpty()) {
            throw new IllegalArgumentException("Version string is empty");
        }

        Matcher matcher = VERSION_PATTERN.matcher(versionString);

        int[] result = new int[3];
        if (matcher.find()) {
            String major = matcher.group(1);  // 主版本号
            String minor = matcher.group(2);  // 次版本号
            String patch = matcher.group(3);  // 修订版本号
            result[0] = Integer.parseInt(major);

            if (minor != null) {
                result[1] = Integer.parseInt(minor);
            } else {
                result[1] = 0;
            }

            if (patch != null) {
                result[2] = Integer.parseInt(patch);
            } else {
                result[2] = 0;
            }

            return result;
        }

        throw new IllegalArgumentException("Invalid version format: " + versionString);
    }
}
