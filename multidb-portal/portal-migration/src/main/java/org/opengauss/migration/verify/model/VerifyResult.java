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

package org.opengauss.migration.verify.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Verify result
 *
 * @since 2025/5/8
 */
@Getter
public class VerifyResult {
    private final List<ChainResult> chainResults;
    private boolean isSuccess;

    public VerifyResult() {
        chainResults = new ArrayList<>();
        isSuccess = true;
    }

    /**
     * Add one verify chain result
     *
     * @param chainResult chain result
     */
    public void addChainResult(ChainResult chainResult) {
        chainResults.add(chainResult);
        if (!chainResult.isSuccess()) {
            isSuccess = false;
        }
    }

    /**
     * Get verify result contents
     *
     * @return verify result contents
     */
    public String getResult() {
        StringBuilder result = new StringBuilder();
        String success = "SUCCESS";
        String failed = "FAILED";
        result.append("[Verify Result] : ").append(isSuccess ? success : failed).append(".")
                .append(System.lineSeparator());
        for (ChainResult chainResult : chainResults) {
            result.append("[").append(chainResult.getName()).append("] : ");

            if (chainResult.isSuccess()) {
                result.append(success).append(".").append(System.lineSeparator());
            } else {
                result.append(failed).append("; ");
                result.append(chainResult.getDetail()).append(".").append(System.lineSeparator());
            }
        }
        return result.substring(0, result.length() - 1);
    }
}
