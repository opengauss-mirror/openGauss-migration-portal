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

package org.opengauss.portalcontroller.alert;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * alert log entity
 *
 * @since 2024/12/6
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertLogEntity {
    private int migrationPhase;
    private int logSource;
    private String causeCn;
    private String causeEn;
    private String dateTime;
    private String thread;
    private String logLevel;
    private String className;
    private String methodName;
    private String lineNumber;
    private String message;
    private String logCode;
}
