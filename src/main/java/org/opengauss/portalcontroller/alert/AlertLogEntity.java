/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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
