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

import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * kafka connect alert log listener
 *
 * @since 2025/1/7
 */
public class KafkaConnectAlertLogListener extends TailerListenerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectAlertLogListener.class);
    private static final String ALERT_LOG_REGEX = "(?<datetime>\\d{4}-\\d{2}-\\d{2} "
            + "\\d{2}:\\d{2}:\\d{2}(,\\d{3})) \\[(?<thread>[^\\]]+)] ERROR "
            + "(?<logger>[^:]+):\\((?<line>\\d*)\\) - (?<message>[\\s\\S]+)";
    private static final Pattern ALERT_LOG_PATTERN = Pattern.compile(ALERT_LOG_REGEX);

    private final StringBuilder currentLog = new StringBuilder();
    private final int logSource;

    public KafkaConnectAlertLogListener(int logSource) {
        this.logSource = logSource;
    }

    @Override
    public void handle(String line) {
        Matcher lineMatcher = ALERT_LOG_PATTERN.matcher(line);
        if (lineMatcher.matches()) {
            processLog(currentLog.toString());
            currentLog.setLength(0);
            currentLog.append(line).append(System.lineSeparator());
        } else {
            currentLog.append(line).append(System.lineSeparator());
        }
    }

    private void processLog(String logString) {
        if (logString.isBlank()) {
            return;
        }

        Matcher matcher = ALERT_LOG_PATTERN.matcher(logString);
        if (matcher.find()) {
            AlertLogEntity alertLog = AlertLogEntity.builder()
                    .dateTime(matcher.group("datetime"))
                    .thread(matcher.group("thread"))
                    .logLevel("ERROR")
                    .className(matcher.group("logger").trim())
                    .methodName("")
                    .lineNumber(matcher.group("line"))
                    .logCode(String.valueOf(ErrorCode.KAFKA_CONNECT_ABNORMALLY.getCode()))
                    .message(matcher.group("message").trim())
                    .logSource(logSource)
                    .causeCn(ErrorCode.KAFKA_CONNECT_ABNORMALLY.getCauseCn())
                    .causeEn(ErrorCode.KAFKA_CONNECT_ABNORMALLY.getCauseEn())
                    .build();

            AlertLogMessageProcessor.setAlertLogMigrationPhase(alertLog);
            LOGGER.info("Parse one alert log record of kafka-connect.");
            AlertLogFileUtils.writeAlertLogsToFile(List.of(alertLog));
        }
    }

    @Override
    public void handle(Exception ex) {
        LOGGER.error("An exception occurred when listening to the kafka connect error log.", ex);
    }

    @Override
    public void endOfFileReached() {
        processLog(currentLog.toString());
        currentLog.setLength(0);
    }
}
