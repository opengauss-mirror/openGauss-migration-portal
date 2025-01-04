/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * migration tool's alert log message processor
 *
 * @since 2024/12/6
 */
public class AlertLogMessageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertLogMessageProcessor.class);
    private static final String ALERT_LOG_REGEX = "(?<datetime>\\d{4}-\\d{2}-\\d{2} "
            + "\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?(,\\d{3})?) \\[(?<thread>[^\\]]+)] (?<level>\\w+) "
            + "(?<logger>[^:]+):(?<method>[^(]*)\\((?<line>\\d*)\\) - <CODE:(?<code>\\d{4})> (?<message>[\\s\\S]+)";
    private static final Pattern ALERT_LOG_PATTERN = Pattern.compile(ALERT_LOG_REGEX);
    private static final Map<String, Map<Integer, String>> CODE_CAUSE_CN_MAP;
    private static final Map<String, Map<Integer, String>> CODE_CAUSE_EN_MAP;
    private static final List<AlertLogEntity> alertLogs = new ArrayList<>();

    @Getter
    private static int recentMigrationStatus = 1;

    static {
        CODE_CAUSE_CN_MAP = new HashMap<>();
        CODE_CAUSE_EN_MAP = new HashMap<>();

        CODE_CAUSE_CN_MAP.put(AlertLogConstants.MigrationTools.PORTAL, ErrorCode.getCodeCauseCnMap());
        CODE_CAUSE_CN_MAP.put(AlertLogConstants.MigrationTools.CHAMELEON, null);
        CODE_CAUSE_CN_MAP.put(AlertLogConstants.MigrationTools.DATA_CHECKER, null);
        CODE_CAUSE_CN_MAP.put(AlertLogConstants.MigrationTools.DEBEZIUM, null);

        CODE_CAUSE_EN_MAP.put(AlertLogConstants.MigrationTools.PORTAL, ErrorCode.getCodeCauseEnMap());
        CODE_CAUSE_EN_MAP.put(AlertLogConstants.MigrationTools.CHAMELEON, null);
        CODE_CAUSE_EN_MAP.put(AlertLogConstants.MigrationTools.DATA_CHECKER, null);
        CODE_CAUSE_EN_MAP.put(AlertLogConstants.MigrationTools.DEBEZIUM, null);
    }

    /**
     * process alert log records in kafka
     *
     * @param records records list
     */
    public static void processMessage(ConsumerRecords<String, String> records) {
        alertLogs.clear();

        for (ConsumerRecord<String, String> record : records) {
            String value = record.value().trim();
            if (value.startsWith(AlertLogConstants.CODE_CAUSE_CN_PREFIX)) {
                parseCodeCause(record.key(), value);
            } else {
                parseAlertLog(record.key(), value);
            }
        }

        if (!alertLogs.isEmpty()) {
            AlertLogFileUtils.writeAlertLogsToFile(alertLogs);
        }
    }

    private static void parseCodeCause(String source, String cause) {
        if (!AlertLogConstants.MigrationTools.MIGRATION_TOOL_LIST.contains(source)) {
            LOGGER.warn("Invalid alert log message in kafka.");
        }

        if (CODE_CAUSE_CN_MAP.get(source) == null) {
            String[] causes = cause.substring(AlertLogConstants.CODE_CAUSE_CN_PREFIX.length())
                    .split(AlertLogConstants.CODE_CAUSE_EN_PREFIX);
            if (causes.length != 2) {
                LOGGER.warn("Invalid error code cause.");
                return;
            }

            CODE_CAUSE_CN_MAP.put(source,
                    JSON.parseObject(causes[0], new TypeReference<HashMap<Integer, String>>() {}));
            CODE_CAUSE_EN_MAP.put(source,
                    JSON.parseObject(causes[1], new TypeReference<HashMap<Integer, String>>() {}));
            LOGGER.info("Error code cause of {} registered successfully.", source);
        }
    }

    private static void parseAlertLog(String source, String logMessage) {
        if (!AlertLogConstants.AlertLogSources.ALERT_LOG_SOURCE_LIST.contains(source)) {
            LOGGER.warn("Invalid alert log message in kafka.");
            return;
        }

        Matcher matcher = ALERT_LOG_PATTERN.matcher(logMessage);
        if (matcher.find()) {
            AlertLogEntity alertLog = AlertLogEntity.builder()
                    .dateTime(matcher.group("datetime"))
                    .thread(matcher.group("thread"))
                    .logLevel(matcher.group("level"))
                    .className(matcher.group("logger").trim())
                    .methodName(matcher.group("method"))
                    .lineNumber(matcher.group("line"))
                    .logCode(matcher.group("code"))
                    .message(matcher.group("message").trim()).build();

            alertLog.setLogSource(AlertLogSourceEnum.getIdOfSource(source));
            setAlertLogCause(alertLog, source);
            setAlertLogMigrationPhase(alertLog);

            alertLogs.add(alertLog);
            LOGGER.info("Parse one alert log record from {}.", source);
        }
    }

    private static void setAlertLogCause(AlertLogEntity alertLog, String source) {
        Integer code = Integer.valueOf(alertLog.getLogCode());
        String toolName = AlertLogConstants.getMigrationToolBySource(source);

        alertLog.setCauseCn(CODE_CAUSE_CN_MAP.get(toolName).get(code));
        alertLog.setCauseEn(CODE_CAUSE_EN_MAP.get(toolName).get(code));
    }

    private static void setAlertLogMigrationPhase(AlertLogEntity alertLog) {
        setRecentMigrationStatus(PortalControl.status);
        alertLog.setMigrationPhase(AlertLogMigrationPhaseEnum.getPhaseIdByStatus(recentMigrationStatus));
    }

    public static void setRecentMigrationStatus(int recentMigrationStatus) {
        if (recentMigrationStatus != Status.ERROR) {
            AlertLogMessageProcessor.recentMigrationStatus = recentMigrationStatus;
        }
    }
}
