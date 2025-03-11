/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * alert log file utils
 *
 * @since 2024/12/13
 */
public class AlertLogFileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertLogFileUtils.class);
    private static final int ALERT_FILE_SIZE = 100;
    private static int alertLogNumbers = 0;
    private static int alertFileNumbers = 0;
    private static String alertFilePath;

    /**
     * write alert log entities to alert file
     *
     * @param alertLogs alert log entity list
     */
    public static synchronized void writeAlertLogsToFile(List<AlertLogEntity> alertLogs) {
        for (AlertLogEntity alertLog : alertLogs) {
            generateAlertFilePath();
            alertLogNumbers += 1;
            writeLogToFile(alertLog);
            LOGGER.info("One alert log has been write to alert file. Alert log numbers is {}", alertLogNumbers);
        }
    }

    private static void generateAlertFilePath() {
        if (alertLogNumbers / ALERT_FILE_SIZE + 1 > alertFileNumbers) {
            alertFileNumbers = alertLogNumbers / ALERT_FILE_SIZE + 1;
            alertFilePath = String.format(AlertLogCollectionManager.getAlertFilePathModel(), alertFileNumbers);
            LOGGER.info("Generate a new alert file.");
        }
    }

    private static void writeLogToFile(AlertLogEntity alertLog) {
        BufferedWriter writer = null;
        try (RandomAccessFile raf = new RandomAccessFile(alertFilePath, "rw");
             FileChannel channel = raf.getChannel();
             FileLock lock = channel.lock()) {
            raf.seek(raf.length());
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    raf.getFD()), StandardCharsets.UTF_8));
            String jsonString = JSON.toJSONString(alertLog, true);
            writer.write(jsonString);
            writer.write(AlertLogConstants.OBJECT_SEPARATOR);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write alert logs to file", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to close writer", e);
                }
            }
        }
    }

    /**
     * print error to alert file
     *
     * @param clazz class
     * @param errorMsg error msg
     * @param errorCode error code
     */
    public static void printErrorToAlertFile(Class<?> clazz, String errorMsg, ErrorCode errorCode) {
        AlertLogEntity alertLog = AlertLogEntity.builder()
                .message(errorMsg)
                .thread(Thread.currentThread().getName())
                .className(clazz.getName())
                .migrationPhase(AlertLogMigrationPhaseEnum.getPhaseIdByStatus(
                        AlertLogMessageProcessor.getRecentMigrationStatus()))
                .logSource(AlertLogSourceEnum.PORTAL.getSourceId())
                .logCode(String.valueOf(errorCode.getCode()))
                .causeCn(errorCode.getCauseCn())
                .causeEn(errorCode.getCauseEn())
                .logLevel("ERROR")
                .build();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        alertLog.setDateTime(LocalDateTime.now().format(formatter));

        writeAlertLogsToFile(List.of(alertLog));
    }

    /**
     * print uncaught error to alert file
     *
     * @param alertLog alert log entity
     * @param e throwable
     */
    public static void printUncaughtError(AlertLogEntity alertLog, Throwable e) {
        if (AlertLogCollectionManager.isAlertLogCollectionEnabled()) {
            generateAlertLog(alertLog, e);
            writeAlertLogsToFile(List.of(alertLog));
        }
    }

    private static void generateAlertLog(AlertLogEntity alertLog, Throwable e) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        alertLog.setDateTime(LocalDateTime.now().format(formatter));

        StringBuilder message = new StringBuilder();
        message.append(alertLog.getMessage()).append(System.lineSeparator());
        String name = e.getClass().getName();
        message.append(name).append(": ").append(e.getMessage()).append(System.lineSeparator());
        for (StackTraceElement element : e.getStackTrace()) {
            message.append("\tat ").append(element.toString()).append(System.lineSeparator());
        }
        alertLog.setMessage(message.toString().trim());

        alertLog.setMigrationPhase(AlertLogMigrationPhaseEnum.getPhaseIdByStatus(
                AlertLogMessageProcessor.getRecentMigrationStatus()));
        alertLog.setLogSource(AlertLogSourceEnum.PORTAL.getSourceId());
        alertLog.setCauseCn(ErrorCode.UNKNOWN.getCauseCn());
        alertLog.setCauseEn(ErrorCode.UNKNOWN.getCauseEn());
        alertLog.setLogLevel("ERROR");
        alertLog.setMethodName("");
        alertLog.setLineNumber("");
        alertLog.setLogCode(String.valueOf(ErrorCode.UNKNOWN.getCode()));
    }
}
