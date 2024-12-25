/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * log4j2 utils
 *
 * @since 2024/12/6
 */
public class Log4jUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);
    private static final String KAFKA_APPENDER_NAME = "kafka";

    /**
     * remove kafka appender in root logger
     */
    public static void removeRootKafkaAppender() {
        if (LogManager.getContext(false) instanceof LoggerContext) {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = context.getConfiguration();

            LoggerConfig loggerConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
            loggerConfig.removeAppender(KAFKA_APPENDER_NAME);
            LOGGER.info("KafkaAppender has been removed from root logger.");
        }
    }

    /**
     * stop kafka appender
     */
    public static void stopKafkaAppender() {
        if (LogManager.getContext(false) instanceof LoggerContext) {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = context.getConfiguration();

            Appender kafkaAppender = configuration.getAppenders().get(KAFKA_APPENDER_NAME);
            if (kafkaAppender != null && !kafkaAppender.isStopped()) {
                kafkaAppender.stop();
                LOGGER.info("KafkaAppender has been stopped.");
            }
        }
    }

    /**
     * remove log4j2.xml file in workspace config directory
     *
     * @param workspacePath workspace path
     */
    public static void removeLog4jXmlInWorkspace(String workspacePath) {
        String xmlPath = PathUtils.combainPath(true, workspacePath, "config", "log4j2.xml");
        FileUtils.removeFileOrDirectory(xmlPath);
    }
}
