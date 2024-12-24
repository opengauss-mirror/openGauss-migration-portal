/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import lombok.Getter;
import org.opengauss.portalcontroller.constant.Command;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.utils.FileUtils;
import org.opengauss.portalcontroller.utils.Log4jUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * alert log collection manager
 *
 * @since 2024/12/6
 */
public class AlertLogCollectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertLogCollectionManager.class);

    private static String alertFileHome;
    @Getter
    private static String alertFilePathModel;
    @Getter
    private static boolean isAlertLogCollectionEnabled = false;
    @Getter
    private static String kafkaServer;
    @Getter
    private static String kafkaTopic;
    private static KafkaAlertLogCollectorThread collectorThread;

    /**
     * start alert log collection
     */
    public static void startCollection() {
        loadConfig();
        if (isAlertLogCollectionEnabled) {
            initAlertFileHome();
            collectorThread = new KafkaAlertLogCollectorThread(kafkaServer, kafkaTopic);
            collectorThread.start();
        } else {
            Log4jUtils.removeRootKafkaAppender();
            Log4jUtils.stopKafkaAppender();
        }
    }

    /**
     * stop alert log collection
     */
    public static void stopCollection() {
        if (collectorThread != null) {
            collectorThread.shutdown();
        }
    }

    private static void loadConfig() {
        if (getSystemProperty(AlertLogConstants.Params.ENABLE_ALERT_LOG_COLLECTION, false).equals("true")) {
            isAlertLogCollectionEnabled = true;
            kafkaServer = getSystemProperty(AlertLogConstants.Params.KAFKA_SEVER, true);
            kafkaTopic = String.format(AlertLogConstants.KAFKA_TOPIC_MODEL,
                    getSystemProperty(Command.Parameters.ID, true));
            alertFileHome = String.format(AlertLogConstants.ALERT_FILE_HOME_PATH_MODEL,
                    getSystemProperty(Command.Parameters.PATH, true),
                    getSystemProperty(Command.Parameters.ID, true));
            alertFilePathModel = alertFileHome + AlertLogConstants.ALERT_FILE_NAME_MODEL;
        }
    }

    private static void initAlertFileHome() {
        try {
            FileUtils.deleteFileOrDirectory(alertFileHome);
            FileUtils.createFile(alertFileHome, false);
        } catch (PortalException e) {
            LOGGER.error("Filed to initialize alert file home.", e);
        }
    }

    private static String getSystemProperty(String propertyName, boolean isLogError) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.trim().isEmpty()) {
            if (isLogError) {
                LOGGER.error("Required property {} is missing or empty.", propertyName);
            }
            return "";
        }
        return propertyValue;
    }
}
