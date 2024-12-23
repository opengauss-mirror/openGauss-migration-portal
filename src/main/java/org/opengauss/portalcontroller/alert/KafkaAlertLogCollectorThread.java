/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opengauss.portalcontroller.handler.ThreadExceptionHandler;
import org.opengauss.portalcontroller.task.Plan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * alert log collection thread
 *
 * @since 2024/12/6
 */
public class KafkaAlertLogCollectorThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAlertLogCollectorThread.class);
    private static final String GROUP_ID = "portal";
    private static final Duration POLL_DURATION = Duration.ofMillis(500);

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final String topic;
    private volatile boolean isRunning = true;

    public KafkaAlertLogCollectorThread(String bootstrapServers, String topic) {
        this.topic = topic;
        this.kafkaConsumer = createConsumer(bootstrapServers);
    }

    private KafkaConsumer<String, String> createConsumer(String bootstrapServers) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(config);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        try {
            while (isRunning && !Plan.stopPlan) {
                processMessages();
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    private void processMessages() {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(POLL_DURATION);
        if (!records.isEmpty()) {
            AlertLogMessageProcessor.processMessage(records);
        }
    }

    /**
     * shut down thread
     */
    public void shutdown() {
        isRunning = false;
    }
}
