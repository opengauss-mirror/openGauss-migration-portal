/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.printer.TablePrinter;
import org.opengauss.domain.dto.KafkaStatusDto;
import org.opengauss.migration.tools.Kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * kafka command receiver
 *
 * @since 2025/3/29
 */
public class KafkaCommandReceiver implements CommandReceiver {
    private static final Logger LOGGER = LogManager.getLogger(KafkaCommandReceiver.class);

    private final Kafka kafka;

    public KafkaCommandReceiver() {
        kafka = Kafka.getInstance();
    }

    /**
     * start kafka processes
     */
    public void start() {
        kafka.start();
    }

    /**
     * stop kafka processes
     */
    public void stop() {
        kafka.stop();
    }

    /**
     * get kafka processes status
     */
    public void status() {
        Optional<KafkaStatusDto> statusOptional = kafka.getStatusDetail();
        if (statusOptional.isEmpty()) {
            return;
        }

        List<String> header = new ArrayList<>();
        header.add("Component");
        header.add("Running");
        header.add("Stopped");

        KafkaStatusDto kafkaStatusDto = statusOptional.get();
        List<String> row = new ArrayList<>();
        if (kafkaStatusDto.isZookeeperRunning()) {
            row.add("Zookeeper");
            row.add("Y");
            row.add("");
        } else {
            row.add("Zookeeper");
            row.add("");
            row.add("Y");
        }
        List<List<String>> tableInfoList = new ArrayList<>();
        tableInfoList.add(row);

        row = new ArrayList<>();
        if (kafkaStatusDto.isKafkaRunning()) {
            row.add("Kafka");
            row.add("Y");
            row.add("");
        } else {
            row.add("Kafka");
            row.add("");
            row.add("Y");
        }
        tableInfoList.add(row);

        row = new ArrayList<>();
        if (kafkaStatusDto.isSchemaRegistryRunning()) {
            row.add("Schema Registry");
            row.add("Y");
            row.add("");
        } else {
            row.add("Schema Registry");
            row.add("");
            row.add("Y");
        }
        tableInfoList.add(row);
        String table = TablePrinter.printTable(header, tableInfoList);
        LOGGER.info("Kafka Processes Status:{}{}", System.lineSeparator(), table);
    }

    /**
     * clean kafka logs
     */
    public void clean() {
        kafka.clean();
    }
}
