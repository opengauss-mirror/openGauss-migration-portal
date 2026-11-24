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

package org.opengauss.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.command.receiver.KafkaCommandReceiver;

/**
 * Kafka command
 *
 * @since 2025/3/26
 */
public class KafkaCommand implements Command {
    private static final Logger LOGGER = LogManager.getLogger(KafkaCommand.class);

    private final String operation;

    KafkaCommand(String operation) {
        this.operation = operation;
    }

    @Override
    public void execute() {
        KafkaCommandReceiver commandReceiver = new KafkaCommandReceiver();

        switch (operation) {
            case "start":
                LOGGER.info("Start command to start Kafka");
                commandReceiver.start();
                break;
            case "stop":
                LOGGER.info("Start command to stop Kafka");
                commandReceiver.stop();
                break;
            case "status":
                LOGGER.info("Start command to get Kafka status");
                commandReceiver.status();
                break;
            case "clean":
                LOGGER.info("Start command to clean Kafka data");
                commandReceiver.clean();
                break;
            default:
                throw new IllegalArgumentException("Unsupported Kafka operation: " + operation);
        }
    }
}
