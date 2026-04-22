/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.dto;

import lombok.Data;

/**
 * kafka status dto
 *
 * @since 2025/4/24
 */
@Data
public class KafkaStatusDto {
    private boolean isZookeeperRunning;
    private boolean isKafkaRunning;
    private boolean isSchemaRegistryRunning;
}
