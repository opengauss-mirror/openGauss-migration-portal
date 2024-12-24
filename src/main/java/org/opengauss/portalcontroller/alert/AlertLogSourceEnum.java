/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * alert log source enum
 *
 * @since 2024/12/13
 */
@Getter
public enum AlertLogSourceEnum {
    PORTAL(0, "portal log file portal_id.log"),
    FULL_MIGRATION(10, "full migration log file full_migration.log"),
    CHECK_CHECK(20, "data check log file check.log"),
    CHECK_SOURCE(21, "data check log file source.log"),
    CHECK_SINK(22, "data check log file sink.log"),
    CONNECT_SOURCE(31, "incremental migration log file connect_source.log"),
    CONNECT_SINK(32, "incremental migration log file connect_sink.log"),
    REVERSE_CONNECT_SOURCE(41, "reverse migration log file reverse_connect_source.log"),
    REVERSE_CONNECT_SINK(42, "reverse migration log file reverse_connect_sink.log");

    private static final Map<String, Integer> SOURCE_ID_MAP = new HashMap<>();

    static {
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.PORTAL, PORTAL.sourceId);
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.CHAMELEON, FULL_MIGRATION.sourceId);
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.DATA_CHECK_CHECK, CHECK_CHECK.sourceId);
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.DATA_CHECK_SOURCE, CHECK_SOURCE.sourceId);
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.DATA_CHECK_SINK, CHECK_SINK.sourceId);
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.CONNECT_SOURCE, CONNECT_SOURCE.sourceId);
        SOURCE_ID_MAP.put(AlertLogConstants.AlertLogSources.CONNECT_SINK, CONNECT_SINK.sourceId);
        SOURCE_ID_MAP.put
                (AlertLogConstants.AlertLogSources.REVERSE_CONNECT_SOURCE, REVERSE_CONNECT_SOURCE.sourceId);
        SOURCE_ID_MAP.put(
                AlertLogConstants.AlertLogSources.REVERSE_CONNECT_SINK, REVERSE_CONNECT_SINK.sourceId);
    }

    private final int sourceId;
    private final String description;

    AlertLogSourceEnum(int sourceId, String description) {
        this.sourceId = sourceId;
        this.description = description;
    }

    /**
     * get source id by source name
     *
     * @param source source name
     * @return Integer source id
     */
    public static Integer getIdOfSource(String source) {
        return SOURCE_ID_MAP.get(source);
    }
}
