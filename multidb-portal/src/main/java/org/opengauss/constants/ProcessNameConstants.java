/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.constants;

/**
 * Process name constants
 *
 * @since 2025/3/3
 */
public class ProcessNameConstants {
    /**
     * chameleon drop_replica_schema order process name
     */
    public static final String CHAMELEON_DROP_REPLICA_SCHEMA = "chameleon full drop replica schema process";

    /**
     * chameleon create_replica_schema order process name
     */
    public static final String CHAMELEON_CREATE_REPLICA_SCHEMA = "chameleon full create replica schema process";

    /**
     * chameleon add_source order process name
     */
    public static final String CHAMELEON_ADD_SOURCE = "chameleon full add source process";

    /**
     * chameleon init_replica order process name
     */
    public static final String CHAMELEON_INIT_REPLICA = "chameleon full init replica process";

    /**
     * chameleon start_trigger_replica order process name
     */
    public static final String CHAMELEON_START_TRIGGER_REPLICA = "chameleon full start trigger replica process";

    /**
     * chameleon start_view_replica order process name
     */
    public static final String CHAMELEON_START_VIEW_REPLICA = "chameleon full start view replica process";

    /**
     * chameleon start_func_replica order process name
     */
    public static final String CHAMELEON_START_FUNC_REPLICA = "chameleon full start func replica process";

    /**
     * chameleon start_proc_replica order process name
     */
    public static final String CHAMELEON_START_PROC_REPLICA = "chameleon full start proc replica process";

    /**
     * chameleon detach_replica order process name
     */
    public static final String CHAMELEON_DETACH_REPLICA = "chameleon full detach replica process";

    /**
     * full_replicate order table process name
     */
    public static final String FULL_REPLICATE_MIGRATION_TABLE = "full_replicate migration table process";

    /**
     * full_replicate order sequence process name
     */
    public static final String FULL_REPLICATE_MIGRATION_SEQUENCE = "full_replicate migration sequence process";

    /**
     * full_replicate order primary key process name
     */
    public static final String FULL_REPLICATE_MIGRATION_PRIMARY_KEY =
            "full_replicate migration primary key process";

    /**
     * full_replicate order index process name
     */
    public static final String FULL_REPLICATE_MIGRATION_INDEX = "full_replicate migration index process";

    /**
     * full_replicate order constraint process name
     */
    public static final String FULL_REPLICATE_MIGRATION_CONSTRAINT =
            "full_replicate migration constraint process";

    /**
     * full_replicate order view process name
     */
    public static final String FULL_REPLICATE_MIGRATION_VIEW = "full_replicate migration view process";

    /**
     * full_replicate order function process name
     */
    public static final String FULL_REPLICATE_MIGRATION_FUNCTION = "full_replicate migration function process";

    /**
     * full_replicate order procedure process name
     */
    public static final String FULL_REPLICATE_MIGRATION_PROCEDURE =
            "full_replicate migration procedure process";

    /**
     * full_replicate order trigger process name
     */
    public static final String FULL_REPLICATE_MIGRATION_TRIGGER = "full_replicate migration trigger process";

    /**
     * full_replicate order foreign key process name
     */
    public static final String FULL_REPLICATE_MIGRATION_FOREIGN_KEY =
            "full_replicate migration foreign key process";

    /**
     * full_replicate order drop replica schema process name
     */
    public static final String FULL_REPLICATE_DROP_REPLICA_SCHEMA =
            "full_replicate drop replica schema process";

    /**
     * debezium incremental connect source process name
     */
    public static final String DEBEZIUM_INCREMENTAL_CONNECT_SOURCE = "debezium incremental connect source process";

    /**
     * debezium incremental connect sink process name
     */
    public static final String DEBEZIUM_INCREMENTAL_CONNECT_SINK = "debezium incremental connect sink process";

    /**
     * debezium reverse connect source process name
     */
    public static final String DEBEZIUM_REVERSE_CONNECT_SOURCE = "debezium reverse connect source process";

    /**
     * debezium reverse connect sink process name
     */
    public static final String DEBEZIUM_REVERSE_CONNECT_SINK = "debezium reverse connect sink process";

    /**
     * data checker full sink process name
     */
    public static final String DATA_CHECKER_FULL_SINK = "data checker sink process";

    /**
     * data checker full source process name
     */
    public static final String DATA_CHECKER_FULL_SOURCE = "data checker source process";

    /**
     * data checker full check process name
     */
    public static final String DATA_CHECKER_FULL_CHECK = "data checker check process";

    /**
     * data checker incremental sink process name
     */
    public static final String DATA_CHECKER_INCREMENTAL_SINK = "data checker incremental sink process";

    /**
     * data checker incremental source process name
     */
    public static final String DATA_CHECKER_INCREMENTAL_SOURCE = "data checker incremental source process";

    /**
     * data checker incremental check process name
     */
    public static final String DATA_CHECKER_INCREMENTAL_CHECK = "data checker incremental check process";

    /**
     * Milvus migration tool process name
     */
    public static final String MILVUS_MIGRATION_TOOL = "Milvus migration tool process";

    /**
     * Elasticsearch migration tool process name
     */
    public static final String ELASTICSEARCH_MIGRATION_TOOL = "Elasticsearch migration tool process";

    private ProcessNameConstants() {
    }
}
