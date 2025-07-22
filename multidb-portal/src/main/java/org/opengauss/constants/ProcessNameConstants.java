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
     * full migration tool order table process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_TABLE = "full-migration tool migration table process";

    /**
     * full migration tool order sequence process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_SEQUENCE =
            "full-migration tool migration sequence process";

    /**
     * full migration tool order primary key process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_PRIMARY_KEY =
            "full-migration tool migration primary key process";

    /**
     * full migration tool order index process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_INDEX =
            "full-migration tool migration index process";

    /**
     * full migration tool order constraint process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_CONSTRAINT =
            "full-migration tool migration constraint process";

    /**
     * full migration tool order view process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_VIEW =
            "full-migration tool migration view process";

    /**
     * full migration tool order function process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_FUNCTION =
            "full-migration tool migration function process";

    /**
     * full migration tool order procedure process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_PROCEDURE =
            "full-migration tool migration procedure process";

    /**
     * full migration tool order trigger process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_TRIGGER =
            "full-migration tool migration trigger process";

    /**
     * full migration tool order foreign key process name
     */
    public static final String FULL_MIGRATION_TOOL_MIGRATION_FOREIGN_KEY =
            "full-migration tool migration foreign key process";

    /**
     * full migration tool order drop replica schema process name
     */
    public static final String FULL_MIGRATION_TOOL_DROP_REPLICA_SCHEMA =
            "full-migration tool drop replica schema process";

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

    private ProcessNameConstants() {
    }
}
