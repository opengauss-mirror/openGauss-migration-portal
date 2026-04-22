/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.parser;

import lombok.Getter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * command parser
 *
 * @since 2025/3/26
 */
public class CommandParser {
    @Getter
    private final Options options = new Options();
    private final CommandLineParser parser = new DefaultParser();

    public CommandParser() {
        buildInstallOptions();
        buildUnInstallOptions();
        buildKafkaOptions();
        buildModeOptions();
        buildTaskOptions();
        buildMigrationOptions();
        buildConfigDescriptionOptions();
        buildForceOptions();
        buildDetailOptions();
        buildHelpOptions();
    }

    /**
     * Parse command options
     *
     * @param args args
     * @return CommandLine
     * @throws ParseException ParseException
     */
    public CommandLine parse(String[] args) throws ParseException {
        return parser.parse(options, args);
    }

    private void buildInstallOptions() {
        Option install = Option.builder()
                .option("i")
                .longOpt("install")
                .desc("install component [tools|chameleon|og_datasync|milvus_migration_tool|"
                        + "elasticsearch_migration_tool|debezium|data_checker|kafka|dependencies|check] <--force>")
                .hasArg()
                .argName("component")
                .build();

        options.addOption(install);
    }

    private void buildUnInstallOptions() {
        Option uninstall = Option.builder()
                .option("u")
                .longOpt("uninstall")
                .desc("uninstall component [tools]")
                .hasArg()
                .argName("component")
                .build();
        options.addOption(uninstall);
    }

    private void buildKafkaOptions() {
        Option kafka = Option.builder()
                .option("k")
                .longOpt("kafka")
                .desc("Kafka operation [status|start|stop|clean]")
                .hasArg()
                .argName("operation")
                .build();
        options.addOption(kafka);
    }

    private void buildModeOptions() {
        Option mode = Option.builder()
                .option("mo")
                .longOpt("mode")
                .desc("Migration mode management "
                        + "[list|add|delete|update|template] <mode_file_path> <mode_name>")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .argName("operation> <args...")
                .build();
        options.addOption(mode);
    }

    private void buildTaskOptions() {
        Option prepare = Option.builder()
                .option("t")
                .longOpt("task")
                .desc("Migration task management [list|create|delete] <task_id> <source_db_type>")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .argName("operation> <args...")
                .build();
        options.addOption(prepare);
    }

    private void buildMigrationOptions() {
        Option migration = Option.builder()
                .option("m")
                .longOpt("migration")
                .desc("Migration operations [start|status|stop|start_incremental|stop_incremental|start_rev"
                        + "erse|resume_incremental|restart_incremental|resume_reverse|restart_reverse] <--detail>")
                .numberOfArgs(2)
                .argName("operation> <workspace_id")
                .build();
        options.addOption(migration);
    }

    private void buildConfigDescriptionOptions() {
        Option config = Option.builder()
                .option("cd")
                .longOpt("config_description")
                .desc("Get migration config description")
                .numberOfArgs(1)
                .argName("databaseType")
                .build();
        options.addOption(config);
    }

    private void buildForceOptions() {
        Option force = Option.builder()
                .option("f")
                .longOpt("force")
                .desc("force to do some operations")
                .build();

        options.addOption(force);
    }

    private void buildDetailOptions() {
        Option detail = Option.builder()
                .option("d")
                .longOpt("detail")
                .desc("get migration details")
                .build();
        options.addOption(detail);
    }

    private void buildHelpOptions() {
        Option help = Option.builder()
                .longOpt("help")
                .desc("Display help information")
                .build();
        options.addOption(help);
    }
}
