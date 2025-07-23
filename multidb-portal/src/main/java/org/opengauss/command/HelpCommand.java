/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command;

import org.apache.commons.cli.HelpFormatter;
import org.opengauss.command.parser.CommandParser;

/**
 * help command
 *
 * @since 2025/3/26
 */
public class HelpCommand implements Command {
    public HelpCommand() {
    }

    @Override
    public void execute() {
        new HelpFormatter().printHelp("数据迁移工具", new CommandParser().getOptions());
    }
}
