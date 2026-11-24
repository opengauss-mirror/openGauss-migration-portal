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
