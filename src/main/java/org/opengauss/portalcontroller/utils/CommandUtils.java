/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NecessaryFileCheckStatusNode
 *
 * @date :2023/9/19 16:22
 * @description: CommandUtils
 * @version: 1.1
 * @since 1.1
 */
public class CommandUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandUtils.class);

    /**
     * Combain order string.
     *
     * @param parts the parts
     * @return the string
     */
    public static String combineOrder(String[] parts) {
        StringBuilder path;
        path = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            path.append(" ").append(parts[i]);
        }

        return path.toString();
    }

    /**
     * Contain string boolean.
     *
     * @param order the order
     * @param key   the key
     * @return the boolean
     */
    public static boolean containString(String order, String key) {
        String[] orderPart = order.split(" ");
        for (String part : orderPart) {
            if (part.equals(key)) {
                return true;
            }
        }
        return false;
    }
}
