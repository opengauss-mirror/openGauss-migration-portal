/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller;

import java.io.File;

/**
 * The type Path utils.
 */
public class PathUtils {
    /**
     * Combain path string.
     *
     * @param isFile isfile
     * @param parts  the parts
     * @return the string
     */
    public static String combainPath(boolean isFile, String... parts) {
        StringBuilder path;
        path = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            path.append(File.separator).append(parts[i]);
        }
        if (!isFile) {
            path.append(File.separator);
        }
        return path.toString();
    }
}
