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

package org.opengauss.portalcontroller.software;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * The interface Software.
 */
public interface Software {
    /**
     * Init critical file list array list.
     *
     * @return the array list
     */
    ArrayList<String> initCriticalFileList();

    /**
     * Init parameter hashtable hashtable.
     *
     * @return the hashtable
     */
    Hashtable<String, String> initParameterHashtable();
}
