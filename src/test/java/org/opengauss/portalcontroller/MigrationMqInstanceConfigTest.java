/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller;

import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;
import org.opengauss.portalcontroller.constant.Parameter;
import org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig;

import static org.opengauss.portalcontroller.entity.MigrationConfluentInstanceConfig.getSystemParamAndParseEntity;

/**
 * MigrationMQInstanceConfigTest
 *
 * @author: www
 * @date: 2023/11/28 12:15
 * @description: msg
 * @since: 1.1
 * @version: 1.1
 */
public class MigrationMqInstanceConfigTest {
    /**
     * getSystemParamAndParseEntityTest
     *
     * @author: www
     * @date: 2023/11/28 12:15
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    @Test
    public void getSystemParamAndParseEntityTest() {
        MigrationConfluentInstanceConfig build =
                MigrationConfluentInstanceConfig.builder().id(1).installDir("/usr1").zkIp("2181").build();
        String param = JSON.toJSONString(build);
        System.setProperty(Parameter.ThirdPartySoftwareInstanceParam.THIRD_PARTY_SOFTWARE_INSTANCE_PARAM, param);
        MigrationConfluentInstanceConfig systemParamAndParseEntity = getSystemParamAndParseEntity();
        assert systemParamAndParseEntity != null;
    }
}
