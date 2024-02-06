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

package org.opengauss.portalcontroller;

import org.jdom2.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.portalcontroller.utils.XmlUtils;

import java.io.File;
import java.util.Optional;

import static org.opengauss.portalcontroller.utils.XmlUtils.getLog4j2Properties;

@ExtendWith(MockitoExtension.class)
class XmlUtilsTest {
    private String log4j2Path;

    @BeforeEach
    public void init() {
        String rootPath = System.getProperty("user.dir");
        log4j2Path =
                rootPath + File.separator + "portal" + File.separator + "config" + File.separator + "datacheck"
                        + File.separator + "log4j2.xml";
    }

    @Test
    void testLoadXml() {
        // Run the test
        final Optional<Document> result = XmlUtils.loadXml(log4j2Path);
        // Verify the results
        Assertions.assertNotEquals(result.get(), Optional.empty());
    }

    @Test
    void testLoadXml_nullpath() {
        // Run the test
        final Optional<Document> result = XmlUtils.loadXml("");
        // Verify the results
        Assertions.assertEquals(result, Optional.empty());
    }

    @Test
    void testGetLog4j2Properties() {
        final Optional<Document> result = XmlUtils.loadXml(log4j2Path);
        Optional<String> logLevel = getLog4j2Properties("name", result.get());
        Assertions.assertEquals(logLevel.get(), "INFO");
    }

    @Test
    void testGetLog4j2Properties_nullName() {
        final Optional<Document> result = XmlUtils.loadXml(log4j2Path);
        Optional<String> logLevel = getLog4j2Properties("", result.get());
        Assertions.assertEquals(logLevel, Optional.empty());
    }
}
