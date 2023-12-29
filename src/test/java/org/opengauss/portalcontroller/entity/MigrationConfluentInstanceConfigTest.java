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

package org.opengauss.portalcontroller.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.utils.PathUtils;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
class MigrationConfluentInstanceConfigTest {
    private static final String ZOOKEEPER_PORT = "1111";
    private static final String ZK_IP = "127.0.0.1";
    private static final String KAFKA_IP = "127.0.0.2";
    private static final String KAFKA_PORT = "2222";
    private static final String SCHEMA_REGISTRY_IP = "127.0.0.3";
    private static final String SCHEMA_REGISTRY_PORT = "3333";
    private static final String THIRD_PARTY_SOFTWARE_CONFIG_TYPE = "1";
    private static final String INSTALL_DIR = "/opt";
    private static final String ZOOKEEPER_IP_PORT = "127.0.0.1:2181";
    private static final String KAFKA_IP_PORT = "127.0.0.1:9092";
    private static final String CONFLUENT_IP_PORT = "127.0.0.1:8081";

    @BeforeEach
    void setUp() {
        System.setProperty("zookeeperPort", ZOOKEEPER_PORT);
        System.setProperty("zkIp", ZK_IP);
        System.setProperty("kafkaIp", KAFKA_IP);
        System.setProperty("kafkaPort", KAFKA_PORT);
        System.setProperty("schemaRegistryIp", SCHEMA_REGISTRY_IP);
        System.setProperty("schemaRegistryPort", SCHEMA_REGISTRY_PORT);
        System.setProperty("thirdPartySoftwareConfigType", THIRD_PARTY_SOFTWARE_CONFIG_TYPE);
        System.setProperty("installDir", INSTALL_DIR);
    }

    @Test
    void testGetSystemParamAndParseEntity() {
        // Run the test
        final MigrationConfluentInstanceConfig result = MigrationConfluentInstanceConfig.getSystemParamAndParseEntity();
        assertThat(result.getZookeeperPort()).isEqualTo(ZOOKEEPER_PORT);
        assertThat(result.getKafkaPort()).isEqualTo(KAFKA_PORT);
        assertThat(result.getZkIp()).isEqualTo(ZK_IP);
        assertThat(result.getKafkaIp()).isEqualTo(KAFKA_IP);
        assertThat(result.getInstallDir()).isEqualTo(INSTALL_DIR);
        assertThat(result.getSchemaRegistryIp()).isEqualTo(SCHEMA_REGISTRY_IP);
        assertThat(result.getSchemaRegistryPort()).isEqualTo(SCHEMA_REGISTRY_PORT);
        assertThat(result.getThirdPartySoftwareConfigType()).isEqualTo(THIRD_PARTY_SOFTWARE_CONFIG_TYPE);
    }

    @Test
    void testCheckNecessaryParams() {
        final MigrationConfluentInstanceConfig result = MigrationConfluentInstanceConfig.getSystemParamAndParseEntity();
        assertThat(result.checkNecessaryParams()).isTrue();
        MigrationConfluentInstanceConfig result2 = MigrationConfluentInstanceConfig.builder().build();
        assertThat(result2.checkNecessaryParams()).isFalse();
    }

    @Test
    void testGetInstanceFromPortalConfig() {
        try (MockedStatic<PathUtils> pathUtilsMockedStatic = Mockito.mockStatic(PathUtils.class)) {
            String projectPath = System.getProperty("user.dir");
            String portalConfigPath =
                    projectPath + File.separator + "portal" + File.separator + "config" + File.separator +
                            "migrationConfig.properties";
            pathUtilsMockedStatic.when(() -> PathUtils
                    .combainPath(true, PortalControl.portalControlPath + "config",
                            "migrationConfig.properties")).thenReturn(portalConfigPath);
            MigrationConfluentInstanceConfig result = MigrationConfluentInstanceConfig.getInstanceFromPortalConfig();
            Assertions.assertEquals(result.getZkIpPort(), ZOOKEEPER_IP_PORT);
            Assertions.assertEquals(result.getKafkaIpPort(), KAFKA_IP_PORT);
            Assertions.assertEquals(result.getSchemaRegistryIpPort(), CONFLUENT_IP_PORT);
        }
    }
}
