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

package org.opengauss.portalcontroller.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Parameter;

import java.util.Hashtable;

/**
 * MigrationConfluentInstanceConfig
 *
 * @author: www
 * @date: 2023/11/28 12:10
 * @description: msg
 * @since: 1.1
 * @version: 1.1
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class MigrationConfluentInstanceConfig {
    /**
     * id
     */
    private Integer id;
    @Builder.Default
    private String zookeeperPort = "2181";

    @Builder.Default
    private String kafkaPort = "9092";

    @Builder.Default
    private String zkIp = "127.0.0.1";

    @Builder.Default
    private String kafkaIp = "127.0.0.1";

    private String installDir;

    // 逗号隔开的字符串
    private String bindPortalId;

    private String zkIpPort;

    private String kafkaIpPort;

    private String schemaRegistryIpPort;

    @Builder.Default
    private String schemaRegistryIp = "127.0.0.1";

    private String schemaRegistryPort = "8081";

    private String bindPortalHost;

    private String thirdPartySoftwareConfigType;

    /**
     * ThirdPartySoftwareConfigType
     *
     * @author: www
     * @date: 2023/11/28 12:11
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    @AllArgsConstructor
    @Getter
    public enum ThirdPartySoftwareConfigType {
        BIND("1"),
        INSTALL("2");
        private String code;
    }

    /**
     * getSystemParamAndParseEntity
     *
     * @return MigrationConfluentInstanceConfig config
     * @author: www
     * @date: 2023/11/28 12:11
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public static MigrationConfluentInstanceConfig getSystemParamAndParseEntity() {
        MigrationConfluentInstanceConfig migrationConfluentInstanceConfig =
                MigrationConfluentInstanceConfig.builder().zookeeperPort(System.getProperty("zookeeperPort"))
                        .zkIp(System.getProperty("zkIp"))
                .kafkaIp(System.getProperty("kafkaIp")).kafkaPort(System.getProperty("kafkaPort"))
                .schemaRegistryIp(System.getProperty("schemaRegistryIp")).schemaRegistryPort(System.getProperty(
                        "schemaRegistryPort"))
                .thirdPartySoftwareConfigType(System.getProperty("thirdPartySoftwareConfigType"))
                        .installDir(System.getProperty("installDir")).build();

        log.info("get MigrationConfluentInstanceConfig from system param = {}", migrationConfluentInstanceConfig);
        return migrationConfluentInstanceConfig;
    }

    /**
     * check necessary params
     *
     * @return boolean
     */
    public boolean checkNecessaryParams() {
        if (Strings.isBlank(this.thirdPartySoftwareConfigType)) {
            return false;
        }
        if (Strings.isBlank(this.kafkaIp)) {
            return false;
        }
        if (Strings.isBlank(this.zkIp)) {
            return false;
        }
        if (Strings.isBlank(this.schemaRegistryIp)) {
            return false;
        }
        if (Strings.isBlank(this.kafkaPort)) {
            return false;
        }
        if (Strings.isBlank(this.zookeeperPort)) {
            return false;
        }

        if (Strings.isBlank(this.schemaRegistryPort)) {
            return false;
        }
        return true;
    }


    /**
     * getInstanceFromPortalConfig
     *
     * @return MigrationConfluentInstanceConfig config
     * @author: www
     * @date: 2023/11/28 12:11
     * @description: msg
     * @since: 1.1
     * @version: 1.1
     */
    public static MigrationConfluentInstanceConfig getInstanceFromPortalConfig() {
        String configPath = PathUtils.combainPath(true, PortalControl.portalControlPath + "config",
                "migrationConfig.properties");
        Hashtable<String, String> portalConfig = Tools.getPropertiesParameters(configPath);
        return MigrationConfluentInstanceConfig.builder().zkIpPort(portalConfig.get(Parameter.Port.ZOOKEEPER))
                .kafkaIpPort(portalConfig.get(Parameter.Port.KAFKA))
                .schemaRegistryIpPort(portalConfig.get(Parameter.Port.SCHEMA_REGISTRY)).build();
    }
}
