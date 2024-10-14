/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

package org.opengauss.portalcontroller.verify.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Opengauss;
import org.opengauss.portalcontroller.utils.MigrationParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * pg_hba.conf config model
 *
 * @since 2024-10-11
 */
@Data
public class HbaConfModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaConfModel.class);

    private String type;
    private String database;
    private String user;
    private String address;
    private String method;

    private CheckStatus checkStatus = CheckStatus.NO_PRIVILEGES;

    /**
     * Check status of a record in pg_hba.conf
     *
     * @since 2024-10-11
     */
    @Getter
    @AllArgsConstructor
    public enum CheckStatus {
        NO_PRIVILEGES("The user does not hava replication connection privilegs", 1),
        TYPE_NO_SUPPORT(
                "The connection type is not supported. Supported type: host or hostnossl.", 2),
        ADDRESS_NOT_SUPPORT(
                "The user does not hava the replication connection privileges for the kafka ip address.", 3),
        METHOD_NO_SUPPORT(
                "The authentication method is not supported. Supported type: sha256 or md5.", 4);

        private final String description;
        private final int code;
    }

    /**
     * has replication privileges
     *
     * @return boolean
     */
    public boolean hasReplicationPrivileges() {
        return checkDatabase() && checkUser();
    }

    /**
     * check hba conf
     *
     * @return boolean
     */
    public boolean checkHbaConf() {
        return checkType() && checkAddress() && checkMethod();
    }

    private boolean checkDatabase() {
        return database.equals("replication");
    }

    private boolean checkUser() {
        String[] users = user.split(",");
        return Arrays.stream(users).anyMatch(userName -> userName.equals("all")
                        || userName.equals(PortalControl.toolsMigrationParametersTable.get(Opengauss.USER)));
    }

    private boolean checkType() {
        if (type.equals("host") || type.equals("hostnossl")) {
            return true;
        }

        checkStatus = CheckStatus.TYPE_NO_SUPPORT;
        return false;
    }

    private boolean checkAddress() {
        // If whitelist type is ipv6.
        if (address.contains(":")) {
            checkStatus = CheckStatus.ADDRESS_NOT_SUPPORT;
            return false;
        }

        String kafkaIp = MigrationParamUtils.getKafkaIp();
        // Check whether kafka ip address is in whitelist. For example: 10.10.0.0/24.
        if (address.contains("/") && isInWhitelistWithCIDR(kafkaIp, address)) {
            return true;
        }

        // Check whether kafka ip address is in whitelist. For example: 10.10.0.0 255.255.255.0.
        if (address.contains(" ") && isInWhitelistWithSubnetMask(kafkaIp, address)) {
            return true;
        }

        checkStatus = CheckStatus.ADDRESS_NOT_SUPPORT;
        return false;
    }

    private boolean isInWhitelistWithCIDR(String ip, String whitelistEntry) {
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            String[] parts = whitelistEntry.split("/");
            String whitelistIP = parts[0];
            int subnetMask = Integer.parseInt(parts[1]);

            InetAddress whitelistAddress = InetAddress.getByName(whitelistIP);
            byte[] addressBytes = ipAddress.getAddress();
            byte[] whitelistBytes = whitelistAddress.getAddress();

            for (int i = 0; i < subnetMask / 8; i++) {
                if (addressBytes[i] != whitelistBytes[i]) {
                    return false;
                }
            }

            int remainingBits = subnetMask % 8;
            if (remainingBits > 0) {
                int mask = 0xFF << (8 - remainingBits);
                if ((addressBytes[subnetMask / 8] & mask) != (whitelistBytes[subnetMask / 8] & mask)) {
                    return false;
                }
            }

            return true;
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to verify the whitelist. Error: ", e);
            return false;
        }
    }

    private boolean isInWhitelistWithSubnetMask(String ip, String whitelistEntry) {
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);

            String[] parts = whitelistEntry.split(" ");
            String networkAddress = parts[0];
            String subnetMask = parts[1];

            InetAddress networkAddressIp = InetAddress.getByName(networkAddress);
            byte[] networkBytes = networkAddressIp.getAddress();
            byte[] subnetBytes = InetAddress.getByName(subnetMask).getAddress();
            byte[] targetBytes = ipAddress.getAddress();

            boolean isMatch = true;
            for (int i = 0; i < networkBytes.length; i++) {
                if ((networkBytes[i] & subnetBytes[i]) != (targetBytes[i] & subnetBytes[i])) {
                    isMatch = false;
                    break;
                }
            }

            if (isMatch) {
                return true;
            }
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to verify the whitelist. Error: ", e);
        }
        return false;
    }

    private boolean checkMethod() {
        if (method.equals("sha256") || method.equals("md5")) {
            return true;
        }

        checkStatus = CheckStatus.METHOD_NO_SUPPORT;
        return false;
    }
}
