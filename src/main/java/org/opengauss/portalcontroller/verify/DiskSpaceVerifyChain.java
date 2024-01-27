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

package org.opengauss.portalcontroller.verify;

import com.alibaba.fastjson.util.IOUtils;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.portalcontroller.JdbcTools;
import org.opengauss.portalcontroller.PathUtils;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.Tools;
import org.opengauss.portalcontroller.constant.Chameleon;
import org.opengauss.portalcontroller.constant.Mysql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * DiskSpaceVerifyChain
 *
 * @since 1.1
 * @date :2023/11/3 15:22
 * @description: DiskSpaceVerifyChain
 * @version: 1.1
 */
public class DiskSpaceVerifyChain extends AbstractPreMigrationVerifyChain {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiskSpaceVerifyChain.class);

    @Override
    public void verify(Map<String, Object> resultMap, Connection mysqlConnection, PgConnection pgConnection) {
        Map<String, Object> diskMap = new HashMap<>();
        resultMap.put("disk_space", diskMap);
        if (mysqlConnection == null) {
            diskMap.put(Constants.KEY_RESULT, Constants.CROSS_BAR);
        } else {
            int result = readAndWrite(getMaxTableSpace(mysqlConnection, true), diskMap,
                "0".equals(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()))
                ? Constants.KEY_FLAG_TRUE
                : Constants.KEY_FLAG_FALSE;
            ;
            diskMap.put(Constants.KEY_RESULT, result);
            resultMap.put(Constants.KEY_VERIFY_RESULT_FLAG,
                Integer.parseInt(resultMap.get(Constants.KEY_VERIFY_RESULT_FLAG).toString()) | result);

            super.transfer(resultMap, mysqlConnection, pgConnection);
        }
    }

    /**
     * get single table max capacity
     *
     * @param mysqlConnection mysql connect
     * @param isDefault is default file
     * @return  single table max capacity
     */
    public static BigDecimal getMaxTableSpace(Connection mysqlConnection, boolean isDefault) {
        String permissionStr = "0";
        try {
            permissionStr = JdbcTools.selectStringValue(mysqlConnection,
                "SELECT IFNULL(MAX(DATA_LENGTH + INDEX_LENGTH + DATA_FREE),0) as total from "
                    + "information_schema.tables where TABLE_SCHEMA ='"
                    + PortalControl.toolsMigrationParametersTable.get(Mysql.DATABASE_NAME) + "'", "total");
        } catch (SQLException e) {
            LOGGER.error("sql execute failed.");
        }
        LOGGER.info("a single table is {} byte", permissionStr);
        String path;
        if (isDefault) {
            path = PathUtils.combainPath(true, PortalControl.portalWorkSpacePath + "config", "chameleon",
                "config-example.yml");
        } else {
            path = PortalControl.toolsConfigParametersTable.get(Chameleon.CONFIG_PATH);
        }
        HashMap<String, Object> chameleonConfigMap = Tools.getYmlParameters(path);
        BigDecimal read = new BigDecimal(chameleonConfigMap.get("sources.mysql.readers").toString());
        BigDecimal write = new BigDecimal(chameleonConfigMap.get("sources.mysql.writers").toString());
        LOGGER.info("read:{},write:{}", read, write);
        return new BigDecimal(permissionStr).multiply(read.add(write))
            .multiply(BigDecimal.valueOf(2))
            .divide(BigDecimal.valueOf(1024L * 1024 * 1024), 4, RoundingMode.UP);
    }

    /**
     * read and write dataCapacity,return is met
     *
     * @param diskSpace  a single table max capacity
     * @param diskMap disk Map
     * @param isWrite isWrite flag
     * @return is Met
     */
    public static boolean readAndWrite(BigDecimal diskSpace, Map<String, Object> diskMap, boolean isWrite) {
        FileLock lock = null;
        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        String result;
        try {
            File file = new File(PortalControl.portalControlPath + "dataCapacity.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            // block lock
            lock = fileChannel.lock();
            result = randomAccessFile.readLine();
            BigDecimal need = StringUtils.isEmpty(result) ? diskSpace : diskSpace.add(new BigDecimal(result));
            LOGGER.info("need:{}, tableSpace:{}", need, diskSpace);
            File diskFile = new File(PortalControl.portalControlPath);
            BigDecimal remain = BigDecimal.valueOf(diskFile.getFreeSpace())
                .divide(BigDecimal.valueOf(1024L * 1024 * 1024), 4, RoundingMode.UP);
            // remain compareTo need
            LOGGER.info("remain disk space {}G", remain);
            boolean isMet = remain.compareTo(need) >= 0;
            if (isMet && isWrite) {
                randomAccessFile.setLength(0);
                randomAccessFile.write(need.toString().getBytes(StandardCharsets.UTF_8));
            }
            if (!isMet) {
                Map<String, Object> diskErrorMap = new HashMap<>();
                diskMap.put("disk_error", diskErrorMap);
                diskErrorMap.put("remain", remain.doubleValue() + "G");
                diskErrorMap.put("need", need.doubleValue() + "G");
            }
            return isMet;
        } catch (IOException e) {
            LOGGER.error("create file or write failed.");
        } finally {
            try {
                if (lock != null) {
                    lock.close();
                }
                IOUtils.close(fileChannel);
                if (randomAccessFile != null) {
                    randomAccessFile.close();
                }
            } catch (IOException e) {
                LOGGER.error("lock release failed.");
            }
        }
        return false;
    }
}
