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

package org.opengauss.portalcontroller.utils;

import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LogViewUtils
 *
 * @date :2024/1/18 10:52
 * @description: LogViewUtils
 * @version: 1.1
 * @since 1.1
 */
public class LogViewUtils {
    private static Map<String, AtomicLong> LOG_SEEK_POS_CACHE = new ConcurrentHashMap<>();
    private static int MAX_LOG_SIZE = 200;
    private static int MAX_CHECK_LOG_SUCCESS_FLAG_LENGTH = 10000;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LogViewUtils.class);

    /**
     * Gets error msg.
     *
     * @param logPath the log path
     * @return the error msg
     */
    public static String getErrorMsg(String logPath) {
        return getLog(logPath, List.of(Check.CheckLog.EXCEPTION, Check.CheckLog.ERR, Check.CheckLog.ERR_UPPER));
    }

    /**
     * Gets log.
     *
     * @param logPath   the log path
     * @param targetMsg the target msg
     * @return the log
     */
    public static synchronized String getLog(String logPath, List<String> targetMsg) {
        if (!checkFileExists(logPath)) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        try {
            if (!LOG_SEEK_POS_CACHE.containsKey(logPath)) {
                LOG_SEEK_POS_CACHE.put(logPath, new AtomicLong(0));
            }
            RandomAccessFile randomFile = new RandomAccessFile(logPath, "r");
            int count = 0;
            randomFile.seek(LOG_SEEK_POS_CACHE.get(logPath).get());
            String tempStr;
            while ((tempStr = randomFile.readLine()) != null) {
                boolean hasErrorMsg = targetMsg.stream().map(tempStr::contains).anyMatch(Boolean::booleanValue);
                if (hasErrorMsg) {
                    stringBuilder.append(tempStr).append(System.lineSeparator());
                    break;
                }
                count++;
                if (count > MAX_LOG_SIZE) {
                    break;
                }
            }
            LOG_SEEK_POS_CACHE.get(logPath).set(randomFile.length());
            randomFile.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception",
                    "getting error message in file " + logPath, e.getMessage());
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
        return stringBuilder.toString();
    }

    /**
     * Gets tail log.
     *
     * @param logPath   the log path
     * @param targetMsg the target msg
     * @param prefix    the prefix
     * @return the tail log
     */
    public static String getTailLog(String logPath, List<String> targetMsg, int prefix) {
        StringBuilder stringBuilder = new StringBuilder();
        if (!checkFileExists(logPath)) {
            return "";
        }
        try {
            if (!LOG_SEEK_POS_CACHE.containsKey(logPath)) {
                LOG_SEEK_POS_CACHE.put(logPath, new AtomicLong(0));
            }
            RandomAccessFile randomFile = new RandomAccessFile(logPath, "r");
            long seek = LOG_SEEK_POS_CACHE.get(logPath).get();
            randomFile.seek(Math.max(seek - prefix, 0));
            String tempStr;
            while ((tempStr = randomFile.readLine()) != null) {
                boolean hasTargetMsg = targetMsg.stream().map(tempStr::contains).anyMatch(Boolean::booleanValue);
                if (hasTargetMsg) {
                    stringBuilder.append(tempStr).append(System.lineSeparator());
                }
            }
            LOG_SEEK_POS_CACHE.get(logPath).set(randomFile.length());
            randomFile.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception",
                    "getting error message in file " + logPath, e.getMessage());
            LOGGER.error("", e);
            PortalControl.shutDownPortal(portalException.toString());
        }
        return stringBuilder.toString();
    }

    /**
     * Gets full log.
     *
     * @param logPath the log path
     * @return the full log
     */
    public static String getFullLog(String logPath) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            if (!checkFileExists(logPath)) {
                return "";
            }
            RandomAccessFile randomFile = new RandomAccessFile(logPath, "r");
            String tempStr;
            while ((tempStr = randomFile.readLine()) != null) {
                stringBuilder.append(tempStr).append(System.lineSeparator());
            }
            randomFile.close();
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception",
                    "getting error message in file " + logPath, e.getMessage());
            LOGGER.error("", e);
            PortalControl.shutDownPortal(portalException.toString());
        }
        return stringBuilder.toString();
    }

    private static boolean checkFileExists(String logPath) {
        try {
            File file = new File(logPath);
            return file.exists();
        } catch (Exception ex) {
            LOGGER.error("logPath not exists ", ex);
            PortalControl.shutDownPortal("logPath not exists :" + logPath);
        }
        return false;
    }

    /**
     * Check check success log flag boolean.
     *
     * @param logPath the log path
     * @return the boolean
     */
    public static boolean checkCheckSuccessLogFlag(String logPath) {
        return getTailLog(logPath, List.of("check task execute success ,cost time ="),
                MAX_CHECK_LOG_SUCCESS_FLAG_LENGTH).length() > 1;
    }

    /**
     * Check start sign flag boolean.
     *
     * @param startSign   the start sign
     * @param logListener the LogFileListener
     * @return the boolean
     */
    public static boolean checkStartSignFlag(String startSign, LogFileListener logListener) {
        HashMap<String, String> logMap = logListener.getLogMap();
        return logMap.containsKey(startSign);
    }

    /**
     * Gets full log no separator.
     *
     * @param logPath the log path
     * @return the full log no separator
     */
    public static String getFullLogNoSeparator(String logPath) {
        return getFullLog(logPath).replaceAll(System.lineSeparator(), "");
    }

    /**
     * Last line string.
     *
     * @param path the path
     * @return the string
     */
    public static String lastLine(String path) {
        File file = new File(path);
        StringBuilder builder = new StringBuilder();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long fileLastPointer = randomAccessFile.length() - 1;
            for (long filePointer = fileLastPointer; filePointer != -1; filePointer--) {
                randomAccessFile.seek(filePointer);
                int readByte = randomAccessFile.readByte();
                if (0xA == readByte) {
                    if (filePointer == fileLastPointer) {
                        continue;
                    }
                    break;
                }
                if (0xD == readByte) {
                    if (filePointer == fileLastPointer - 1) {
                        continue;
                    }
                    break;
                }
                builder.append((char) readByte);
            }
        } catch (FileNotFoundException e) {
            PortalException portalException = new PortalException("File not found exception", "reading last line in " +
                    "file " + path, e.getMessage());
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        } catch (IOException e) {
            PortalException portalException = new PortalException("IO exception", "reading last line in file " + path
                    , e.getMessage());
            LOGGER.error(portalException.toString());
            PortalControl.shutDownPortal(portalException.toString());
        }
        return builder.reverse().toString();
    }

    /**
     * Output result.
     *
     * @param flag  the flag
     * @param order the order
     */
    public static void outputResult(boolean flag, String order) {
        if (flag) {
            LOGGER.info(order + " success.");
        } else {
            LOGGER.error("Error message: " + order + " failed.");
        }
    }

    /**
     * Output information.
     *
     * @param flag        the flag
     * @param trueString  the true string
     * @param falseString the false string
     */
    public static void outputInformation(boolean flag, String trueString, String falseString) {
        if (flag) {
            LOGGER.info(trueString);
        } else if (!falseString.equals("")) {
            LOGGER.error(falseString);
        }
    }
}
