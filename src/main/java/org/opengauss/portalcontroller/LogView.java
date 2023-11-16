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

import org.opengauss.portalcontroller.constant.Check;
import org.opengauss.portalcontroller.exception.PortalException;
import org.opengauss.portalcontroller.logmonitor.listener.LogFileListener;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The type Log view.
 */
public class LogView {
    private static Map<String, AtomicLong> LOG_SEEK_POS_CACHE = new ConcurrentHashMap<>();
    private static int MAX_LOG_SIZE = 200;
    private static int MAX_CHECK_LOG_SUCCESS_FLAG_LENGTH = 10000;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LogView.class);

    /**
     * Gets error msg.
     *
     * @param logPath the log path
     * @return the error msg
     */
    public static String getErrorMsg(String logPath) {
        return getLog(logPath, List.of(Check.CheckLog.EXCEPTION, Check.CheckLog.ERR));
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
            PortalException portalException = new PortalException("IO exception", "getting error message in file " + logPath, e.getMessage());
            LOGGER.error(portalException.toString());
            Tools.shutDownPortal(portalException.toString());
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
            PortalException portalException = new PortalException("IO exception", "getting error message in file " + logPath, e.getMessage());
            LOGGER.error("", e);
            Tools.shutDownPortal(portalException.toString());
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
            PortalException portalException = new PortalException("IO exception", "getting error message in file " + logPath, e.getMessage());
            LOGGER.error("", e);
            Tools.shutDownPortal(portalException.toString());
        }
        return stringBuilder.toString();
    }

    private static boolean checkFileExists(String logPath) {
        try {
            File file = new File(logPath);
            return file.exists();
        } catch (Exception ex) {
            LOGGER.error("logPath not exists ", ex);
            Tools.shutDownPortal("logPath not exists :" + logPath);
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
        return getTailLog(logPath, List.of("check task execute success ,cost time ="), MAX_CHECK_LOG_SUCCESS_FLAG_LENGTH).length() > 1;
    }

    /**
     * Check start sign flag boolean.
     *
     * @param logPath     the log path
     * @param startSign   the start sign
     * @param timestamp   the timestamp
     * @param logListener  the LogFileListener
     * @return the boolean
     * @throws PortalException the portal exception
     */
    public static boolean checkStartSignFlag(String logPath, String startSign, long timestamp,
                                             LogFileListener logListener) throws PortalException {
        boolean flag = false;
        HashMap<String, String> logMap = logListener.getLogMap();
        if (!logMap.containsKey(startSign)) {
            return false;
        }
        String[] successStrArray = logMap.get(startSign).split(System.lineSeparator());
        for (String singleSuccessStr : successStrArray) {
            String[] strParts = singleSuccessStr.split(" ");
            try {
                String timeStr = strParts[0] + " " + strParts[1].substring(0, strParts[1].lastIndexOf("."));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = sdf.parse(timeStr);
                long currentTimeStamp = date.getTime();
                if (currentTimeStamp >= timestamp) {
                    flag = true;
                }
            } catch (ParseException | StringIndexOutOfBoundsException e) {
                LOGGER.warn(e.getMessage());
                LOGGER.warn("Please check LOG_PATTERN of log4j2.xml , log4j2source.xml and log4j2sink.xml");
                LOGGER.warn("The value should start with %d{yyyy-MM-dd HH:mm:ss.SSS}");
                throw new PortalException("String index out of bounds exception", "reading log in file " + logPath, e.getMessage());
            }

        }
        return flag;
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
     * Write file.
     *
     * @param stringList the string list
     * @param path       the path
     * @param append     the append
     */
    public static void writeFile(List<String> stringList, String path, boolean append) {
        StringBuilder str = new StringBuilder();
        for (String tempStr : stringList) {
            str.append(tempStr).append(System.lineSeparator());
        }
        writeFile(str.toString(), path, append);
    }

    /**
     * Write file.
     *
     * @param str    the str
     * @param path   the path
     * @param append the append
     */
    public static void writeFile(String str, String path, boolean append) {
        if (append) {
            writeAppendFile(path, str);
        } else {
            writeFileRest(path, str);
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static void writeAppendFile(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE);
        } catch (IOException e) {
            LOGGER.error("file write error:", e);
        }
    }

    /**
     * Write file rest.
     *
     * @param filename the filename
     * @param content  the content
     */
    public static void writeFileRest(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.error("file write error:", e);
        }
    }
}
