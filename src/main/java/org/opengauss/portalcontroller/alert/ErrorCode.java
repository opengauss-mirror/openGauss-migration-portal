/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.portalcontroller.alert;

import lombok.Getter;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * error code
 *
 * @since 2024/11/27
 */
@Getter
public enum ErrorCode {
    UNKNOWN(5000, "未知异常", "Unknown error"),

    INCORRECT_CONFIGURATION(5100, "参数配置错误", "There is an error in the parameter configuration"),
    INVALID_COMMAND(5101, "无效的命令", "Invalid command"),
    LOAD_CONFIGURATION_ERROR(5103, "加载配置信息时发生错误", "Failed to load the configuration"),
    MIGRATION_CONDITIONS_NOT_MET(5104, "迁移条件不满足", "Migration conditions are not met"),
    MIGRATION_ENVIRONMENT_NOT_MET(5105, "迁移环境不满足", "Migration environment are not met"),

    IO_EXCEPTION(5200, "IO异常", "IO exception"),
    UNKNOWN_HOST(5201, "未知服务器地址", "Unknown host address"),

    SQL_EXCEPTION(5300, "SQL执行失败", "SQL execution failed"),

    COMMAND_EXECUTION_FAILED(5400, "Linux命令执行失败", "The linux command failed to execute"),

    LACK_RESOURCE(5500, "所需资源缺失", "Lack of required resources"),
    FILE_NOT_FOUND(5501, "文件未找到", "File not found"),

    PROCESS_EXITS_ABNORMALLY(5600, "进程异常退出", "The process exits abnormally"),
    KAFKA_SERVER_EXCEPTION(5601, "Kafka服务异常", "The Kafka server is abnormal"),
    DATA_CHECK_PROCESS_EXITS_ABNORMALLY(
            5602, "数据校验进程异常退出", "The data-check process exits abnormally"),

    KAFKA_CONNECT_ABNORMALLY(5700, "kafka连接异常", "The kafka connect is abnormal"),

    MIGRATION_PROCESS_FUNCTION_ABNORMALLY(
            5800, "迁移进程功能异常", "The migration process function is abnormal");

    private final int code;
    private final String causeCn;
    private final String causeEn;

    ErrorCode(int code, String causeCn, String causeEn) {
        this.code = code;
        this.causeCn = causeCn;
        this.causeEn = causeEn;
    }

    @Override
    public String toString() {
        return getErrorPrefix();
    }

    /**
     * get error prefix
     *
     * @return String error prefix
     */
    public String getErrorPrefix() {
        return String.format(Locale.ROOT, "<CODE:%d> ", code);
    }

    /**
     * get code causeCn map
     *
     * @return Map code causeCn map
     */
    public static Map<Integer, String> getCodeCauseCnMap() {
        HashMap<Integer, String> result = new HashMap<>();
        for (ErrorCode value : values()) {
            result.put(value.code, value.getCauseCn());
        }
        return result;
    }

    /**
     * get code causeEn map
     *
     * @return Map code causeEn map
     */
    public static Map<Integer, String> getCodeCauseEnMap() {
        HashMap<Integer, String> result = new HashMap<>();
        for (ErrorCode value : values()) {
            result.put(value.code, value.getCauseEn());
        }
        return result;
    }
}
