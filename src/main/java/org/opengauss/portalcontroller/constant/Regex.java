package org.opengauss.portalcontroller.constant;

/**
 * This interface contains regular expressions for matching IPv4 and IPv6 addresses.
 *
 * @since 2023-01-12
 */
public interface Regex {
    /**
     * The regular expression is used to match IPv4 and IPv6 addresses.
     *
     * @since 2025-01-02
     */
    String IP = "((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})"
            + "(\\.((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})){3}"
            + "|([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]{1,4}|:)"
            + "|::([0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}";
    String PORT = "^([1-9][0-9]{0,4})?$";
    String FOLDER_PATH = "^~?\\/((\\w|.|-)+\\/)+$";

    String PKG_NAME = "\\S{1,64}(.tar.gz|.tgz|.zip|.whl)";
    String NAME = "(\\w|.|-){1,64}";
    String URL = "[a-zA-z]+://[^\\s]*";

    String OFFSET_FILE = "";
    String OFFSET_GTID = "";
    String POSITION = "";

    String CHAMELEON_LOG = "^((\\d{4}(-)\\d{1,2}(-)\\d{1,2})|(\\d{4}(/)\\d{1,2}(/)\\d{1,2}))(\\s)\\d{1,2}:\\d{1,"
            + "2}:\\d{1,2}(.*)";
}
