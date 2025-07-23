/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ip utils
 *
 * @since 2025/5/7
 */
public class IpUtils {
    private static final Logger LOGGER = LogManager.getLogger(IpUtils.class);

    /**
     * Represents the ipv4 protocol.
     */
    public static final String IPV4 = "ipv4";

    /**
     * Represents the ipv6 protocol.
     */
    public static final String IPV6 = "ipv6";

    /**
     * Format the IP:Port string to ensure the correct format is used in the Kafka configuration.
     *
     * @implSpec This method checks the last occurrence of the colon to separate the IP and port.
     * @apiNote The method supports both IPv4 and IPv6 formats. For IPv6, the format is [ip]:port.
     * @implNote The method assumes the input is a valid IP:Port string.
     *
     * @param ipPort The IP:Port character string
     * @return The Kafka server address is formatted as [ip]:port for ipv6 and ip:port for ipv4
     * @throws UnknownHostException If the IP address cannot be resolved
     */
    public static String formatIpPort(String ipPort) {
        int colonIndex = ipPort.lastIndexOf(":");
        if (colonIndex == -1) {
            LOGGER.warn("{} is not a valid parameter.", ipPort);
            return "";
        }
        String ip = ipPort.substring(0, colonIndex);
        String port = ipPort.substring(colonIndex + 1);

        if (IPV6.equals(getIpType(ip))) {
            return "[" + ip + "]:" + port;
        } else if (IPV4.equals(getIpType(ip))) {
            return ip + ":" + port;
        } else {
            LOGGER.warn("{} is not a valid IP address.", ip);
            return "";
        }
    }

    /**
     * Get the IP type of the input IP address.
     *
     * @param ip ip
     * @return String
     */
    public static String getIpType(String ip) {
        try {
            InetAddress inetAddress = InetAddress.getByName(ip);
            if (inetAddress instanceof Inet4Address) {
                return IPV4;
            } else if (inetAddress instanceof Inet6Address) {
                return IPV6;
            } else {
                LOGGER.warn("{} is neither an IPv4 nor an IPv6 address.", ip);
            }
        } catch (UnknownHostException e) {
            LOGGER.warn("{} is not a valid IP address.", ip);
        }
        return "";
    }
}
