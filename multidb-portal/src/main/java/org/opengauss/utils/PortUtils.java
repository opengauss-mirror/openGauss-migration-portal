/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.IOException;
import java.net.SocketException;

/**
 * Port utils
 *
 * @since 2025/4/17
 */
public class PortUtils {
    /**
     * Check if the specified TCP port is available
     *
     * @param port port number
     * @return true if the port is available, false otherwise
     */
    public static boolean isTcpPortCanUse(int port) {
        if (port < 1024 || port > 65535) {
            throw new IllegalArgumentException("Invalid port number: " + port);
        }

        try {
            ServerSocketFactory serverSocketFactory = SSLServerSocketFactory.getDefault();
            if (serverSocketFactory instanceof SSLServerSocketFactory) {
                SSLServerSocketFactory sslServerSocketFactory = (SSLServerSocketFactory) serverSocketFactory;
                sslServerSocketFactory.createServerSocket(port).close();
                return true;
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    /**
     * Get a usable TCP port
     *
     * @param expectPort expect port number
     * @return a usable TCP port
     * @throws SocketException if no available port is found
     */
    public static int getUsefulPort(int expectPort) throws SocketException {
        if (expectPort < 1024 || expectPort > 65535) {
            throw new IllegalArgumentException("Invalid port number: " + expectPort);
        }

        for (int port = expectPort; port < 65535; port++) {
            if (isTcpPortCanUse(port)) {
                return port;
            }
        }
        throw new SocketException("No available port found.");
    }
}
