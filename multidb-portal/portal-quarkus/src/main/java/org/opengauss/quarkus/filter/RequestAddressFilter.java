/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.opengauss.quarkus.filter;

import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Set;

/**
 * Request address filter
 *
 * @since 2026/9/7
 */
@Provider
public class RequestAddressFilter implements ContainerRequestFilter {
    private static final Logger LOGGER = LogManager.getLogger(RequestAddressFilter.class);
    private static final Set<String> LOCALHOST_IPS = Set.of(
            "127.0.0.1", "::1", "0:0:0:0:0:0:0:1"
    );

    @Context
    private HttpServerRequest request;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String host = requestContext.getUriInfo().getRequestUri().getHost();
        if (!isLocalHost(host)) {
            abortWithForbidden(requestContext);
            return;
        }

        String remoteAddr = getRemoteAddress(requestContext);
        if (!isLocalAddress(remoteAddr)) {
            abortWithForbidden(requestContext);
            return;
        }

        String forwardedFor = requestContext.getHeaderString("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isEmpty()) {
            String firstIp = forwardedFor.split(",")[0].trim();
            if (!isLocalAddress(firstIp)) {
                abortWithForbidden(requestContext);
            }
        }
    }

    private boolean isLocalHost(String host) {
        if (host == null) {
            return false;
        }
        return "localhost".equalsIgnoreCase(host) || LOCALHOST_IPS.contains(host);
    }

    private String getRemoteAddress(ContainerRequestContext requestContext) {
        String remoteAddress = request.remoteAddress().toString();
        String clientIp = remoteAddress.contains(":")
                ? remoteAddress.substring(0, remoteAddress.lastIndexOf(':'))
                : remoteAddress;
        if (!clientIp.isEmpty()) {
            return clientIp;
        }
        return requestContext.getUriInfo().getRequestUri().getHost();
    }

    private boolean isLocalAddress(String address) {
        if (address == null) {
            return false;
        }
        try {
            String cleanAddr = address.startsWith("/") ? address.substring(1) : address;
            InetAddress addr = InetAddress.getByName(cleanAddr);
            return addr.isLoopbackAddress() || isLocalNetworkInterface(addr);
        } catch (UnknownHostException e) {
            LOGGER.debug("Failed to resolve address {}: {}", address, e.getMessage());
            return false;
        }
    }

    private boolean isLocalNetworkInterface(InetAddress clientAddress) {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress localAddr = addresses.nextElement();
                    LOGGER.info("localAddr: {}", localAddr.getHostAddress());
                    if (localAddr.equals(clientAddress)) {
                        return true;
                    }
                }
            }
        } catch (SocketException e) {
            LOGGER.debug("Failed to enumerate network interfaces", e);
        }
        return false;
    }

    private void abortWithForbidden(ContainerRequestContext ctx) {
        ctx.abortWith(
                Response.status(Response.Status.FORBIDDEN)
                        .entity("Access denied: only localhost allowed")
                        .type(MediaType.TEXT_PLAIN)
                        .build()
        );
    }
}
