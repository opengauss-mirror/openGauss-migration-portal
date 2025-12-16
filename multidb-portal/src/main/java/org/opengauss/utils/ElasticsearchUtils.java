/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * ElasticsearchUtils
 *
 * @since 2025/10/29
 */
public class ElasticsearchUtils {
    private static final Logger LOGGER = LogManager.getLogger(ElasticsearchUtils.class);

    /**
     * Create an Elasticsearch REST client without authentication.
     *
     * @param ip the Elasticsearch ip
     * @param port the Elasticsearch port
     * @return the Elasticsearch REST client
     */
    public static RestClient createRestClient(String ip, int port) {
        return createRestClient(ip, port, null, null);
    }

    /**
     * Create an Elasticsearch REST client.
     *
     * @param ip the Elasticsearch ip
     * @param port the Elasticsearch port
     * @param username the Elasticsearch username (optional)
     * @param password the Elasticsearch password (optional)
     * @return the Elasticsearch REST client
     */
    public static RestClient createRestClient(String ip, int port, String username, String password) {
        if (ip == null || ip.isEmpty()) {
            throw new IllegalArgumentException("IP cannot be null or empty");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Port must be a positive integer");
        }

        RestClientBuilder builder = RestClient.builder(new HttpHost(ip, port, "http"));

        if (username != null && password != null) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                );
        }

        return builder.build();
    }

    /**
     * Get the health status code of the Elasticsearch cluster.
     *
     * @param restClient the Elasticsearch REST client to test
     * @return the health status code of the Elasticsearch cluster
     * @throws IOException if an I/O error occurs while testing the connection
     */
    public static int healthStatusCode(RestClient restClient) throws IOException {
        if (restClient == null) {
            throw new IllegalArgumentException("RestClient cannot be null");
        }

        Request request = new Request("GET", "/_cluster/health");
        Response response = restClient.performRequest(request);
        return response.getStatusLine().getStatusCode();
    }

    /**
     * Get the health status code of the Elasticsearch cluster.
     *
     * @param ip the Elasticsearch ip
     * @param port the Elasticsearch port
     * @return the health status code of the Elasticsearch cluster
     * @throws IOException if an I/O error occurs while testing the connection
     */
    public static int healthStatusCode(String ip, int port) throws IOException {
        return healthStatusCode(ip, port, null, null);
    }

    /**
     * Get the health status code of the Elasticsearch cluster.
     *
     * @param ip the Elasticsearch ip
     * @param port the Elasticsearch port
     * @param username the Elasticsearch username (optional)
     * @param password the Elasticsearch password (optional)
     * @return the health status code of the Elasticsearch cluster
     * @throws IOException if an I/O error occurs while testing the connection
     */
    public static int healthStatusCode(String ip, int port, String username, String password) throws IOException {
        RestClient restClient = null;
        try {
            restClient = createRestClient(ip, port, username, password);
            return healthStatusCode(restClient);
        } finally {
            closeClient(restClient);
        }
    }

    /**
     * Get the version number of the Elasticsearch cluster.
     *
     * @param restClient the Elasticsearch REST client to test
     * @return the version number of the Elasticsearch cluster
     * @throws IOException if an I/O error occurs while testing the connection
     */
    public static String getElasticsearchVersion(RestClient restClient) throws IOException {
        Request request = new Request("GET", "/");
        Response response = restClient.performRequest(request);

        String responseBody = getResponseBody(response);
        if (responseBody.contains("\"number\" : \"")) {
            int startIndex = responseBody.indexOf("\"number\" : \"") + "\"number\" : \"".length();
            int endIndex = responseBody.indexOf("\"", startIndex);
            return responseBody.substring(startIndex, endIndex);
        }

        LOGGER.error("Failed to extract version number from response body: {}", responseBody);
        throw new IOException("Failed to extract version number from response body");
    }

    /**
     * Close the Elasticsearch REST client.
     *
     * @param restClient the Elasticsearch REST client to close
     * @throws IOException if an I/O error occurs while closing the client
     */
    public static void closeClient(RestClient restClient) throws IOException {
        if (restClient == null) {
            return;
        }

        restClient.close();
    }

    private static String getResponseBody(Response response) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
