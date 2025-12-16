/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.database.connect.info;

import lombok.Data;
import org.opengauss.exceptions.ConfigException;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * ElasticsearchConnectInfo
 *
 * @since 2025/10/20
 */
@Data
public class ElasticsearchConnectInfo extends DatabaseConnectInfo {
    private String host;

    public ElasticsearchConnectInfo(String host, String username, String password) {
        super();

        URL url = parseElasticsearchHost(host);
        setIp(url.getHost());
        setPort(String.valueOf(url.getPort()));
        setUsername(username);
        setPassword(password);

        this.host = host;
    }

    private static URL parseElasticsearchHost(String host) {
        try {
            return new URL(host);
        } catch (MalformedURLException e) {
            throw new ConfigException("Invalid Elasticsearch host: " + host);
        }
    }

    @Override
    public String getDatabaseName() {
        throw new UnsupportedOperationException("Elasticsearch does not have a database name");
    }
}
