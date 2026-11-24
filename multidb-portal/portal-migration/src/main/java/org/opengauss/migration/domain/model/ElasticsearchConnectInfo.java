/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-migration-portal is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.migration.domain.model;

import lombok.Data;
import org.opengauss.migration.exceptions.ConfigException;

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
