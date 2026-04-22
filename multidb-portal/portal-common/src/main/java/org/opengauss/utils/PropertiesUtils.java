/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Properties utils
 *
 * @since 2025/4/17
 */
public class PropertiesUtils {
    /**
     * Update properties file content
     * if originalTable key exists in filePath, update value
     * if originalTable key not exists in filePath, add key and value
     *
     * @param filePath properties file path
     * @param updateParams update params
     * @throws IOException if an I/O error occurs
     */
    public static void updateProperties(String filePath, Map<String, String> updateParams) throws IOException {
        if (updateParams == null || StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("Update properties file path and updateParams cannot be null or empty");
        }

        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new FileNotFoundException("Properties file does not exist: " + filePath);
        }

        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        Set<String> processedKeys = new HashSet<>();
        List<String> newLines = new ArrayList<>();

        for (String line : lines) {
            String trimmedLine = line.trim();
            if (trimmedLine.isEmpty() || trimmedLine.startsWith("#") || trimmedLine.startsWith("!")
                    || !trimmedLine.contains("=")) {
                newLines.add(line);
                continue;
            }

            int separatorIndex = line.indexOf('=');
            String key = line.substring(0, separatorIndex).trim();

            if (updateParams.containsKey(key)) {
                String newValue = updateParams.get(key);
                String newLine = line.substring(0, separatorIndex + 1) + newValue;
                newLines.add(newLine);
                processedKeys.add(key);
            } else {
                newLines.add(line);
            }
        }

        for (Map.Entry<String, String> entry : updateParams.entrySet()) {
            if (!processedKeys.contains(entry.getKey())) {
                newLines.add(entry.getKey() + "=" + entry.getValue());
            }
        }

        Files.write(path, newLines, StandardCharsets.UTF_8);
    }

    /**
     * Comment out the specified keys in the properties file.
     *
     * @param filePath properties file path
     * @param keysToComment keys to comment out
     * @throws IOException if an I/O error occurs
     */
    public static void commentProperties(String filePath, Set<String> keysToComment) throws IOException {
        if (keysToComment == null || StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("Update properties file path and keys to comment cannot be empty");
        }

        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new FileNotFoundException("Properties file does not exist: " + filePath);
        }

        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        List<String> newLines = new ArrayList<>();

        for (String line : lines) {
            String trimmedLine = line.trim();
            if (trimmedLine.isEmpty() || trimmedLine.startsWith("#") || trimmedLine.startsWith("!")
                    || !trimmedLine.contains("=")) {
                newLines.add(line);
                continue;
            }

            int separatorIndex = line.indexOf('=');
            String key = line.substring(0, separatorIndex).trim();

            if (keysToComment.contains(key)) {
                newLines.add("#" + line);
            } else {
                newLines.add(line);
            }
        }

        Files.write(path, newLines, StandardCharsets.UTF_8);
    }

    /**
     * Write changeParams file content, overwrite all content
     *
     * @param filePath changeParams file path
     * @param changeParams changeParams
     * @throws IOException if an I/O error occurs
     */
    public static void writeProperties(String filePath, Map<String, String> changeParams)
            throws IOException {
        if (changeParams == null || StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("Write changeParams file path and changeParams cannot be null or empty");
        }

        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : changeParams.entrySet()) {
            content.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(content.toString());
        }
    }

    /**
     * Read properties file content
     *
     * @param filePath properties file path
     * @return Properties
     * @throws IOException if an I/O error occurs
     */
    public static Properties readProperties(String filePath) throws IOException {
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("Read properties file path cannot be null or empty");
        }

        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            properties.load(fis);
        }
        return properties;
    }

    /**
     * Read properties file content as map
     *
     * @param filePath properties file path
     * @return Map<String, String>
     * @throws IOException if an I/O error occurs
     */
    public static Map<String, String> readPropertiesAsMap(String filePath) throws IOException {
        Properties properties = readProperties(filePath);

        Map<String, String> result = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            result.put(key, properties.getProperty(key));
        }
        return result;
    }
}
