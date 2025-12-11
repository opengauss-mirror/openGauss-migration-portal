/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ini file utils
 * Parse the configuration items Ini according to the configuration method of properties.
 *
 * @since 2025/10/22
 */
public class IniUtils {
    private static final Pattern SECTION_PATTERN = Pattern.compile("^\\[(.+)\\]$");
    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("^([^=]+)=(.*)$");

    private IniUtils() {
    }

    /**
     * Load the configuration items in the Ini file into a Map.
     * Map key=value type: section.key=value
     *
     * @param filePath Ini file path
     * @return Map of configuration items
     * @throws IOException If an I/O error occurs
     */
    public static Map<String, String> loadIniAsMap(String filePath) throws IOException {
        Map<String, String> configMap = new LinkedHashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String keyValue;
            String currentSection = "";

            while ((keyValue = reader.readLine()) != null) {
                keyValue = keyValue.trim();

                if (keyValue.isEmpty() || keyValue.startsWith("#") || keyValue.startsWith(";")) {
                    continue;
                }

                Matcher matcher = SECTION_PATTERN.matcher(keyValue);
                if (matcher.matches()) {
                    currentSection = matcher.group(1).trim();
                    continue;
                }

                Matcher kvMatcher = KEY_VALUE_PATTERN.matcher(keyValue);
                if (kvMatcher.matches()) {
                    String key = kvMatcher.group(1).trim();
                    String value = kvMatcher.group(2).trim();

                    String configKey = currentSection.isEmpty() ? key : currentSection + "." + key;
                    configMap.put(configKey, value);
                }
            }
        }

        return configMap;
    }

    /**
     * Save the configuration items in the Map to the Ini file.
     * Map key=value type: section.key=value
     *
     * @param configMap Map of configuration items
     * @param filePath Ini file path
     * @throws IOException If an I/O error occurs
     */
    public static void saveMapAsIni(Map<String, String> configMap, String filePath) throws IOException {
        Map<String, Map<String, String>> sectionMap = new LinkedHashMap<>();
        sectionMap.put("", new HashMap<String, String>());

        for (String key : configMap.keySet()) {
            String value = configMap.get(key);

            int dotIndex = key.indexOf(".");
            if (dotIndex == -1) {
                sectionMap.get("").put(key, value);
            } else {
                String section = key.substring(0, dotIndex);
                String actualKey = key.substring(dotIndex + 1);

                sectionMap.computeIfAbsent(section, k -> new HashMap<String, String>()).put(actualKey, value);
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<String, Map<String, String>> entry : sectionMap.entrySet()) {
                String section = entry.getKey();
                Map<String, String> sectionConfigs = entry.getValue();

                if (!section.isEmpty() && !sectionConfigs.isEmpty()) {
                    writer.write("[" + section + "]");
                    writer.newLine();
                }

                for (String key : sectionConfigs.keySet()) {
                    writer.write(key + "=" + sectionConfigs.get(key));
                    writer.newLine();
                }

                if (!sectionConfigs.isEmpty()) {
                    writer.newLine();
                }
            }
        }
    }

    /**
     * Change the configuration items in the Ini file.
     * Map key=value type: section.key=value
     *
     * @param configMap Map of configuration items
     * @param filePath Ini file path
     * @throws IOException If an I/O error occurs
     */
    public static void changeIniConfig(Map<String, String> configMap, String filePath) throws IOException {
        Map<String, String> originalConfigMap = loadIniAsMap(filePath);
        originalConfigMap.putAll(configMap);
        saveMapAsIni(originalConfigMap, filePath);
    }
}
