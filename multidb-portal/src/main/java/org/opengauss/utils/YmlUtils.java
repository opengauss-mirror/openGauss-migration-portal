/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * yml utils
 *
 * @since 2025/4/30
 */
public class YmlUtils {
    /**
     * Loads a YAML file from an external file path into a Map<String, Object>.
     *
     * @param filePath yaml file path
     * @return Map<String, Object> containing the key-value pairs from the YAML file.
     * @throws IOException if an I/O error occurs
     */
    public static Map<String, Object> loadYaml(String filePath) throws IOException {
        Map<String, Object> rawMap = readYamlFile(filePath);
        return flattenYamlMap(rawMap);
    }

    /**
     * Updates the content of a YAML file with the provided key-value pairs.
     * If the key already exists, it will be overwritten.
     * If the key does not exist, it will be added.
     *
     * @param filePath YAML file path
     * @param updateParams the updates key-value pairs
     * @throws IOException if an I/O error occurs
     */
    public static void updateYaml(String filePath, Map<String, Object> updateParams) throws IOException {
        if (updateParams == null) {
            throw new IllegalArgumentException("Updates map cannot be null");
        }
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }
        if (updateParams.isEmpty()) {
            return;
        }

        Map<String, Object> yamlData = readYamlFile(filePath);
        for (Map.Entry<String, Object> entry : updateParams.entrySet()) {
            String keyPath = entry.getKey();
            Object value = entry.getValue();

            List<String> keys = Arrays.asList(keyPath.split("\\."));

            updateNestedMap(yamlData, keys, value);
        }

        writeYamlFile(filePath, yamlData);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readYamlFile(String fileName) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream inputStream = new FileInputStream(fileName)) {
            return yaml.load(inputStream);
        }
    }

    private static void writeYamlFile(String fileName, Map<String, Object> data) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);

        Representer representer = new Representer(options) {
            {
                this.nullRepresenter = data -> representScalar(Tag.NULL, "null");
            }
        };
        representer.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yaml = new Yaml(representer, options);

        try (Writer writer = new FileWriter(fileName)) {
            yaml.dump(data, writer);
        }
    }

    @SuppressWarnings("unchecked")
    private static void updateNestedMap(Map<String, Object> map, List<String> keys, Object value) {
        if (keys.isEmpty()) {
            return;
        }

        String currentKey = keys.get(0);

        if (keys.size() == 1) {
            map.put(currentKey, value);
        } else {
            if (!map.containsKey(currentKey)) {
                map.put(currentKey, new LinkedHashMap<String, Object>());
            }

            Object nextLevel = map.get(currentKey);
            if (nextLevel instanceof Map) {
                updateNestedMap((Map<String, Object>) nextLevel, keys.subList(1, keys.size()), value);
            } else {
                Map<String, Object> newMap = new LinkedHashMap<>();
                map.put(currentKey, newMap);
                updateNestedMap(newMap, keys.subList(1, keys.size()), value);
            }
        }
    }

    private static Map<String, Object> flattenYamlMap(Map<String, Object> nestedMap) {
        Map<String, Object> flatMap = new HashMap<>();
        flattenYamlMap("", nestedMap, flatMap);
        return flatMap;
    }

    private static void flattenYamlMap(String prefix, Map<String, Object> nestedMap,
                                       Map<String, Object> flatMap) {
        nestedMap.forEach((key, value) -> {
            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;

            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> innerMap = (Map<String, Object>) value;
                flattenYamlMap(fullKey, innerMap, flatMap);
            } else {
                flatMap.put(fullKey, value);
            }
        });
    }
}
