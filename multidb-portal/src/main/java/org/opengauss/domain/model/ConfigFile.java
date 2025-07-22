/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.domain.model;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.enums.FileFormat;
import org.opengauss.enums.TemplateConfigType;
import org.opengauss.exceptions.ConfigException;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.PropertiesUtils;
import org.opengauss.utils.YmlUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * config file
 *
 * @since 2025/4/29
 */
@Getter
public class ConfigFile {
    private static final Logger LOGGER = LogManager.getLogger(ConfigFile.class);

    private final String name;
    private final String fileDirPath;
    private final TaskWorkspace taskWorkspace;
    private final TemplateConfigType templateConfigType;
    private final Map<String, Object> configMap;
    private final Set<String> deleteConfigKeySet;

    public ConfigFile(String name, String fileDirPath, TaskWorkspace taskWorkspace,
                      TemplateConfigType templateConfigType) {
        this.name = name;
        this.fileDirPath = fileDirPath;
        this.taskWorkspace = taskWorkspace;
        this.templateConfigType = templateConfigType;
        this.configMap = new HashMap<>();
        this.deleteConfigKeySet = new HashSet<>();
    }

    /**
     * get file path
     *
     * @return file path
     */
    public String getFilePath() {
        return String.format("%s/%s", fileDirPath, name);
    }

    /**
     * get config map
     *
     * @return config map
     */
    public Map<String, Object> getConfigMap() {
        if (configMap.isEmpty() && !templateConfigType.getFileFormat().equals(FileFormat.XML)) {
            throw new IllegalStateException("Config map has not loaded yet. Please call loadConfigMap() first. ");
        }
        return configMap;
    }

    /**
     * load config map
     */
    public void loadConfigMap() {
        try {
            if (templateConfigType.getFileFormat().equals(FileFormat.PROPERTIES)) {
                configMap.putAll(PropertiesUtils.readPropertiesAsMap(getFilePath()));
                return;
            }

            if (templateConfigType.getFileFormat().equals(FileFormat.YML)) {
                configMap.putAll(YmlUtils.loadYaml(getFilePath()));
                return;
            }

            if (templateConfigType.getFileFormat().equals(FileFormat.XML)) {
                return;
            }
        } catch (IOException e) {
            throw new ConfigException("Failed to load config map from file: " + getFilePath(), e);
        }
        LOGGER.warn("Unsupported file format: {} to load config map", templateConfigType.getFileFormat());
    }

    /**
     * generate config file from template
     */
    public void generateFile() {
        String configTemplatePath = templateConfigType.getFilePath();
        boolean isInResources = templateConfigType.isInResources();
        String configFilePath = getFilePath();
        try {
            if (isInResources) {
                FileUtils.exportResource(configTemplatePath, configFilePath);
            } else {
                FileUtils.copyFile(configTemplatePath, configFilePath);
            }
        } catch (IOException e) {
            throw new ConfigException("Failed to prepare migration config file: " + configFilePath, e);
        }
    }

    /**
     * save config map to file
     */
    public void saveConfigMap() {
        changeConfig(configMap);
        deleteConfigKeys();
    }

    /**
     * change config file params in config map
     *
     * @param configMap config map
     */
    public void changeConfig(Map<String, Object> configMap) {
        try {
            if (templateConfigType.getFileFormat().equals(FileFormat.PROPERTIES)) {
                HashMap<String, String> changeParams = new HashMap<>();
                for (Map.Entry<String, Object> entry : configMap.entrySet()) {
                    changeParams.put(entry.getKey(), String.valueOf(entry.getValue()));
                }
                PropertiesUtils.updateProperties(getFilePath(), changeParams);
                return;
            }
            if (templateConfigType.getFileFormat().equals(FileFormat.YML)) {
                YmlUtils.updateYaml(getFilePath(), configMap);
                return;
            }
            if (templateConfigType.getFileFormat().equals(FileFormat.XML)) {
                for (Map.Entry<String, Object> entry : configMap.entrySet()) {
                    FileUtils.replaceInFile(getFilePath(), entry.getKey(), String.valueOf(entry.getValue()));
                }
                return;
            }
        } catch (IOException e) {
            throw new ConfigException("Failed to save config map to file: " + getFilePath(), e);
        }
        LOGGER.warn("Unsupported file format: {} to save config map", templateConfigType.getFileFormat());
    }

    /**
     * delete config keys in config file
     */
    public void deleteConfigKeys() {
        try {
            if (templateConfigType.getFileFormat().equals(FileFormat.PROPERTIES)) {
                PropertiesUtils.commentProperties(getFilePath(), deleteConfigKeySet);
            }
        } catch (IOException e) {
            throw new ConfigException("Failed to comment keys from file: " + getFilePath(), e);
        }
    }
}
