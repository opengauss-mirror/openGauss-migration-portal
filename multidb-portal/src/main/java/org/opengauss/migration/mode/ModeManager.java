/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.mode;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONWriter;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opengauss.constants.MigrationModeConstants;
import org.opengauss.enums.MigrationPhase;
import org.opengauss.exceptions.MigrationModeException;
import org.opengauss.config.Portal;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.PropertiesUtils;
import org.opengauss.utils.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * Migration mode manager
 *
 * @since 2025/2/27
 */
@Getter
public class ModeManager {
    private static final Logger LOGGER = LogManager.getLogger(ModeManager.class);

    private final String modeJsonPath;

    public ModeManager() {
        modeJsonPath = String.format("%s/%s", Portal.getInstance().getPortalDataDirPath(),
                MigrationModeConstants.CUSTOM_MODE_STORAGE_FILE_NAME);
    }

    /**
     * Get mode by name
     *
     * @param modeName mode name
     * @return migration mode
     */
    public MigrationMode getModeByName(String modeName) {
        LOGGER.info("Getting migration mode by name: {}", modeName);
        for (MigrationMode migrationMode : list()) {
            if (migrationMode.getModeName().equals(modeName)) {
                return migrationMode;
            }
        }
        throw new MigrationModeException("Migration mode " + modeName + " does not exist");
    }

    /**
     * List all migration modes
     *
     * @return List of migration modes
     */
    public List<MigrationMode> list() {
        LOGGER.info("List all migration modes");
        List<MigrationMode> customModeList = loadCustomModeList();
        List<MigrationMode> defalutModeList = MigrationModeConstants.DEFALUT_MODE_LIST;

        List<MigrationMode> mergedModeList = new ArrayList<>(customModeList);
        mergedModeList.addAll(defalutModeList);
        return mergedModeList;
    }

    /**
     * Add migration mode
     *
     * @param modeFilePath migration mode define file path
     */
    public void add(String modeFilePath) {
        LOGGER.info("Start to add migration mode");
        Path filePath = Paths.get(modeFilePath).toAbsolutePath().normalize();
        checkModeFileExists(filePath.toString());

        try {
            Properties config = loadModeFile(filePath.toString());
            String addModeName = config.getProperty(MigrationModeConstants.TEMPLATE_KEY_MODE_NAME).trim();
            String addPhasesStr = config.getProperty(MigrationModeConstants.TEMPLATE_KEY_MIGRATION_PHASE_LIST).trim();
            checkModeName(addModeName);

            List<MigrationMode> migrationModeList = list();
            if (isModeNameExists(addModeName, migrationModeList)) {
                throw new MigrationModeException("Migration mode " + addModeName + " already exists, "
                        + "please use a different name");
            }

            List<MigrationPhase> addPhaseList = parseMigrationPhasesStr(addPhasesStr);

            checkPhaseListExists(addPhaseList, migrationModeList);

            MigrationMode addMigrationMode = new MigrationMode(addModeName, addPhaseList);
            writeModeToJsonFile(addMigrationMode);
            LOGGER.info("Migration mode {} added successfully", addModeName);
        } catch (IOException e) {
            throw new MigrationModeException("Failed to add migration mode", e);
        }
    }

    /**
     * Delete migration mode.
     *
     * @param modeName migration mode name
     */
    public void delete(String modeName) {
        LOGGER.info("Start to delete migration mode");
        if (isModeNameExists(modeName, MigrationModeConstants.DEFALUT_MODE_LIST)) {
            throw new MigrationModeException("Default migration mode " + modeName + " cannot be deleted or modified");
        }

        List<MigrationMode> customModeList = loadCustomModeList();
        if (customModeList.isEmpty() || !isModeNameExists(modeName, customModeList)) {
            throw new MigrationModeException("Migration mode " + modeName + " does not exist");
        }

        customModeList.removeIf(migrationMode -> migrationMode.getModeName().equals(modeName));
        try {
            writeModeListToJsonFile(customModeList);
            LOGGER.info("Migration mode {} deleted successfully", modeName);
        } catch (IOException e) {
            throw new MigrationModeException("Failed to delete migration mode", e);
        }
    }

    /**
     * Update migration mode
     *
     * @param modeFilePath migration mode define file path
     */
    public void update(String modeFilePath) {
        LOGGER.info("Start to update migration mode");
        Path filePath = Paths.get(modeFilePath).toAbsolutePath().normalize();
        checkModeFileExists(filePath.toString());

        Properties config = loadModeFile(filePath.toString());
        String updateModeName = config.getProperty(MigrationModeConstants.TEMPLATE_KEY_MODE_NAME).trim();
        String updatePhasesStr = config.getProperty(MigrationModeConstants.TEMPLATE_KEY_MIGRATION_PHASE_LIST).trim();
        checkModeName(updateModeName);

        if (isModeNameExists(updateModeName, MigrationModeConstants.DEFALUT_MODE_LIST)) {
            throw new MigrationModeException("Default migration mode " + updateModeName
                    + " cannot be modified or deleted");
        }

        List<MigrationMode> customModeList = loadCustomModeList();
        if (customModeList.isEmpty() || !isModeNameExists(updateModeName, customModeList)) {
            throw new MigrationModeException("Migration mode " + updateModeName + " does not exist");
        }

        List<MigrationPhase> updatePhaseList = parseMigrationPhasesStr(updatePhasesStr);
        customModeList.removeIf(migrationMode -> migrationMode.getModeName().equals(updateModeName));
        checkPhaseListExists(updatePhaseList, customModeList);
        checkPhaseListExists(updatePhaseList, MigrationModeConstants.DEFALUT_MODE_LIST);

        MigrationMode addMigrationMode = new MigrationMode(updateModeName, updatePhaseList);
        customModeList.add(addMigrationMode);
        try {
            writeModeListToJsonFile(customModeList);
            LOGGER.info("Migration mode {} updated successfully", updateModeName);
        } catch (IOException e) {
            throw new MigrationModeException("Failed to update migration mode", e);
        }
    }

    /**
     * Export migration mode template file
     */
    public void template() {
        try {
            String targetFilePath = String.format("%s/%s", Portal.getInstance().getPortalTmpDirPath(),
                    MigrationModeConstants.DEFINE_MODE_TEMPLATE_NAME);
            FileUtils.exportResource(MigrationModeConstants.DEFINE_MODE_TEMPLATE_RESOURCES_PATH, targetFilePath);
            LOGGER.info("Template file exported successfully");
            LOGGER.info("Template file path: {}", targetFilePath);
        } catch (IOException e) {
            throw new MigrationModeException("Failed to export template file", e);
        }
    }

    private void checkModeName(String modeName) {
        if (modeName.length() > MigrationModeConstants.MODE_NAME_MAX_LENGTH) {
            throw new MigrationModeException("The length of the mode name cannot exceed "
                    + MigrationModeConstants.MODE_NAME_MAX_LENGTH + " characters");
        }

        if (!modeName.matches(MigrationModeConstants.MODE_NAME_PATTERN)) {
            throw new MigrationModeException("Invalid mode name: " + modeName + ". "
                    + "Only letters(a-z A-Z), numbers(0-9), underscores(_), and hyphens(-) are allowed");
        }
    }

    private void checkModeFileExists(String modeFilePath) {
        if (!FileUtils.checkFileExists(modeFilePath)) {
            throw new MigrationModeException("File does not exist or is a directory: " + modeFilePath);
        }
    }

    private boolean isModeNameExists(String modeName, List<MigrationMode> migrationModeList) {
        return migrationModeList.stream().anyMatch(
                migrationMode -> migrationMode.getModeName().equals(modeName));
    }

    private void checkPhaseListExists(List<MigrationPhase> phaseList, List<MigrationMode> migrationModeList) {
        for (MigrationMode migrationMode : migrationModeList) {
            List<MigrationPhase> oldPhaseList = migrationMode.getMigrationPhaseList();
            if (new HashSet<>(oldPhaseList).equals(new HashSet<>(phaseList))) {
                throw new MigrationModeException("The same migration phase list already exists in the migration mode "
                        + migrationMode.getModeName());
            }
        }
    }

    private Properties loadModeFile(String modeFilePath) {
        try {
            Properties properties = PropertiesUtils.readProperties(modeFilePath);
            String modeName = properties.getProperty(MigrationModeConstants.TEMPLATE_KEY_MODE_NAME).trim();
            String phasesStr = properties.getProperty(MigrationModeConstants.TEMPLATE_KEY_MIGRATION_PHASE_LIST).trim();
            if (StringUtils.isNullOrBlank(modeName) || StringUtils.isNullOrBlank(phasesStr)) {
                String errorMsg = String.format("Invalid mode file, %s or %s cannot be null or empty",
                        MigrationModeConstants.TEMPLATE_KEY_MODE_NAME,
                        MigrationModeConstants.TEMPLATE_KEY_MIGRATION_PHASE_LIST);
                throw new MigrationModeException(errorMsg);
            }
            return properties;
        } catch (IOException e) {
            throw new MigrationModeException("Failed to load mode file", e);
        }
    }

    private List<MigrationPhase> parseMigrationPhasesStr(String phasesStr) {
        List<MigrationPhase> migrationPhaseList = new ArrayList<>();
        List<String> phaseStrs = Arrays.asList(phasesStr.split(","));

        if (phaseStrs.contains(MigrationPhase.FULL_MIGRATION.getPhaseName())) {
            migrationPhaseList.add(MigrationPhase.FULL_MIGRATION);
        }
        if (phaseStrs.contains(MigrationPhase.FULL_DATA_CHECK.getPhaseName())) {
            migrationPhaseList.add(MigrationPhase.FULL_DATA_CHECK);
        }

        boolean hasIncremental = phaseStrs.contains(MigrationPhase.INCREMENTAL_MIGRATION.getPhaseName());
        if (hasIncremental) {
            migrationPhaseList.add(MigrationPhase.INCREMENTAL_MIGRATION);
        }
        if (phaseStrs.contains(MigrationPhase.INCREMENTAL_DATA_CHECK.getPhaseName())) {
            if (!hasIncremental) {
                throw new MigrationModeException("Invalid migration phase list: " + phasesStr
                        + ", please add incremental migration phase before incremental data check phase");
            }
            migrationPhaseList.add(MigrationPhase.INCREMENTAL_DATA_CHECK);
        }
        if (phaseStrs.contains(MigrationPhase.REVERSE_MIGRATION.getPhaseName())) {
            migrationPhaseList.add(MigrationPhase.REVERSE_MIGRATION);
        }

        if (migrationPhaseList.isEmpty()) {
            throw new MigrationModeException("Invalid migration phase list: " + phasesStr
                    + ", please use the correct migration phase");
        }

        return Collections.unmodifiableList(migrationPhaseList);
    }

    private List<MigrationMode> loadCustomModeList() {
        try {
            createJsonFileIfNotExists();

            String modeJsonStr = FileUtils.readFileContents(modeJsonPath);
            if (StringUtils.isNullOrBlank(modeJsonStr)) {
                return Collections.emptyList();
            }

            ArrayList<MigrationMode> migrationModeList = new ArrayList<>();
            String[] modeJsonStrs = modeJsonStr.split(MigrationModeConstants.OBJECT_SEPARATOR);
            for (String modeJson : modeJsonStrs) {
                if (!modeJson.isBlank()) {
                    try {
                        migrationModeList.add(JSON.parseObject(modeJson.trim(), MigrationMode.class));
                    } catch (JSONException e) {
                        LOGGER.error("Failed to parse custom migration mode JSON: {}, "
                                + "all custom migration modes has been cleared", modeJson);
                        FileUtils.writeToFile(modeJsonPath, "", false);
                        return Collections.emptyList();
                    }
                }
            }
            return migrationModeList;
        } catch (IOException e) {
            LOGGER.error("Failed to load custom migration mode list", e);
            return Collections.emptyList();
        }
    }

    private void writeModeToJsonFile(MigrationMode migrationMode) throws IOException {
        createJsonFileIfNotExists();

        String objectJson = JSON.toJSONString(migrationMode, JSONWriter.Feature.PrettyFormat);
        String writeStr = String.format("%s%s%s", objectJson, MigrationModeConstants.OBJECT_SEPARATOR,
                System.lineSeparator());
        FileUtils.writeToFile(modeJsonPath, writeStr, true);
    }

    private void writeModeListToJsonFile(List<MigrationMode> modeList) throws IOException {
        createJsonFileIfNotExists();

        StringBuilder jsonBuilder = new StringBuilder();
        for (MigrationMode mode : modeList) {
            String objectJson = JSON.toJSONString(mode);
            jsonBuilder.append(objectJson)
                    .append(MigrationModeConstants.OBJECT_SEPARATOR)
                    .append(System.lineSeparator());
        }

        FileUtils.writeToFile(modeJsonPath, jsonBuilder.toString(), false);
    }

    private void createJsonFileIfNotExists() throws IOException {
        if (!FileUtils.checkFileExists(modeJsonPath)) {
            FileUtils.createFile(modeJsonPath);
        }
    }
}
