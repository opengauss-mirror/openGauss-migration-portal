/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.migration.tools;

import org.opengauss.exceptions.InstallException;
import org.opengauss.utils.FileUtils;
import org.opengauss.utils.UnzipUtils;

import java.io.IOException;

/**
 * tool
 *
 * @since 2025/2/17
 */
public abstract class Tool {
    /**
     * Install tool
     */
    public abstract void install();

    /**
     * Uninstall tool
     */
    public abstract void unInstall();

    /**
     * Get tool name
     *
     * @return tool name
     */
    public abstract String getToolName();

    /**
     * Check tool install
     *
     * @return true if tool is installed, false otherwise
     */
    public abstract boolean checkInstall();

    /**
     * Create tool install directory path
     *
     * @param installDirPath tool install directory path
     */
    protected void createInstallDirPath(String installDirPath) {
        try {
            FileUtils.createDirectory(installDirPath);
        } catch (IOException e) {
            throw new InstallException("Failed to create " + getToolName() + " install directory", e);
        }
    }

    /**
     * Unzip tool install package
     *
     * @param pkgDirPath     tool install package directory path
     * @param pkgName        tool install package name
     * @param installDirPath tool install directory path
     */
    protected void unzipPackage(String pkgDirPath, String pkgName, String installDirPath) {
        try {
            String pkgPath = String.format("%s/%s", pkgDirPath, pkgName);
            UnzipUtils.decompress(pkgPath, installDirPath);
        } catch (IOException e) {
            throw new InstallException("Failed to unzip " + getToolName() + " install package", e);
        }
    }

    /**
     * Check key file exists
     *
     * @param filePath key file path
     */
    protected void checkKeyFileExists(String filePath) {
        if (!FileUtils.checkFileExists(filePath)) {
            throw new InstallException("Failed to install " + getToolName()
                    + ", required file not found - " + filePath);
        }
    }

    /**
     * Delete path
     *
     * @param path path
     */
    protected void deletePath(String path) {
        try {
            FileUtils.deletePath(path);
        } catch (IOException e) {
            throw new InstallException("Failed to delete " + getToolName() + " install file - " + path, e);
        }
    }

    /**
     * Copy file
     *
     * @param sourcePath source file path
     * @param targetPath target file path
     */
    protected void copyFile(String sourcePath, String targetPath) {
        try {
            FileUtils.copyFile(sourcePath, targetPath);
        } catch (IOException e) {
            throw new InstallException("Failed to copy " + getToolName() + " install file - " + sourcePath, e);
        }
    }
}
