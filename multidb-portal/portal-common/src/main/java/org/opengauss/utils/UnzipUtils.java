/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;

/**
 * Unzip utils
 *
 * @since 2025/4/15
 */
public class UnzipUtils {
    /**
     * Decompress the file to the target directory
     *
     * @param sourceFilePath the source file path
     * @param targetDirPath the target directory path
     * @throws IOException if an I/O error occurs
     */
    public static void decompress(String sourceFilePath, String targetDirPath) throws IOException {
        Path sourcePath = Paths.get(sourceFilePath);
        Path targetPath = Paths.get(targetDirPath);

        if (!Files.exists(sourcePath)) {
            throw new FileNotFoundException("The source file does not exist: " + sourceFilePath);
        }

        if (sourceFilePath.endsWith(".zip")) {
            unzip(sourcePath, targetPath);
        } else if (sourceFilePath.endsWith(".tar.gz") || sourceFilePath.endsWith(".tgz")) {
            untarGz(sourcePath, targetPath);
        } else {
            throw new IllegalArgumentException("Only .zip and .tar.gz files are supported for decompression");
        }
    }

    private static void unzip(Path zipFile, Path targetDir) throws IOException {
        try (ZipFile zip = new ZipFile(zipFile.toFile())) {
            Enumeration<ZipArchiveEntry> entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                Path newPath = zipSlipProtect(entry, targetDir);

                if (entry.isDirectory()) {
                    Files.createDirectories(newPath);
                } else {
                    Files.createDirectories(newPath.getParent());
                    try (InputStream is = zip.getInputStream(entry)) {
                        Files.copy(is, newPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                }

                int mode = entry.getUnixMode();
                if (mode != 0) {
                    setFilePermissions(newPath, mode);
                }
            }
        }
    }

    private static void untarGz(Path tarGzFile, Path targetDir) throws IOException {
        try (InputStream fi = Files.newInputStream(tarGzFile);
             InputStream gzi = new GzipCompressorInputStream(fi);
             TarArchiveInputStream ti = new TarArchiveInputStream(gzi)) {
            TarArchiveEntry entry;
            while ((entry = ti.getNextEntry()) != null) {
                Path newPath = zipSlipProtect(entry, targetDir);
                if (entry.isDirectory()) {
                    Files.createDirectories(newPath);
                } else {
                    Files.createDirectories(newPath.getParent());
                    Files.copy(ti, newPath, StandardCopyOption.REPLACE_EXISTING);
                }
                setFilePermissions(newPath, entry.getMode());
            }
        }
    }

    private static Path zipSlipProtect(ZipEntry zipEntry, Path targetDir) throws IOException {
        Path targetDirResolved = targetDir.resolve(zipEntry.getName()).normalize();
        if (!targetDirResolved.startsWith(targetDir)) {
            throw new IOException("Malicious zip entry: " + zipEntry.getName());
        }
        return targetDirResolved;
    }

    private static Path zipSlipProtect(TarArchiveEntry tarEntry, Path targetDir) {
        String entryName = new String(tarEntry.getName().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
        return safeResolvePath(targetDir, entryName);
    }

    private static Path safeResolvePath(Path baseDir, String entryName) {
        try {
            String normalizedEntry = entryName.replace("\\", "/");
            return baseDir.resolve(normalizedEntry).normalize();
        } catch (InvalidPathException e) {
            String safeName = entryName.replaceAll("[^a-zA-Z0-9._-/]", "_");
            return baseDir.resolve(safeName).normalize();
        }
    }

    private static void setFilePermissions(Path path, int mode) throws IOException {
        if (mode == 0) {
            return;
        }

        if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            int permissionBits = mode & 0777;
            Set<PosixFilePermission> permissions = new HashSet<>();

            if ((permissionBits & 0400) != 0) {
                permissions.add(PosixFilePermission.OWNER_READ);
            }
            if ((permissionBits & 0200) != 0) {
                permissions.add(PosixFilePermission.OWNER_WRITE);
            }
            if ((permissionBits & 0100) != 0) {
                permissions.add(PosixFilePermission.OWNER_EXECUTE);
            }

            if ((permissionBits & 0040) != 0) {
                permissions.add(PosixFilePermission.GROUP_READ);
            }
            if ((permissionBits & 0020) != 0) {
                permissions.add(PosixFilePermission.GROUP_WRITE);
            }
            if ((permissionBits & 0010) != 0) {
                permissions.add(PosixFilePermission.GROUP_EXECUTE);
            }

            if ((permissionBits & 0004) != 0) {
                permissions.add(PosixFilePermission.OTHERS_READ);
            }
            if ((permissionBits & 0002) != 0) {
                permissions.add(PosixFilePermission.OTHERS_WRITE);
            }
            if ((permissionBits & 0001) != 0) {
                permissions.add(PosixFilePermission.OTHERS_EXECUTE);
            }

            Files.setPosixFilePermissions(path, permissions);
        }
    }
}
