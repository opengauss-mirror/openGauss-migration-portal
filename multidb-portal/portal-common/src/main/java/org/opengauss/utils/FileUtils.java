/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * File utils
 *
 * @since 2025/2/14
 */
public class FileUtils {
    /**
     * Create directory
     *
     * @param dir directory path
     * @throws IOException if an I/O error occurs
     */
    public static void createDirectory(String dir) throws IOException {
        if (StringUtils.isNullOrBlank(dir)) {
            throw new IllegalArgumentException("Directory path cannot be null or empty");
        }

        Path path = Paths.get(dir).normalize();
        if (Files.exists(path)) {
            if (!Files.isDirectory(path)) {
                throw new IOException("Path exists but is not a directory: " + path);
            }
            return;
        }

        Files.createDirectories(path);
    }

    /**
     * Create multiple directories
     *
     * @param dirs directory paths
     * @throws IOException if an I/O error occurs
     */
    public static void createDirectories(String... dirs) throws IOException {
        if (dirs == null || dirs.length == 0) {
            throw new IllegalArgumentException("Directories cannot be null or empty");
        }

        for (String dir : dirs) {
            createDirectory(dir);
        }
    }

    /**
     * Check if the specified directory has enough free space
     *
     * @param dir directory path
     * @param mbThreshold threshold (MB)
     * @return true if there is enough free space, false otherwise
     * @throws IOException if an I/O error occurs
     */
    public static boolean isSpaceSufficient(String dir, long mbThreshold) throws IOException {
        if (StringUtils.isNullOrBlank(dir)) {
            throw new IllegalArgumentException("Directory path cannot be null or empty");
        }
        if (mbThreshold <= 0) {
            throw new IllegalArgumentException("The threshold must be greater than 0");
        }

        File directory = new File(dir);
        if (!directory.exists()) {
            throw new IOException("The directory does not exist: " + dir);
        }
        if (!directory.isDirectory()) {
            throw new IOException("The path is not a directory: " + dir);
        }
        if (!directory.canRead()) {
            throw new IOException("No read permission for the directory: " + dir);
        }

        long freeSpaceBytes = directory.getUsableSpace();
        long freeSpaceMB = freeSpaceBytes / (1024 * 1024);
        return freeSpaceMB >= mbThreshold;
    }

    /**
     * Read file contents
     *
     * @param filePath file path
     * @return file contents
     * @throws IOException if an I/O error occurs
     */
    public static String readFileContents(String filePath) throws IOException {
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
            }
        }
        return content.toString();
    }

    /**
     * Read file last line
     *
     * @param filePath file path
     * @return file last line
     * @throws IOException if an I/O error occurs
     */
    public static String readFileLastLine(String filePath) throws IOException {
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            StringBuilder resultBuilder = new StringBuilder();
            long length = file.length();
            if (length == 0) {
                return resultBuilder.toString();
            }

            long pos = length - 1;

            while (pos >= 0) {
                file.seek(pos);
                int b = file.read();
                if (b == '\n' || b == '\r') {
                    if (pos == length - 1) {
                        length--;
                        pos--;
                        continue;
                    }
                    break;
                }
                resultBuilder.append((char) b);
                pos--;
            }

            return resultBuilder.reverse().toString();
        }
    }

    /**
     * Write file contents
     *
     * @param filePath file path
     * @param content file contents
     * @param isAppend is append mode
     * @throws IOException if an I/O error occurs
     */
    public static void writeToFile(String filePath, String content, boolean isAppend) throws IOException {
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, isAppend))) {
            writer.write(content);
        }
    }

    /**
     * Check if the file exists
     *
     * @param filePath file path
     * @return true if the file exists, false otherwise
     */
    public static boolean checkFileExists(String filePath) {
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        File file = new File(filePath);
        return file.exists() && file.isFile();
    }

    /**
     * Check if the directory exists
     *
     * @param dirPath directory path
     * @return true if the directory exists, false otherwise
     */
    public static boolean checkDirExists(String dirPath) {
        if (StringUtils.isNullOrBlank(dirPath)) {
            throw new IllegalArgumentException("Directory path cannot be null or empty");
        }

        File file = new File(dirPath);
        return file.exists() && file.isDirectory();
    }

    /**
     * Delete a file or directory.
     * If the path is a directory, delete all its contents.
     *
     * @param deletePath delete file or directory path
     * @throws IOException if an I/O error occurs
     */
    public static void deletePath(String deletePath) throws IOException {
        if (StringUtils.isNullOrBlank(deletePath)) {
            throw new IllegalArgumentException("Delete path cannot be null or empty");
        }

        Path path = Paths.get(deletePath);
        if (Files.exists(path)) {
            try (Stream<Path> pathStream = Files.walk(path)) {
                List<Path> pathList = pathStream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
                for (Path p : pathList) {
                    Files.delete(p);
                }
            }
        }
    }

    /**
     * Delete all files in a directory
     *
     * @param directoryPath directory path
     * @throws IOException if an I/O error occurs
     */
    public static void cleanDirectory(String directoryPath) throws IOException {
        if (StringUtils.isNullOrBlank(directoryPath)) {
            throw new IllegalArgumentException("Directory path cannot be null or empty");
        }

        Path dir = Paths.get(directoryPath);
        if (!Files.exists(dir)) {
            throw new NoSuchFileException("Directory does not exist: " + directoryPath);
        }
        if (!Files.isDirectory(dir)) {
            throw new NotDirectoryException("Not a directory: " + directoryPath);
        }

        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .filter(path -> !path.equals(dir))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Create a file.
     * If the file already exists, do nothing.
     * If the directory does not exist, create it.
     *
     * @param filePath file path
     * @throws IOException if an I/O error occurs
     */
    public static void createFile(String filePath) throws IOException {
        if (StringUtils.isNullOrBlank(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        Path path = Paths.get(filePath);
        if (Files.exists(path)) {
            if (Files.isDirectory(path)) {
                throw new IOException("The path is a directory: " + path);
            }
            return;
        }

        Path parentDir = path.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        Files.createFile(path);
    }

    /**
     * Set the file to read-only permissions.
     *
     * @param filePath file path
     * @throws IOException if the file does not exist or permission modification fails
     */
    public static void setFileReadOnly(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new IOException("File does not exist: " + filePath);
        }

        Set<PosixFilePermission> permissions = new HashSet<>();
        permissions.add(PosixFilePermission.OWNER_READ);

        try {
            Files.setPosixFilePermissions(path, permissions);
        } catch (UnsupportedOperationException e) {
            throw new IOException("Current file system does not support POSIX permission setting", e);
        }
    }

    /**
     * move a file to a different directory, or rename a file
     * this method will overwrite the target file if it already exists
     *
     * @param oldFilePath old file path
     * @param newFilePath new file path
     * @throws IOException if an I/O error occurs
     */
    public static void moveFile(String oldFilePath, String newFilePath) throws IOException {
        Path source = Paths.get(oldFilePath);
        Path target = Paths.get(newFilePath);

        if (!Files.exists(source)) {
            throw new IOException("Source file does not exist: " + oldFilePath);
        }

        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * copy file to specified path (can modify file name)
     * this method will overwrite the target file if it already exists
     *
     * @param sourceFilePath source file path
     * @param targetFilePath target file path
     * @throws IOException if an I/O error occurs
     */
    public static void copyFile(String sourceFilePath, String targetFilePath) throws IOException {
        if (StringUtils.isNullOrBlank(sourceFilePath) || StringUtils.isNullOrBlank(targetFilePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }
        Path source = Paths.get(sourceFilePath);
        Path target = Paths.get(targetFilePath);

        if (!Files.exists(source)) {
            throw new IOException("Source file does not exist: " + sourceFilePath);
        }

        if (!Files.isRegularFile(source)) {
            throw new IOException("Source path is not a file: " + sourceFilePath);
        }

        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * export resource file to external path
     *
     * @param resourceFilePath resource file path relative to resources directory
     * @param outputFilePath output file path
     * @throws IOException if an I/O error occurs
     */
    public static void exportResource(String resourceFilePath, String outputFilePath) throws IOException {
        Path outputPath = Paths.get(outputFilePath);
        Path outputDir = outputPath.getParent();
        if (outputDir != null) {
            Files.createDirectories(outputDir);
        }

        try (InputStream in = FileUtils.class.getClassLoader().getResourceAsStream(resourceFilePath);
             OutputStream out = Files.newOutputStream(outputPath)) {
            if (in == null) {
                throw new IOException("Resource file not found: " + resourceFilePath);
            }

            byte[] buffer = new byte[1024];
            int length;
            while ((length = in.read(buffer)) > 0) {
                out.write(buffer, 0, length);
            }
        }
    }

    /**
     * Replace all matching strings in the file
     *
     * @param filePath file path
     * @param oldString old string
     * @param newString new string, can be null or empty
     * @throws IOException if an I/O error occurs
     */
    public static void replaceInFile(String filePath, String oldString, String newString) throws IOException {
        if (StringUtils.isNullOrBlank(filePath) || StringUtils.isNullOrBlank(oldString)) {
            throw new IllegalArgumentException("File path and old string cannot be null or empty");
        }
        Path path = Path.of(filePath);
        File file = path.toFile();
        if (!file.exists() || !file.isFile()) {
            throw new FileNotFoundException("File does not exist or file is a directory: " + filePath);
        }

        File tempFile = File.createTempFile("replace", ".tmp", file.getParentFile());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file),
                        StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile),
                        StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String replacedLine = line.replace(oldString, newString == null ? "" : newString);
                writer.write(replacedLine);
                writer.newLine();
            }
        }

        Files.move(tempFile.toPath(), path, StandardCopyOption.REPLACE_EXISTING);
    }
}
