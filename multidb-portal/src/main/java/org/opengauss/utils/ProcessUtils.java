/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * process utils
 *
 * @since 2025/3/1
 */
public class ProcessUtils {
    private static final Logger LOGGER = LogManager.getLogger(ProcessUtils.class);

    /**
     * Get the pid of the process with the given command
     *
     * @param command the command to be executed
     * @return the pid of the process
     *         if the process is not found, return -1
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static int getCommandPid(String command) throws IOException, InterruptedException {
        if (StringUtils.isNullOrBlank(command)) {
            throw new IllegalArgumentException("Command is null or empty");
        }

        int pid = -1;
        String processString = "";
        processString = executeCommandWithResult("ps ux | grep -- '" + command + "' | grep -v grep");
        if (!processString.isEmpty()) {
            String[] processArray = processString.split(System.lineSeparator());
            for (String singleProcess : processArray) {
                if (singleProcess.trim().contains(command)) {
                    String[] parts = singleProcess.split("\\s+");
                    pid = Integer.parseInt(parts[1]);
                }
            }
        }
        return pid;
    }

    /**
     * Kill the processes with the given command snippet
     *
     * @param commandSnippet the command snippet
     * @param isForce if true, use kill -9 to kill the process, otherwise use kill -15 to kill the process
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void killProcessByCommandSnippet(String commandSnippet, boolean isForce)
            throws IOException, InterruptedException {
        String killCommandPart = isForce ? "xargs -I {} kill -s KILL {}"
                : "xargs -I {} kill -s TERM {} || xargs -I {} kill -s KILL {}";
        String[] killProcessesCmd = {
                "/bin/sh",
                "-c",
                "ps -ef | grep -- '" + commandSnippet + "' | grep -v grep | awk '{print $2}' | " + killCommandPart
        };
        executeCommand(killProcessesCmd);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String command) throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @param workDirectory work directory
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String command, String workDirectory)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands, workDirectory);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String command, long waitMilliseconds) throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @param workDirectory work directory
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String command, String workDirectory, long waitMilliseconds)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands, workDirectory, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @param workDirectory work directory
     * @param waitMilliseconds wait milliseconds
     * @param env environment variables
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(
            String command, String workDirectory, long waitMilliseconds, Map<String, String> env)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands, workDirectory, waitMilliseconds, env);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @param workDirectory work directory
     * @param logPath log path
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String command, String workDirectory, String logPath, long waitMilliseconds)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands, workDirectory, logPath, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command string
     * @param workDirectory work directory
     * @param logPath log path
     * @param waitMilliseconds wait milliseconds
     * @param env environment variables
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(
            String command, String workDirectory, String logPath, long waitMilliseconds, Map<String, String> env)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        executeCommand(commands, workDirectory, logPath, waitMilliseconds, env);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String[] command) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        executeCommand(processBuilder);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param workDirectory work directory
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String[] command, String workDirectory)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File(workDirectory));

        executeCommand(processBuilder);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String[] command, long waitMilliseconds)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        executeCommand(processBuilder, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param workDirectory work directory
     * @param logPath log path
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String[] command, String workDirectory, String logPath)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File(workDirectory));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath)));

        executeCommand(processBuilder);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param workDirectory work directory
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String[] command, String workDirectory, long waitMilliseconds)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File(workDirectory));
        executeCommand(processBuilder, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param workDirectory work directory
     * @param waitMilliseconds wait milliseconds
     * @param env environment variables
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(
            String[] command, String workDirectory, long waitMilliseconds, Map<String, String> env)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.environment().putAll(env);
        processBuilder.directory(new File(workDirectory));
        executeCommand(processBuilder, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param workDirectory work directory
     * @param logPath log path
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(String[] command, String workDirectory, String logPath, long waitMilliseconds)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File(workDirectory));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath)));

        executeCommand(processBuilder, waitMilliseconds);
    }

    /**
     * Execute command
     *
     * @param command command array
     * @param workDirectory work directory
     * @param logPath log path
     * @param waitMilliseconds wait milliseconds
     * @param env environment variables
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeCommand(
            String[] command, String workDirectory, String logPath, long waitMilliseconds, Map<String, String> env)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.environment().putAll(env);
        processBuilder.directory(new File(workDirectory));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath)));

        executeCommand(processBuilder, waitMilliseconds);
    }

    /**
     * Execute interactive command
     *
     * @param command command string
     * @param workDirectory work directory
     * @param logPath log path
     * @param env environment variables
     * @param inputs inputs to be sent to the process
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeInteractiveCommand(String command, String workDirectory, String logPath,
                                                 Map<String, String> env, String[] inputs)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.environment().putAll(env);
        processBuilder.directory(new File(workDirectory));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath)));

        Process process = processBuilder.start();
        process.waitFor(1000, TimeUnit.MILLISECONDS);
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(),
                StandardCharsets.UTF_8))) {
            for (String outputOrder : inputs) {
                bw.write(outputOrder);
                Thread.sleep(1000);
            }
        }
    }

    /**
     * Execute command with result
     *
     * @param command command string
     * @return command result
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static String executeCommandWithResult(String command)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.redirectErrorStream(true);
        return executeCommandWithResult(processBuilder);
    }

    /**
     * Execute command with result
     *
     * @param command command string
     * @param workDirectory work directory
     * @return command result
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static String executeCommandWithResult(String command, String workDirectory)
            throws IOException, InterruptedException {
        String[] commands = new String[]{"bash", "-c", command};
        return executeCommandWithResult(commands, workDirectory);
    }

    /**
     * Execute command with result
     *
     * @param command command array
     * @param workDirectory work directory
     * @return command result
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static String executeCommandWithResult(String[] command, String workDirectory)
            throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File(workDirectory));
        processBuilder.redirectErrorStream(true);

        return executeCommandWithResult(processBuilder);
    }

    /**
     * Execute shell script
     *
     * @param scriptName script name
     * @param scriptDir script directory
     * @param logPath log path
     * @param waitMilliseconds wait milliseconds
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static void executeShellScript(String scriptName, String scriptDir, String logPath, long waitMilliseconds)
            throws IOException, InterruptedException {
        if (StringUtils.isNullOrBlank(scriptName) || StringUtils.isNullOrBlank(scriptDir)) {
            throw new IllegalArgumentException("ScriptName and scriptHomeDir cannot be null or empty");
        }
        String[] commands = new String[]{"sh", scriptName};

        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.directory(new File(scriptDir));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath)));

        executeCommandAndExit(processBuilder, waitMilliseconds);
    }

    /**
     * Execute command with result
     *
     * @param processBuilder process builder
     * @return command result
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the process to finish
     */
    public static String executeCommandWithResult(ProcessBuilder processBuilder)
            throws IOException, InterruptedException {
        StringBuilder output = new StringBuilder();
        Process process = null;
        try {
            process = processBuilder.start();
            try (InputStream inputStream = process.getInputStream();
                 BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
                process.waitFor();
            }
        } finally {
            if (process != null) {
                process.destroy();
            }
        }

        return output.toString();
    }

    private static void executeCommand(ProcessBuilder processBuilder) throws IOException, InterruptedException {
        Process process = null;
        try {
            process = processBuilder.start();

            StringBuilder stringBuilder = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream(),
                    StandardCharsets.UTF_8))) {
                String str;
                while ((str = br.readLine()) != null) {
                    stringBuilder.append(str).append(System.lineSeparator());
                }
            }

            int retCode = process.waitFor();
            String command = String.join(" ", processBuilder.command());
            if (retCode == 0) {
                LOGGER.debug("Execute command \"{}\" successfully", command);
            } else {
                String errorStr = stringBuilder.toString();
                if (!errorStr.isEmpty()) {
                    LOGGER.error("Execute command {} failed. Error: {}", command, errorStr);
                }
            }
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }

    private static void executeCommand(ProcessBuilder processBuilder, long waitMilliseconds)
            throws IOException, InterruptedException {
        if (waitMilliseconds > 0) {
            Process process = processBuilder.start();
            process.waitFor(waitMilliseconds, TimeUnit.MILLISECONDS);
        }
    }

    private static void executeCommandAndExit(ProcessBuilder processBuilder, long waitMilliseconds)
            throws IOException, InterruptedException {
        if (waitMilliseconds > 0) {
            Process process = processBuilder.start();
            process.waitFor(waitMilliseconds, TimeUnit.MILLISECONDS);
            process.destroy();
        }
    }
}
