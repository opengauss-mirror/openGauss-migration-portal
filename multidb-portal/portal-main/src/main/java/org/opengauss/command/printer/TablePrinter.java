/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.opengauss.command.printer;

import java.util.List;
import java.util.Locale;

/**
 * This class provides a utility method to print a 2D table with headers and rows.
 *
 * @since 2025/4/29
 */
public class TablePrinter {
    private static final String PLUS_SIGN = "+";
    private static final String MINUS_SIGN = "-";
    private static final String PIPE_SIGN = "|";

    /**
     * Prints a 2D table with the given headers and rows.
     *
     * @param headers table header list
     * @param rows table row list, each internal list must have the same size as headers
     * @return the table string
     */
    public static String printTable(List<String> headers, List<List<String>> rows) {
        validateArgs(headers, rows);

        int[] columnWidths = new int[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            columnWidths[i] = headers.get(i).length();
        }

        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                String cell = row.get(i);
                if (cell != null && cell.length() > columnWidths[i]) {
                    columnWidths[i] = cell.length();
                }
            }
        }

        for (int i = 0; i < columnWidths.length; i++) {
            columnWidths[i] += 2;
        }

        StringBuilder table = new StringBuilder();
        appendLine(table, columnWidths);
        appendRow(table, headers, columnWidths);
        appendLine(table, columnWidths);

        for (List<String> row : rows) {
            appendRow(table, row, columnWidths);
        }

        appendLine(table, columnWidths);
        return table.toString();
    }

    private static void appendLine(StringBuilder sb, int[] columnWidths) {
        sb.append(PLUS_SIGN);
        for (int width : columnWidths) {
            sb.append(MINUS_SIGN.repeat(width));
            sb.append(PLUS_SIGN);
        }
        sb.append(System.lineSeparator());
    }

    private static void appendRow(StringBuilder sb, List<String> cells, int[] columnWidths) {
        sb.append(PIPE_SIGN);
        for (int i = 0; i < cells.size(); i++) {
            String cell = cells.get(i) != null ? cells.get(i) : "";
            sb.append(String.format(" %-" + (columnWidths[i] - 1) + "s|", cell));
        }
        sb.append(System.lineSeparator());
    }

    private static void validateArgs(List<String> headers, List<List<String>> rows) {
        if (headers == null || headers.isEmpty()) {
            throw new IllegalArgumentException("Headers cannot be null or empty");
        }

        if (rows == null) {
            throw new IllegalArgumentException("Rows cannot be null");
        }

        int headerSize = headers.size();
        for (int i = 0; i < rows.size(); i++) {
            List<String> row = rows.get(i);
            if (row == null || row.size() != headerSize) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Row %d has invalid size (header size: %d, row size: %s)",
                                i + 1, headerSize, row == null ? "null" : row.size()));
            }
        }
    }
}
