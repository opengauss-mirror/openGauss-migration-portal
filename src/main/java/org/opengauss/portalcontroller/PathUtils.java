package org.opengauss.portalcontroller;

import java.io.File;

public class PathUtils {
    public static String combainPath(boolean isFile, String... parts) {
        StringBuilder path = new StringBuilder();
        path = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            path.append(File.separator).append(parts[i]);
        }
        if (!isFile) {
            path.append(File.separator);
        }
        return path.toString();
    }
}
