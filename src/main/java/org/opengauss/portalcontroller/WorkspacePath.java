package org.opengauss.portalcontroller;

import java.io.File;

/**
 * The type Workspace path.
 */
public class WorkspacePath {
    private static volatile WorkspacePath workspacePath;
    private static final String WORKSPACE = "workspace";
    private static final String CONFIG = "config";
    private static final String LOGS = "logs";
    private static final String TMP = "tmp";

    private static final String STATUS = "status";

    private String root;
    private String workspaceId;

    /**
     * Instantiates a new Workspace path.
     *
     * @param root        the root
     * @param workspaceid the workspaceid
     */
    public WorkspacePath(String root, String workspaceid) {
        this.root = root;
        this.workspaceId = workspaceid;
    }

    /**
     * Gets workspace status path.
     *
     * @return the workspace status path
     */
    public String getWorkspaceStatusPath() {
        return getWorkspacePath() + File.separator + STATUS;
    }

    /**
     * Gets workspace config path.
     *
     * @return the workspace config path
     */
    public String getWorkspaceConfigPath() {
        return getWorkspacePath() + File.separator + CONFIG;
    }

    /**
     * Gets workspace log path.
     *
     * @return the workspace log path
     */
    public String getWorkspaceLogPath() {
        return getWorkspacePath() + File.separator + LOGS;
    }

    /**
     * Gets workspace path.
     *
     * @return the workspace path
     */
    public String getWorkspacePath() {
        return root + WORKSPACE + File.separator + workspaceId;
    }

    /**
     * Gets instance.
     *
     * @param root        the root
     * @param workspaceid the workspaceid
     * @return the instance
     */
    public static WorkspacePath getInstance(String root, String workspaceid) {
        if (workspacePath == null) {
            synchronized (Plan.class) {
                if (workspacePath == null) {
                    workspacePath = new WorkspacePath(root, workspaceid);
                }
            }
        }
        return workspacePath;
    }

}
