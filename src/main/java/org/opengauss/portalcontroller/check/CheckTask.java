package org.opengauss.portalcontroller.check;

/**
 * The interface Check task.
 */
public interface CheckTask {

    /**
     * Install all packages boolean.
     *
     * @param download the download
     * @return the boolean
     */
    boolean installAllPackages(boolean download);

    /**
     * Install all packages boolean.
     *
     * @return the boolean
     */
    boolean installAllPackages();

    /**
     * Change parameters.
     *
     * @param workspaceId the workspace id
     */
    void changeParameters(String workspaceId);

    /**
     * Copy config files.
     *
     * @param workspaceId the workspace id
     */
    void copyConfigFiles(String workspaceId);

    /**
     * Prepare work.
     *
     * @param workspaceId the workspace id
     */
    void prepareWork(String workspaceId);

    /**
     * Start.
     *
     * @param workspaceId the workspace id
     */
    void start(String workspaceId);

    /**
     * Check end.
     */
    void checkEnd();

    /**
     * Uninstall.
     */
    void uninstall();
}
