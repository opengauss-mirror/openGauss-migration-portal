package org.opengauss.portalcontroller.check;

import org.opengauss.portalcontroller.exception.PortalException;

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
     * @throws PortalException the portal exception
     */
    void changeParameters(String workspaceId) throws PortalException;

    /**
     * Copy config files.
     *
     * @param workspaceId the workspace id
     * @throws PortalException the portal exception
     */
    void copyConfigFiles(String workspaceId) throws PortalException;

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
