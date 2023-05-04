package org.opengauss.portalcontroller.exception;

import org.opengauss.portalcontroller.Plan;
import org.opengauss.portalcontroller.PortalControl;
import org.opengauss.portalcontroller.constant.Status;

/**
 * The type Portal exception.
 */
public class PortalException extends Exception {
    private String standardExceptionType;
    private String standardExceptionMessage;
    private String processName;
    private String requestInformation;
    private String repairTips;

    /**
     * Gets standard exception type.
     *
     * @return the standard exception type
     */
    public String getStandardExceptionType() {
        return standardExceptionType;
    }

    /**
     * Sets standard exception type.
     *
     * @param standardExceptionType the standard exception type
     */
    public void setStandardExceptionType(String standardExceptionType) {
        this.standardExceptionType = standardExceptionType;
    }

    /**
     * Gets standard exception message.
     *
     * @return the standard exception message
     */
    public String getStandardExceptionMessage() {
        return standardExceptionMessage;
    }

    /**
     * Sets standard exception message.
     *
     * @param standardExceptionMessage the standard exception message
     */
    public void setStandardExceptionMessage(String standardExceptionMessage) {
        this.standardExceptionMessage = standardExceptionMessage;
    }

    /**
     * Gets process name.
     *
     * @return the process name
     */
    public String getProcessName() {
        return processName;
    }

    /**
     * Sets process name.
     *
     * @param processName the process name
     */
    public void setProcessName(String processName) {
        this.processName = processName;
    }

    /**
     * Gets request information.
     *
     * @return the request information
     */
    public String getRequestInformation() {
        return requestInformation;
    }

    /**
     * Sets request information.
     *
     * @param requestInformation the request information
     */
    public void setRequestInformation(String requestInformation) {
        this.requestInformation = requestInformation;
    }

    /**
     * Gets repair tips.
     *
     * @return the repair tips
     */
    public String getRepairTips() {
        return repairTips;
    }

    /**
     * Sets repair tips.
     *
     * @param repairTips the repair tips
     */
    public void setRepairTips(String repairTips) {
        this.repairTips = repairTips;
    }

    /**
     * Instantiates a new Portal exception.
     *
     * @param standardExceptionType    the standard exception type
     * @param processName              the process name
     * @param standardExceptionMessage the standard exception message
     */
    public PortalException(String standardExceptionType, String processName, String standardExceptionMessage) {
        this.standardExceptionType = standardExceptionType;
        this.standardExceptionMessage = standardExceptionMessage;
        this.processName = processName;
    }

    /**
     * Instantiates a new Portal exception.
     *
     * @param standardExceptionType    the standard exception type
     * @param processName              the process name
     * @param standardExceptionMessage the standard exception message
     * @param requestInformation       the request information
     */
    public PortalException(String standardExceptionType, String processName, String standardExceptionMessage, String requestInformation) {
        this.standardExceptionType = standardExceptionType;
        this.standardExceptionMessage = standardExceptionMessage;
        this.processName = processName;
        this.requestInformation = requestInformation;
    }

    /**
     * Instantiates a new Portal exception.
     *
     * @param standardExceptionType    the standard exception type
     * @param processName              the process name
     * @param standardExceptionMessage the standard exception message
     * @param requestInformation       the request information
     * @param repairTips               the repair tips
     */
    public PortalException(String standardExceptionType, String processName, String standardExceptionMessage, String requestInformation, String repairTips) {
        this.standardExceptionType = standardExceptionType;
        this.standardExceptionMessage = standardExceptionMessage;
        this.processName = processName;
        this.requestInformation = requestInformation;
        this.repairTips = repairTips;
    }

    /**
     * Gets information.
     *
     * @return the information
     */
    public String getInformation() {
        if (standardExceptionType.equals("")) {
            standardExceptionType = "Portal exception";
        }
        String information = standardExceptionType + " occurred in " + processName + "." + System.lineSeparator();
        information += "Error message: " + standardExceptionMessage + System.lineSeparator();
        information += requestInformation;
        if (repairTips != null && !repairTips.equals("")) {
            information += ",or you can try " + repairTips + " to solve the exception.";
        } else {
            information += ".";
        }
        return information;
    }

    /**
     * Print log.
     *
     * @param LOGGER the logger
     */
    public void printLog(org.slf4j.Logger LOGGER) {
        String[] parts = getInformation().split(System.lineSeparator());
        for (String part : parts) {
            LOGGER.error(part);
        }
    }

    /**
     * Shut down portal.
     *
     * @param LOGGER the logger
     */
    public void shutDownPortal(org.slf4j.Logger LOGGER) {
        printLog(LOGGER);
        Plan.stopPlan = true;
        PortalControl.status = Status.ERROR;
        PortalControl.errorMsg = getInformation();
    }
}
