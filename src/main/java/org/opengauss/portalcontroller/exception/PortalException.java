/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.portalcontroller.exception;

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
        this(standardExceptionType, processName, standardExceptionMessage, "", "");
    }

    /**
     * Instantiates a new Portal exception.
     *
     * @param standardExceptionType    the standard exception type
     * @param processName              the process name
     * @param standardExceptionMessage the standard exception message
     * @param requestInformation       the request information
     */
    public PortalException(String standardExceptionType, String processName, String standardExceptionMessage,
                           String requestInformation) {
        this(standardExceptionType, processName, standardExceptionMessage, requestInformation, "");
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
    public PortalException(String standardExceptionType, String processName, String standardExceptionMessage,
                           String requestInformation, String repairTips) {
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
    public String toString() {
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
}
