package org.opengauss.portalcontroller.constant;

public enum InstallWay {
    OFFLINE("offline"), ONLINE("online");
    private final String name;

    InstallWay(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
