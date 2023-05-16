package org.opengauss.portalcontroller.constant;

public enum ExceptionType {
    PSQL("PSQLException");
    private final String name;

    ExceptionType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
