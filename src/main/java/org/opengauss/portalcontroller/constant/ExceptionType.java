package org.opengauss.portalcontroller.constant;

import java.util.List;

public interface ExceptionType {
    String PSQL = "PSQLException";
    String RETRYABLE = "RetryableException";
    List<String> IGNORED_EXCEPTION_LIST = List.of(PSQL,RETRYABLE);
}
