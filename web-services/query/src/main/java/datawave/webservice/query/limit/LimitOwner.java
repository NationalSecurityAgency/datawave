package datawave.webservice.query.limit;

import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Indicates the originating type of 'owner' of a query limit.
 */
public enum LimitOwner {
    // @formatter:off
    USER(DatawaveErrorCode.CONCURRENT_USER_QUERY_LIMIT_EXCEEDED),
    SYSTEM(DatawaveErrorCode.CONCURRENT_SYSTEM_QUERY_LIMIT_EXCEEDED);
    // @formatter:on

    private final DatawaveErrorCode errorCode;

    LimitOwner(DatawaveErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public DatawaveErrorCode getErrorCode() {
        return errorCode;
    }
}
