package datawave.core.query.logic.composite;

import datawave.webservice.query.exception.QueryException;

/**
 * This class exists to be used when a {@link CompositeLogicException} has a cause that is not a {@link QueryException}, but contains a {@link QueryException}
 * in its stack trace. In order for the error code to be properly passed to query metrics, the error code must be present as part of the
 * {@link CompositeLogicException}'s cause. This exception is intended to be a wrapper for the original cause, with the error code of the identified query
 * exception.
 */
public class CompositeRaisedQueryException extends QueryException {

    public CompositeRaisedQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
}
