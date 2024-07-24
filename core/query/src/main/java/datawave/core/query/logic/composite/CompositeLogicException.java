package datawave.core.query.logic.composite;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import datawave.webservice.query.exception.QueryException;

public class CompositeLogicException extends RuntimeException {

    public CompositeLogicException(String message, String logicName, Exception exception) {
        super(getMessage(message, Collections.singletonMap(logicName, exception)), getRaisedQueryException(exception));
    }

    public CompositeLogicException(String message, Map<String,Exception> exceptions) {
        super(getMessage(message, exceptions), getCause(exceptions.values()));
        if (exceptions.size() > 1) {
            exceptions.values().forEach(this::addSuppressed);
        }
    }

    /**
     * Return the cause to use, prioritizing the first {@link QueryException} instance that we see. In the case where the {@link QueryException} is found to be
     * the cause or further nested in the stack of an {@link Exception}, a {@link CompositeRaisedQueryException} will be returned with the query exception's
     * error code, and the original exception as the cause. This is necessary to ensure the error code is passed to query metrics.
     */
    private static Exception getCause(Collection<Exception> exceptions) {
        if (exceptions.size() == 1) {
            return exceptions.iterator().next();
        }
        Exception cause = null;
        for (Exception exception : exceptions) {
            // Establish the initial cause as the first seen exception.
            if (cause == null) {
                cause = getRaisedQueryException(exception);
                // If the first cause we see is a QueryException, there's nothing further to do.
                if (cause instanceof QueryException) {
                    return cause;
                }
                // If a subsequent exception is a or contains a QueryException in its stack, return it with the query exception error code available at the root
                // exception.
            } else if (hasQueryExceptionInStack(exception)) {
                return getRaisedQueryException(exception);
            }
        }
        return cause;
    }

    /**
     * Return whether the given throwable contains at least one {@link QueryException} in its stack trace (including itself).
     */
    private static boolean hasQueryExceptionInStack(Throwable throwable) {
        return getFirstQueryExceptionInStack(throwable) != null;
    }

    /**
     * Return the given exception with query exception's error code (if present) available at the root exception. This means one of the following cases will
     * occur:
     * <ul>
     * <li>The exception is not a {@link QueryException} and no {@link QueryException} exists in the exception's stack: The exception will be returned.</li>
     * <li>The exception is a {@link QueryException}: The exception will be returned.</li>
     * <li>The exception is not a {@link QueryException}, but a {@link QueryException} exists in the exception's stack. A {@link CompositeRaisedQueryException}
     * will be returned with the error code of the first {@link QueryException} found in the stack, and the original exception as its cause.</li>
     * </ul>
     */
    private static Exception getRaisedQueryException(Exception exception) {
        if (exception instanceof QueryException) {
            return exception;
        } else {
            // TODO - should we fetch the top-most or bottom-most query exception in the stack?
            QueryException queryException = getFirstQueryExceptionInStack(exception);
            if (queryException != null) {
                return new CompositeRaisedQueryException(exception, queryException.getErrorCode());
            } else {
                return exception;
            }
        }
    }

    /**
     * Return the first {@link QueryException} found in the stack, or null if none were found.
     */
    private static QueryException getFirstQueryExceptionInStack(Throwable throwable) {
        if (throwable != null) {
            if (throwable instanceof QueryException) {
                return (QueryException) throwable;
            } else {
                return getFirstQueryExceptionInStack(throwable.getCause());
            }
        }
        return null;
    }

    private static String getMessage(String message, Map<String,Exception> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append(message).append(":");
        exceptions.forEach((key, value) -> builder.append('\n').append(key).append(": ").append(value.getMessage()));
        return builder.toString();
    }
}
