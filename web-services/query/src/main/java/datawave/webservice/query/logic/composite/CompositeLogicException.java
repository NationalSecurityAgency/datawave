package datawave.webservice.query.logic.composite;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import datawave.webservice.query.exception.QueryException;

public class CompositeLogicException extends RuntimeException {
    public CompositeLogicException(String message, String logicName, Exception exception) {
        super(getMessage(message, Collections.singletonMap(logicName, exception)), exception);
    }
    
    public CompositeLogicException(String message, Map<String,Exception> exceptions) {
        super(getMessage(message, exceptions), getQueryException(exceptions.values()));
        if (exceptions.size() > 1) {
            exceptions.values().stream().forEach(e -> addSuppressed(e));
        }
    }
    
    // looking for an exception that has a nested QueryException such that we may return an error code
    private static Exception getQueryException(Collection<Exception> exceptions) {
        if (exceptions.size() == 1) {
            return exceptions.iterator().next();
        }
        Exception e = null;
        for (Exception test : exceptions) {
            if (e == null) {
                e = test;
            } else if (isQueryException(test)) {
                e = test;
            }
            if (isQueryException(e)) {
                break;
            }
        }
        return e;
    }
    
    private static boolean isQueryException(Exception e) {
        return new QueryException(e).getQueryExceptionsInStack().size() > 1;
    }
    
    private static String getMessage(String message, Map<String,Exception> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append(message).append(":");
        exceptions.entrySet().stream().forEach(e -> builder.append('\n').append(e.getKey()).append(": ").append(e.getValue().getMessage()));
        return builder.toString();
    }
}
