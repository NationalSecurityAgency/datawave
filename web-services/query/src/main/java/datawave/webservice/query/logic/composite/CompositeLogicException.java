package datawave.webservice.query.logic.composite;

import datawave.webservice.query.logic.QueryLogic;

import java.util.List;
import java.util.Map;

public class CompositeLogicException extends RuntimeException {
    public CompositeLogicException(String message, List<Map.Entry<QueryLogic<?>,Exception>> exceptions) {
        super(getMessage(message, exceptions));
        exceptions.stream().forEach(e -> addSuppressed(e.getValue()));
    }
    
    private static String getMessage(String message, List<Map.Entry<QueryLogic<?>,Exception>> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append(message).append(":");
        exceptions.stream().forEach(e -> builder.append('\n').append(e.getKey().getLogicName()).append(": ").append(e.getValue().getMessage()));
        return builder.toString();
    }
}
