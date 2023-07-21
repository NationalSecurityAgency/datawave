package datawave.webservice.query.logic.composite;

import java.util.Map;

public class CompositeLogicException extends RuntimeException {
    public CompositeLogicException(String message, Map<String,Exception> exceptions) {
        super(getMessage(message, exceptions));
        exceptions.values().stream().forEach(e -> addSuppressed(e));
    }

    private static String getMessage(String message, Map<String,Exception> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append(message).append(":");
        exceptions.entrySet().stream().forEach(e -> builder.append('\n').append(e.getKey()).append(": ").append(e.getValue().getMessage()));
        return builder.toString();
    }
}
