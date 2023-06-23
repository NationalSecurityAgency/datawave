package datawave.core.query.logic.composite;

import java.util.Map;

public class CompositeLogicException extends RuntimeException {
    public CompositeLogicException(String message, Map<String,Exception> exceptions) {
        super(getMessage(message, exceptions));
        exceptions.values().forEach(this::addSuppressed);
    }

    private static String getMessage(String message, Map<String,Exception> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append(message).append(":");
        exceptions.forEach((key, value) -> builder.append('\n').append(key).append(": ").append(value.getMessage()));
        return builder.toString();
    }
}
