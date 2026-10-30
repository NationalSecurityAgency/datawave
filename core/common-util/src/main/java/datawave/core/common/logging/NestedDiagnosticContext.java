package datawave.core.common.logging;

import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.MDC;

/**
 * A nested diagnostic context stored in SLF4J's MDC under the {@code NDC} key.
 */
public final class NestedDiagnosticContext {

    private static final ThreadLocal<Deque<String>> context = ThreadLocal.withInitial(ArrayDeque::new);

    private NestedDiagnosticContext() {}

    public static void push(String message) {
        Deque<String> stack = context.get();
        stack.addLast(String.valueOf(message));
        MDC.put("NDC", String.join(" ", stack));
    }

    public static String pop() {
        Deque<String> stack = context.get();
        String message = stack.pollLast();
        if (stack.isEmpty()) {
            MDC.remove("NDC");
            context.remove();
        } else {
            MDC.put("NDC", String.join(" ", stack));
        }
        return message == null ? "" : message;
    }

    public static String peek() {
        String message = context.get().peekLast();
        return message == null ? "" : message;
    }
}
