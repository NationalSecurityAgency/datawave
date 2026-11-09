package datawave.core.common.util;

import static java.util.Objects.requireNonNull;

/**
 * Utility methods for searching the cause chain of a {@link Throwable}.
 * <p>
 * A search starts with the given throwable itself and follows {@link Throwable#getCause()}. A throwable matches when it is an instance of the requested type,
 * so subclasses of that type match too. A null throwable has no matches.
 * <p>
 * {@code maxDepth} is the maximum number of throwables examined, counting the given throwable itself. The given throwable is at depth 0, its cause at depth 1,
 * and so on, and only throwables at a depth less than {@code maxDepth} are examined. A {@code maxDepth} of 1 examines only the given throwable, and a
 * {@code maxDepth} of 0 examines nothing. The overloads without a {@code maxDepth} use {@link #DEFAULT_MAX_DEPTH}.
 * <p>
 * A search also stops at a throwable whose {@code getCause()} returns itself. A longer cycle, such as A caused by B caused by A, ends when {@code maxDepth}
 * throwables have been examined.
 */
public final class ExceptionUtils {

    /**
     * The {@code maxDepth} used by {@link #containsCause(Throwable, Class)} and {@link #findCause(Throwable, Class)}.
     */
    public static final int DEFAULT_MAX_DEPTH = 100;

    private ExceptionUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Checks whether {@code t} or one of its causes is an instance of {@code type}, examining at most {@link #DEFAULT_MAX_DEPTH} throwables.
     *
     * @param t
     *            the throwable to check, may be null
     * @param type
     *            the type to look for
     * @return true if {@code t} or one of the causes examined is an instance of {@code type}
     * @throws NullPointerException
     *             if {@code type} is null
     */
    public static boolean containsCause(Throwable t, Class<? extends Throwable> type) {
        return containsCause(t, type, DEFAULT_MAX_DEPTH);
    }

    /**
     * Checks whether {@code t} or one of its causes is an instance of {@code type}, examining at most {@code maxDepth} throwables.
     *
     * @param t
     *            the throwable to check, may be null
     * @param type
     *            the type to look for
     * @param maxDepth
     *            the maximum number of throwables to examine, counting {@code t} itself
     * @return true if {@code t} or one of the causes examined is an instance of {@code type}
     * @throws NullPointerException
     *             if {@code type} is null
     * @throws IllegalArgumentException
     *             if {@code maxDepth} is negative
     */
    public static boolean containsCause(Throwable t, Class<? extends Throwable> type, int maxDepth) {
        return findCause(t, type, maxDepth) != null;
    }

    /**
     * Finds the first throwable in the cause chain of {@code t}, starting with {@code t} itself, that is an instance of {@code type}, examining at most
     * {@link #DEFAULT_MAX_DEPTH} throwables.
     *
     * @param t
     *            the throwable to check, may be null
     * @param type
     *            the type to look for
     * @param <T>
     *            the type to look for
     * @return the match closest to {@code t}, or null if there is none
     * @throws NullPointerException
     *             if {@code type} is null
     */
    public static <T extends Throwable> T findCause(Throwable t, Class<T> type) {
        return findCause(t, type, DEFAULT_MAX_DEPTH);
    }

    /**
     * Finds the first throwable in the cause chain of {@code t}, starting with {@code t} itself, that is an instance of {@code type}, examining at most
     * {@code maxDepth} throwables.
     *
     * @param t
     *            the throwable to check, may be null
     * @param type
     *            the type to look for
     * @param maxDepth
     *            the maximum number of throwables to examine, counting {@code t} itself
     * @param <T>
     *            the type to look for
     * @return the match closest to {@code t}, or null if there is none
     * @throws NullPointerException
     *             if {@code type} is null
     * @throws IllegalArgumentException
     *             if {@code maxDepth} is negative
     */
    public static <T extends Throwable> T findCause(Throwable t, Class<T> type, int maxDepth) {
        requireNonNull(type, "type cannot be null");
        if (maxDepth < 0) {
            throw new IllegalArgumentException("maxDepth cannot be negative: " + maxDepth);
        }
        Throwable current = t;
        for (int depth = 0; current != null && depth < maxDepth; depth++) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return null;
    }
}
