package datawave.core.common.util;

import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.tserver.tablet.TabletClosedException;

/**
 * Centralizes non-public Accumulo exception type checks behind a single class.
 * <p>
 * Keeps actual imports so changes to these exception classes are caught at compile time rather than silently failing at runtime via string comparison.
 * <p>
 * Non-public imports are restricted to this class via import-control-accumulo.xml.
 *
 * @see <a href="https://github.com/NationalSecurityAgency/datawave/issues/2443">Issue #2443</a>
 * @see <a href="https://github.com/NationalSecurityAgency/datawave/pull/3318">PR #3318</a>
 */
public final class AccumuloExceptionChecker {

    private static final int MAX_CAUSE_DEPTH = 100;

    private AccumuloExceptionChecker() {}

    /**
     * @param t
     *            the throwable to check
     * @return true if {@code t} or any of its causes is a {@code TabletClosedException}
     */
    public static boolean isTabletClosedException(Throwable t) {
        return findTabletClosedException(t) != null;
    }

    /**
     * @param t
     *            the throwable to check
     * @return true if {@code t} or any of its causes is a {@code ScanTimedOutException}
     */
    public static boolean isScanTimedOutException(Throwable t) {
        return findCause(t, ScanTimedOutException.class) != null;
    }

    /**
     * Walks {@code t} and its causes for a {@code TabletClosedException}.
     *
     * @param t
     *            the throwable to check
     * @return the first {@code TabletClosedException} in the cause chain, or null if there is none
     */
    public static RuntimeException findTabletClosedException(Throwable t) {
        return findCause(t, TabletClosedException.class);
    }

    private static <T extends Throwable> T findCause(Throwable t, Class<T> type) {
        int depth = 0;
        while (t != null && depth < MAX_CAUSE_DEPTH) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
            if (t.getCause() == t) {
                break;
            }
            t = t.getCause();
            depth++;
        }
        return null;
    }
}
