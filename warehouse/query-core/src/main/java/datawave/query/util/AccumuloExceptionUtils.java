package datawave.query.util;

/**
 * Utility methods for handling Accumulo exceptions without importing non-public APIs.
 * <p>
 * These methods check exception types by class name to avoid dependencies on non-public Accumulo APIs that may change between versions.
 */
public final class AccumuloExceptionUtils {

    private static final String ITERATION_INTERRUPTED_EXCEPTION = "org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException";
    private static final String TABLET_CLOSED_EXCEPTION = "org.apache.accumulo.tserver.tablet.TabletClosedException";
    private static final String SCAN_TIMED_OUT_EXCEPTION = "org.apache.accumulo.core.clientImpl.ThriftScanner$ScanTimedOutException";

    private AccumuloExceptionUtils() {
        // utility class
    }

    /**
     * Check if the throwable is an IterationInterruptedException.
     * <p>
     * This is thrown by Accumulo when an iteration is interrupted, typically due to a client disconnecting or a scan timeout.
     *
     * @param t
     *            the throwable to check
     * @return true if the throwable is an IterationInterruptedException
     */
    public static boolean isIterationInterruptedException(Throwable t) {
        return isExceptionOfType(t, ITERATION_INTERRUPTED_EXCEPTION);
    }

    /**
     * Check if the throwable is a TabletClosedException.
     * <p>
     * This is thrown by Accumulo when a tablet server closes a tablet during a scan.
     *
     * @param t
     *            the throwable to check
     * @return true if the throwable is a TabletClosedException
     */
    public static boolean isTabletClosedException(Throwable t) {
        return isExceptionOfType(t, TABLET_CLOSED_EXCEPTION);
    }

    /**
     * Check if the throwable is a ScanTimedOutException.
     * <p>
     * This is thrown by Accumulo when a scan times out.
     *
     * @param t
     *            the throwable to check
     * @return true if the throwable is a ScanTimedOutException
     */
    public static boolean isScanTimedOutException(Throwable t) {
        return isExceptionOfType(t, SCAN_TIMED_OUT_EXCEPTION);
    }

    /**
     * Check if the throwable or any of its causes is one of the known Accumulo interruption exceptions (IterationInterruptedException, TabletClosedException,
     * ScanTimedOutException).
     *
     * @param t
     *            the throwable to check
     * @return true if any exception in the chain is an interruption exception
     */
    public static boolean isInterruptionException(Throwable t) {
        while (t != null) {
            if (isIterationInterruptedException(t) || isTabletClosedException(t) || isScanTimedOutException(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static boolean isExceptionOfType(Throwable t, String className) {
        return t != null && t.getClass().getName().equals(className);
    }
}
