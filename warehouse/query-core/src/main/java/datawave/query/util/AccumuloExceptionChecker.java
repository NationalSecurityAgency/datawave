package datawave.query.util;

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

    private AccumuloExceptionChecker() {}

    public static boolean isTabletClosedException(Throwable t) {
        return t instanceof TabletClosedException;
    }

    public static boolean isScanTimedOutException(Throwable t) {
        return t instanceof ScanTimedOutException;
    }
}
