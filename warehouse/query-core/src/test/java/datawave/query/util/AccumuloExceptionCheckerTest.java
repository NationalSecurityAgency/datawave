package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanTimedOutException;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.junit.jupiter.api.Test;

public class AccumuloExceptionCheckerTest {

    @Test
    public void testIsTabletClosedException() {
        assertTrue(AccumuloExceptionChecker.isTabletClosedException(new TabletClosedException()));
        assertFalse(AccumuloExceptionChecker.isTabletClosedException(new RuntimeException()));
        assertFalse(AccumuloExceptionChecker.isTabletClosedException(null));
        // only the throwable itself is checked, callers walk the cause chain
        assertFalse(AccumuloExceptionChecker.isTabletClosedException(new RuntimeException(new TabletClosedException())));
    }

    @Test
    public void testIsScanTimedOutException() {
        assertTrue(AccumuloExceptionChecker.isScanTimedOutException(new ScanTimedOutException()));
        assertFalse(AccumuloExceptionChecker.isScanTimedOutException(new IOException()));
        assertFalse(AccumuloExceptionChecker.isScanTimedOutException(null));
        assertFalse(AccumuloExceptionChecker.isScanTimedOutException(new IOException(new ScanTimedOutException())));
    }
}
