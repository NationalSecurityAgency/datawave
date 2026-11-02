package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
        assertTrue(AccumuloExceptionChecker.isTabletClosedException(new RuntimeException(new TabletClosedException())));
    }

    @Test
    public void testFindTabletClosedException() {
        TabletClosedException tce = new TabletClosedException();
        assertSame(tce, AccumuloExceptionChecker.findTabletClosedException(tce));
        assertSame(tce, AccumuloExceptionChecker.findTabletClosedException(new IOException(new RuntimeException(tce))));
        assertNull(AccumuloExceptionChecker.findTabletClosedException(new IOException(new RuntimeException())));
        assertNull(AccumuloExceptionChecker.findTabletClosedException(null));
    }

    @Test
    public void testIsScanTimedOutException() {
        assertTrue(AccumuloExceptionChecker.isScanTimedOutException(new ScanTimedOutException("timed out")));
        assertFalse(AccumuloExceptionChecker.isScanTimedOutException(new IOException()));
        assertFalse(AccumuloExceptionChecker.isScanTimedOutException(null));
        assertTrue(AccumuloExceptionChecker.isScanTimedOutException(new IOException(new ScanTimedOutException("timed out"))));
        assertFalse(AccumuloExceptionChecker.isScanTimedOutException(new IOException(new RuntimeException())));
    }
}
