package datawave.query.iterator;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.junit.jupiter.api.Test;

/**
 * QueryIterator must pass a TabletClosedException up to the tablet server unwrapped, so the client retries the scan elsewhere.
 */
public class QueryIteratorExceptionTest {

    private static void handleException(QueryIterator iterator, Exception e) throws Throwable {
        Method handleException = QueryIterator.class.getDeclaredMethod("handleException", Exception.class);
        handleException.setAccessible(true);
        try {
            handleException.invoke(iterator, e);
        } catch (InvocationTargetException ite) {
            throw ite.getCause();
        }
    }

    @Test
    public void testTabletClosedExceptionIsRethrown() {
        TabletClosedException tce = new TabletClosedException();
        Throwable thrown = assertThrows(TabletClosedException.class, () -> handleException(new QueryIterator(), tce));
        assertSame(tce, thrown);
    }

    @Test
    public void testWrappedTabletClosedExceptionIsUnwrappedAndRethrown() {
        TabletClosedException tce = new TabletClosedException();
        Exception wrapped = new RuntimeException("wrapper", new IllegalStateException("inner", tce));
        Throwable thrown = assertThrows(TabletClosedException.class, () -> handleException(new QueryIterator(), wrapped));
        assertSame(tce, thrown);
    }
}
