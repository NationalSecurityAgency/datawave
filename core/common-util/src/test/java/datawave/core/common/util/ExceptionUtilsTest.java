package datawave.core.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ExceptionUtilsTest {

    /**
     * A throwable whose {@code getCause()} returns itself, which {@link Throwable#initCause(Throwable)} does not allow. It counts the calls to
     * {@code getCause()}, so a test can tell where a search stopped.
     */
    private static class SelfCausedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private int getCauseCalls;

        @Override
        public synchronized Throwable getCause() {
            getCauseCalls++;
            return this;
        }
    }

    /**
     * Wraps {@code root} in {@code wrappers} RuntimeExceptions, so that {@code root} ends up at depth {@code wrappers}.
     */
    private static Throwable wrap(Throwable root, int wrappers) {
        Throwable t = root;
        for (int i = 0; i < wrappers; i++) {
            t = new RuntimeException("wrapper " + i, t);
        }
        return t;
    }

    @Test
    public void testDirectMatch() {
        IOException ioe = new IOException();
        assertSame(ioe, ExceptionUtils.findCause(ioe, IOException.class));
        assertTrue(ExceptionUtils.containsCause(ioe, IOException.class));
    }

    @Test
    public void testNestedCause() {
        IOException ioe = new IOException();
        Throwable t = new RuntimeException(new IllegalStateException(ioe));
        assertSame(ioe, ExceptionUtils.findCause(t, IOException.class));
        assertTrue(ExceptionUtils.containsCause(t, IOException.class));
    }

    @Test
    public void testClosestMatchIsReturned() {
        IOException inner = new IOException("inner");
        IOException outer = new IOException("outer", new RuntimeException(inner));
        assertSame(outer, ExceptionUtils.findCause(outer, IOException.class));
        assertSame(inner, ExceptionUtils.findCause(outer.getCause(), IOException.class));
    }

    @Test
    public void testSubclassMatches() {
        FileNotFoundException fnfe = new FileNotFoundException();
        assertSame(fnfe, ExceptionUtils.findCause(fnfe, IOException.class));
        assertSame(fnfe, ExceptionUtils.findCause(new RuntimeException(fnfe), IOException.class));
        assertTrue(ExceptionUtils.containsCause(new RuntimeException(fnfe), IOException.class));
    }

    @Test
    public void testSuperclassDoesNotMatch() {
        assertNull(ExceptionUtils.findCause(new IOException(), FileNotFoundException.class));
        assertFalse(ExceptionUtils.containsCause(new RuntimeException(new IOException()), FileNotFoundException.class));
    }

    @Test
    public void testNoMatch() {
        Throwable t = new IOException(new RuntimeException());
        assertNull(ExceptionUtils.findCause(t, IllegalStateException.class));
        assertFalse(ExceptionUtils.containsCause(t, IllegalStateException.class));
        assertNull(ExceptionUtils.findCause(new RuntimeException(), IOException.class));
        assertFalse(ExceptionUtils.containsCause(new RuntimeException(), IOException.class));
    }

    @Test
    public void testNullThrowable() {
        assertNull(ExceptionUtils.findCause(null, IOException.class));
        assertNull(ExceptionUtils.findCause(null, IOException.class, 5));
        assertFalse(ExceptionUtils.containsCause(null, IOException.class));
        assertFalse(ExceptionUtils.containsCause(null, IOException.class, 5));
    }

    @Test
    @Timeout(10)
    public void testSelfCausedThrowableEndsTheSearch() {
        SelfCausedException selfCaused = new SelfCausedException();
        // maxDepth cannot end this search, so the self-cause check must
        assertNull(ExceptionUtils.findCause(selfCaused, IOException.class, Integer.MAX_VALUE));
        assertEquals(1, selfCaused.getCauseCalls);

        // the self-caused throwable is still examined
        Exception wrapper = new Exception(selfCaused);
        assertSame(selfCaused, ExceptionUtils.findCause(wrapper, SelfCausedException.class));
        assertFalse(ExceptionUtils.containsCause(wrapper, IOException.class));
    }

    @Test
    @Timeout(10)
    public void testTwoExceptionCycle() {
        IOException a = new IOException("a");
        IllegalStateException b = new IllegalStateException("b");
        a.initCause(b);
        b.initCause(a);
        assertSame(b, a.getCause());
        assertSame(a, b.getCause());

        assertSame(b, ExceptionUtils.findCause(a, IllegalStateException.class));
        assertSame(a, ExceptionUtils.findCause(b, IOException.class));

        // a type outside the cycle is searched for until maxDepth throwables have been examined
        assertNull(ExceptionUtils.findCause(a, FileNotFoundException.class));
        assertFalse(ExceptionUtils.containsCause(a, FileNotFoundException.class));
        assertFalse(ExceptionUtils.containsCause(b, FileNotFoundException.class, 1000));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 10, 99})
    public void testMaxDepthBoundary(int depth) {
        IOException ioe = new IOException();
        Throwable t = wrap(ioe, depth);

        // the match is the (depth + 1)th throwable examined
        assertSame(ioe, ExceptionUtils.findCause(t, IOException.class, depth + 1));
        assertTrue(ExceptionUtils.containsCause(t, IOException.class, depth + 1));

        assertNull(ExceptionUtils.findCause(t, IOException.class, depth));
        assertFalse(ExceptionUtils.containsCause(t, IOException.class, depth));
    }

    @Test
    public void testMaxDepthZeroExaminesNothing() {
        IOException ioe = new IOException();
        assertNull(ExceptionUtils.findCause(ioe, IOException.class, 0));
        assertFalse(ExceptionUtils.containsCause(ioe, IOException.class, 0));
    }

    @Test
    public void testDefaultMaxDepth() {
        assertEquals(100, ExceptionUtils.DEFAULT_MAX_DEPTH);

        IOException deepest = new IOException();
        Throwable t = wrap(deepest, ExceptionUtils.DEFAULT_MAX_DEPTH - 1);
        assertSame(deepest, ExceptionUtils.findCause(t, IOException.class));
        assertTrue(ExceptionUtils.containsCause(t, IOException.class));

        Throwable tooDeep = wrap(new IOException(), ExceptionUtils.DEFAULT_MAX_DEPTH);
        assertNull(ExceptionUtils.findCause(tooDeep, IOException.class));
        assertFalse(ExceptionUtils.containsCause(tooDeep, IOException.class));
    }

    @Test
    public void testNegativeMaxDepthIsRejected() {
        IOException ioe = new IOException();
        assertThrows(IllegalArgumentException.class, () -> ExceptionUtils.findCause(ioe, IOException.class, -1));
        assertThrows(IllegalArgumentException.class, () -> ExceptionUtils.containsCause(ioe, IOException.class, -1));
        assertThrows(IllegalArgumentException.class, () -> ExceptionUtils.findCause(null, IOException.class, Integer.MIN_VALUE));
    }

    @Test
    public void testNullTypeIsRejected() {
        assertThrows(NullPointerException.class, () -> ExceptionUtils.findCause(new IOException(), null));
        assertThrows(NullPointerException.class, () -> ExceptionUtils.containsCause(null, null));
    }

    @Test
    public void testConstructorThrows() throws NoSuchMethodException {
        Constructor<ExceptionUtils> constructor = ExceptionUtils.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        InvocationTargetException thrown = assertThrows(InvocationTargetException.class, constructor::newInstance);
        assertInstanceOf(UnsupportedOperationException.class, thrown.getCause());
    }
}
