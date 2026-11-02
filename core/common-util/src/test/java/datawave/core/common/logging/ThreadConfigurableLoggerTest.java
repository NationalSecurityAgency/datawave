package datawave.core.common.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.event.Level;
import org.slf4j.helpers.AbstractLogger;
import org.slf4j.spi.LocationAwareLogger;
import org.slf4j.spi.LoggingEventBuilder;

class ThreadConfigurableLoggerTest {

    private LocationAwareLogger backend;
    private ThreadConfigurableLogger logger;

    @BeforeEach
    void setUp() {
        backend = mock(LocationAwareLogger.class);
        when(backend.getName()).thenReturn("thread-test");
        when(backend.isTraceEnabled()).thenReturn(true);
        when(backend.isDebugEnabled()).thenReturn(true);
        when(backend.isInfoEnabled()).thenReturn(true);
        when(backend.isWarnEnabled()).thenReturn(true);
        when(backend.isErrorEnabled()).thenReturn(true);
        logger = new ThreadConfigurableLogger(backend);
        clearInvocations(backend);
    }

    @AfterEach
    void cleanUp() {
        ThreadConfigurableLogger.clearThreadLevels();
    }

    @Test
    void filtersEveryLevelAndEnabledCheck() {
        logger.setLevelForThread(Level.WARN);
        assertFalse(logger.isTraceEnabled());
        assertFalse(logger.isDebugEnabled());
        assertFalse(logger.isInfoEnabled());
        logger.trace("trace");
        logger.debug("debug {}", 1);
        logger.info("info", new RuntimeException("filtered"));
        verifyNoInteractions(backend);

        assertTrue(logger.isWarnEnabled());
        assertTrue(logger.isErrorEnabled());
        logger.warn("warn");
        logger.error("error");
        verify(backend).log(null, AbstractLogger.class.getName(), Level.WARN.toInt(), "warn", null, null);
        verify(backend).log(null, AbstractLogger.class.getName(), Level.ERROR.toInt(), "error", null, null);
    }

    @Test
    void preservesMarkersArgumentsCausesAndCallerBoundary() {
        Marker marker = MarkerFactory.getMarker("thread-marker");
        when(backend.isErrorEnabled(marker)).thenReturn(true);
        RuntimeException cause = new RuntimeException("cause");

        logger.error(marker, "failed {} {}", "first", "second", cause);

        verify(backend).log(eq(marker), eq(AbstractLogger.class.getName()), eq(Level.ERROR.toInt()), eq("failed {} {}"), eq(new Object[] {"first", "second"}),
                        eq(cause));
    }

    @Test
    void respectsMarkerSpecificBackendLevels() {
        Marker marker = MarkerFactory.getMarker("enabled-marker");
        when(backend.isDebugEnabled()).thenReturn(false);
        when(backend.isDebugEnabled(marker)).thenReturn(true);

        assertFalse(logger.isDebugEnabled());
        logger.debug(marker, "marker message");
        verify(backend).log(marker, AbstractLogger.class.getName(), Level.DEBUG.toInt(), "marker message", null, null);

        logger.setLevelForThread(Level.INFO);
        assertFalse(logger.isDebugEnabled(marker));
    }

    @Test
    void cannotEnableALevelDisabledByTheBackend() {
        when(backend.isDebugEnabled()).thenReturn(false);
        logger.setLevelForThread(Level.TRACE);

        assertFalse(logger.isDebugEnabled());
        logger.debug("disabled");
        verify(backend, never()).log(any(), any(), eq(Level.DEBUG.toInt()), any(), any(), any());
    }

    @Test
    void suppressesFluentSuppliersWhenDisabledAndRestoresLogging() {
        AtomicBoolean evaluated = new AtomicBoolean();
        ThreadConfigurableLogger.disableForThread(logger.getName());
        logger.atError().log(() -> {
            evaluated.set(true);
            return "expensive";
        });
        assertFalse(evaluated.get());
        verifyNoInteractions(backend);

        logger.clearLevelForThread();
        assertTrue(logger.isErrorEnabled());
        logger.setLevelForThread(Level.INFO);
        assertEquals(Level.INFO, logger.getLevelForThread());
        logger.setLevelForThread(null);
        assertNull(logger.getLevelForThread());
    }

    @Test
    void sharesOverridesByNameWithoutAffectingOtherThreadsAndClearsReusedThreads() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            logger.setLevelForThread(Level.ERROR);
            ThreadConfigurableLogger other = new ThreadConfigurableLogger(backend);
            assertFalse(other.isInfoEnabled());

            Future<Boolean> first = executor.submit(() -> {
                assertTrue(logger.isInfoEnabled());
                logger.setLevelForThread(Level.ERROR);
                assertFalse(other.isInfoEnabled());
                ThreadConfigurableLogger.clearThreadLevels();
                return logger.isInfoEnabled();
            });
            assertTrue(first.get());
            assertTrue(executor.submit(() -> logger.isInfoEnabled()).get());
            assertEquals(Level.ERROR, logger.getLevelForThread());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void delegatesToProvidersWithoutLocationAwareLogging() {
        Logger plainBackend = mock(Logger.class);
        when(plainBackend.getName()).thenReturn("plain-backend");
        when(plainBackend.isWarnEnabled()).thenReturn(true);
        Marker marker = MarkerFactory.getMarker("plain-marker");
        when(plainBackend.isWarnEnabled(marker)).thenReturn(true);
        LoggingEventBuilder builder = mock(LoggingEventBuilder.class);
        when(plainBackend.atLevel(Level.WARN)).thenReturn(builder);
        when(builder.setCause(any())).thenReturn(builder);
        RuntimeException cause = new RuntimeException("plain cause");

        new ThreadConfigurableLogger(plainBackend).warn(marker, "plain {}", "argument", cause);

        verify(builder).setCause(cause);
        verify(builder).addMarker(marker);
        verify(builder).log("plain {}", new Object[] {"argument"});

        new ThreadConfigurableLogger(plainBackend).warn("plain message");
        verify(builder).log("plain message");
    }

    @Test
    void preservesFluentMarkersKeyValuesAndCauses() {
        LoggingEventBuilder builder = mock(LoggingEventBuilder.class);
        when(backend.atLevel(Level.INFO)).thenReturn(builder);
        RuntimeException cause = new RuntimeException("fluent cause");
        when(builder.setCause(cause)).thenReturn(builder);
        Marker marker = MarkerFactory.getMarker("fluent-marker");

        logger.atInfo().addMarker(marker).addKeyValue("request", "id").setCause(cause).log("event {}", 42);

        verify(builder).setCause(cause);
        verify(builder).addMarker(marker);
        verify(builder).addKeyValue("request", "id");
        verify(builder).log("event {}", new Object[] {42});
    }
}
