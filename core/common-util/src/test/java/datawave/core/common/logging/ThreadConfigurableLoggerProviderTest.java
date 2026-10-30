package datawave.core.common.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import datawave.common.test.logging.TestLogCollectorJUnit5;

class ThreadConfigurableLoggerProviderTest {

    @RegisterExtension
    final TestLogCollectorJUnit5 collector = new TestLogCollectorJUnit5.Builder().with(getClass(), "TRACE").build();

    private final Logger logger = new ThreadConfigurableLogger(LoggerFactory.getLogger(getClass()));
    private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    private final List<LogEvent> events = new ArrayList<>();
    private Appender appender;

    @BeforeEach
    void attachAppender() {
        appender = new AbstractAppender("ThreadLoggerEvents", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public boolean requiresLocation() {
                return true;
            }

            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        context.getLogger(logger.getName()).addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        context.getLogger(logger.getName()).removeAppender(appender);
        appender.stop();
        ThreadConfigurableLogger.clearThreadLevels();
    }

    @Test
    void retainsClassicCallerMarkersAndExceptions() {
        RuntimeException cause = new RuntimeException("classic cause");
        Marker marker = MarkerFactory.getMarker("classic-marker");
        logger.error(marker, "classic {}", "value", cause);

        assertEquals(1, events.size());
        LogEvent event = events.get(0);
        assertEquals("classic value", event.getMessage().getFormattedMessage());
        assertSame(cause, event.getThrown());
        assertEquals("classic-marker", event.getMarker().getName());
        assertCaller(event, "retainsClassicCallerMarkersAndExceptions");
    }

    @Test
    void retainsFluentCallerMarkersKeyValuesAndExceptions() {
        RuntimeException cause = new RuntimeException("fluent cause");
        Marker marker = MarkerFactory.getMarker("fluent-marker");
        logger.atInfo().addMarker(marker).addKeyValue("request", "id").setCause(cause).log("fluent {}", 42);

        assertEquals(1, events.size());
        LogEvent event = events.get(0);
        assertEquals("fluent 42", event.getMessage().getFormattedMessage());
        assertSame(cause, event.getThrown());
        assertEquals("fluent-marker", event.getMarker().getName());
        assertEquals("id", event.getContextData().<String> getValue("request"));
        assertCaller(event, "retainsFluentCallerMarkersKeyValuesAndExceptions");
    }

    private void assertCaller(LogEvent event, String method) {
        assertNotNull(event.getSource());
        assertEquals(getClass().getName(), event.getSource().getClassName());
        assertEquals(method, event.getSource().getMethodName());
    }
}
