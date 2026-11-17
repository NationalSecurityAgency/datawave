package datawave.common.test.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestLogCollectorTest {

    private static class NestedLogger {}

    private static final Logger log = LoggerFactory.getLogger(NestedLogger.class);

    @Test
    void capturesFormattedSlf4jMessagesAndRestoresLevelsAcrossInvocations() {
        Level oldLevel = LogManager.getLogger(NestedLogger.class.getName()).getLevel();
        TestLogCollectorJUnit5 collector = new TestLogCollectorJUnit5.Builder().with(NestedLogger.class, Level.TRACE).build();
        assertEquals(oldLevel, LogManager.getLogger(NestedLogger.class.getName()).getLevel());

        for (int i = 0; i < 2; i++) {
            collector.beforeEach(null);
            try {
                log.debug("value {} {}", i, "argument");
                log.info("literal {} braces", new RuntimeException("cause"));
                assertEquals(List.of("value " + i + " argument", "literal {} braces"), collector.getMessages());
            } finally {
                collector.afterEach(null);
            }
            assertTrue(collector.getMessages().isEmpty());
            assertEquals(oldLevel, LogManager.getLogger(NestedLogger.class.getName()).getLevel());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void junit4CollectorCapturesSlf4jAndRestoresItsLevel() {
        Level oldLevel = LogManager.getLogger(NestedLogger.class.getName()).getLevel();
        TestLogCollector collector = new TestLogCollector.Builder().with(NestedLogger.class, "DEBUG").build();
        assertEquals(oldLevel, LogManager.getLogger(NestedLogger.class.getName()).getLevel());
        collector.before();
        try {
            log.debug("legacy rule {}", "message");
            assertEquals(List.of("legacy rule message"), collector.getMessages());
        } finally {
            collector.after();
        }
        assertTrue(collector.getMessages().isEmpty());
        assertEquals(oldLevel, LogManager.getLogger(NestedLogger.class.getName()).getLevel());
    }
}
