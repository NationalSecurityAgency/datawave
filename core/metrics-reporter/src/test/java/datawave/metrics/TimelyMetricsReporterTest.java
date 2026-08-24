package datawave.metrics;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;
import org.slf4j.event.SubstituteLoggingEvent;
import org.slf4j.helpers.SubstituteLogger;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

class TimelyMetricsReporterTest {

    @Test
    void reportMetricUsesLocaleNeutralDecimal() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

            reporter.reportMetric("metric", "value", 1.5d, "GAUGE", null, 123L);

            assertTrue(reporter.metric.contains(" 123 1.5 "));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @Test
    void reportMetricPreservesDecimalPrecision() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

        reporter.reportMetric("metric", "value", 0.0000004d, "GAUGE", null, 123L);
        assertEquals(0.0000004d, metricValue(reporter.metric));

        reporter.reportMetric("metric", "value", 1.23456789d, "GAUGE", null, 123L);
        assertEquals(1.23456789d, metricValue(reporter.metric));
    }

    @Test
    void reportGaugePreservesNumberPrecision() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

        reporter.reportGauge("metric", () -> new BigDecimal("0.0000004"), 123L);

        assertEquals(0.0000004d, metricValue(reporter.metric));
    }

    @Test
    void reportGaugeFallsBackToNumberValueWhenTextIsNotNumeric() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

        reporter.reportGauge("metric", () -> new NonNumericTextNumber(0.5d), 123L);

        assertEquals(0.5d, metricValue(reporter.metric));
    }

    @Test
    void reportGaugeSkipsNullAndNonNumericValues() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();
        Queue<SubstituteLoggingEvent> events = new LinkedBlockingQueue<>();
        reporter.logger = new SubstituteLogger("test", events, false);

        reporter.reportGauge("null", () -> null, 123L);
        reporter.reportGauge("text", () -> "UP", 123L);
        reporter.reportGauge("boolean", () -> true, 123L);

        assertNull(reporter.metric);
        assertWarning(events.remove(), "text", String.class);
        assertWarning(events.remove(), "boolean", Boolean.class);
        assertTrue(events.isEmpty());
    }

    @Test
    void reportGaugeContinuesAfterNonNumericValue() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

        reporter.reportGauge("invalid", () -> "UP", 123L);
        reporter.reportGauge("integer", () -> 7, 123L);
        assertEquals(7d, metricValue(reporter.metric));

        reporter.reportGauge("floating", () -> 1.25d, 123L);
        assertEquals(1.25d, metricValue(reporter.metric));
    }

    @Test
    void reportMetricNormalizesWhitespaceInMetricName() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

        reporter.reportMetric("metric name\twith\nwhitespace\u2003", "value", 1L, "COUNTER", null, 123L);

        assertTrue(reporter.metric.startsWith("put metric_name_with_whitespace_ 123 1 "));
        assertEquals(1, reporter.metric.chars().filter(character -> character == '\n').count());
    }

    @Test
    void reportMetricNormalizesWhitespaceInValue() {
        TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

        reporter.reportMetric("metric", "value", "1\n2", "GAUGE", null, 123L);

        assertTrue(reporter.metric.startsWith("put metric 123 1_2 "));
        assertEquals(1, reporter.metric.chars().filter(character -> character == '\n').count());
    }

    @Test
    void reportMetricUsesAsciiIntegerAndTimestampDigits() {
        Locale originalLocale = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));
            TestTimelyMetricsReporter reporter = new TestTimelyMetricsReporter();

            reporter.reportMetric("metric", "value", 456L, "COUNTER", null, 123L);

            assertTrue(reporter.metric.contains(" 123 456 "));
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalLocale);
        }
    }

    private static double metricValue(String metric) {
        return Double.parseDouble(metric.split(" ")[3]);
    }

    private static void assertWarning(SubstituteLoggingEvent event, String metricName, Class<?> valueType) {
        assertEquals(Level.WARN, event.getLevel());
        assertEquals("Skipping non-numeric Timely gauge {} with value type {}", event.getMessage());
        assertArrayEquals(new Object[] {metricName, valueType.getName()}, event.getArgumentArray());
    }

    private static class NonNumericTextNumber extends Number {
        private final double value;

        private NonNumericTextNumber(double value) {
            this.value = value;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public double doubleValue() {
            return value;
        }

        @Override
        public String toString() {
            return "not-a-number";
        }
    }

    private static class TestTimelyMetricsReporter extends TimelyMetricsReporter {
        private String metric;

        private TestTimelyMetricsReporter() {
            super("localhost", 0, new MetricRegistry(), "test", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void reportMetric(String timelyMetric) {
            metric = timelyMetric;
        }
    }
}
