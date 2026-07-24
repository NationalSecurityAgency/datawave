package datawave.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

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
