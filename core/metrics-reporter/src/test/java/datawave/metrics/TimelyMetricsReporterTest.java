package datawave.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;

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

            assertTrue(reporter.metric.contains(" 123 1.500000 "));
        } finally {
            Locale.setDefault(originalLocale);
        }
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
