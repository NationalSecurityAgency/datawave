package datawave.webservice.query.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Locale;

import org.junit.jupiter.api.Test;

class QueryMetricsWriterTest {

    @Test
    void formatCallTimePerRecordUsesLocaleNeutralDecimal() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);

            QueryMetricsWriter writer = new QueryMetricsWriter();
            assertEquals("1.5", writer.formatCallTimePerRecord(3, 2));
            assertEquals("0.004", writer.formatCallTimePerRecord(1, 250));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @Test
    void formatTimelyMetricUsesAsciiIntegerAndTimestampDigits() {
        Locale originalLocale = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));

            assertEquals("put metric 123 456 host=test\n", QueryMetricsWriter.formatTimelyMetric("metric", 123L, 456L, "host=test\n"));
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalLocale);
        }
    }
}
