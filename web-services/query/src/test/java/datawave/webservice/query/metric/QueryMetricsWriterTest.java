package datawave.webservice.query.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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

            assertEquals("put metric 123 456 host=test\n", QueryMetricsWriter.formatTimelyMetric("metric", 123L, 456L, "host=test"));
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalLocale);
        }
    }

    @Test
    void formatTimelyTagsNormalizesWhitespaceInNamesAndValues() {
        Set<String> fields = new LinkedHashSet<>();
        fields.add("region zone");
        Map<String,String> values = new LinkedHashMap<>();
        values.put("region zone", "us\teast\n1\u2003");

        String tags = QueryMetricsWriter.formatTimelyTags(fields, values);

        assertEquals("region_zone=us_east_1_", tags);
        assertEquals("put metric 123 1 region_zone=us_east_1_\n", QueryMetricsWriter.formatTimelyMetric("metric", 123L, 1, tags));
        assertThrows(IllegalArgumentException.class, () -> QueryMetricsWriter.formatTimelyMetric("metric", 123L, 1, "region=test\n"));

        fields.add("region_zone");
        values.put("region_zone", "replacement");
        assertThrows(IllegalArgumentException.class, () -> QueryMetricsWriter.formatTimelyTags(fields, values));
    }
}
