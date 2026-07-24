package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import datawave.microservice.querymetric.config.TimelyProperties;

class QueryMetricOperationsStatsTest {

    @Test
    void formatStatsUsesLocaleNeutralDecimalsForTimely() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            QueryMetricOperationsStats stats = new QueryMetricOperationsStats(new TimelyProperties(), null, null, null, null);
            Map<String,Double> values = new LinkedHashMap<>();
            values.put("rate", 1234.5d);
            values.put("queryLatency", 1234.0d);

            Map<String,String> formatted = stats.formatStats(values, false);
            Map<String,String> displayFormatted = stats.formatStats(values, true);

            assertEquals("1234.50", formatted.get("rate"));
            assertEquals("1234", formatted.get("queryLatency"));
            assertEquals("1.234,50", displayFormatted.get("rate"));
            assertEquals("1.234", displayFormatted.get("queryLatency"));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }
}
