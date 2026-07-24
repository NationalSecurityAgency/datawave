package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
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
            values.put("smallRate", 0.0000004d);

            Map<String,String> formatted = stats.formatStats(values, false);
            Map<String,String> displayFormatted = stats.formatStats(values, true);

            assertEquals("1234.5", formatted.get("rate"));
            assertEquals("1234.0", formatted.get("queryLatency"));
            assertEquals("4.0E-7", formatted.get("smallRate"));
            assertEquals("1.234,50", displayFormatted.get("rate"));
            assertEquals("1.234", displayFormatted.get("queryLatency"));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @Test
    void commonTagsNormalizeWhitespace() {
        TimelyProperties properties = new TimelyProperties();
        properties.getTags().put("region zone", "us\teast\n1\u2003");
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);

        assertTrue(stats.getCommonTags().contains(" region_zone=us_east_1_"));
        assertEquals("host_name_with_whitespace", QueryMetricOperationsStats.sanitizeTimelyToken("host name\twith\nwhitespace"));

        properties.getTags().put("region_zone", "replacement");
        assertThrows(IllegalArgumentException.class, stats::getCommonTags);
    }

    @Test
    void queryTagsNormalizeBeforeCommandsAndAggregation() {
        TimelyProperties properties = new TimelyProperties();
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);
        properties.setEnabled(true);

        QueryMetric closedMetric = newMetric(BaseQueryMetric.Lifecycle.CLOSED, "host name", "user\nname", "logic\tname");
        closedMetric.setCreateDate(new Date(123L));
        stats.queueTimelyMetrics(closedMetric);

        assertEquals(6, stats.queryStatsToWriteToTimely.size());
        assertTrue(stats.queryStatsToWriteToTimely.stream().allMatch(metric -> metric.chars().filter(character -> character == '\n').count() == 1));
        assertTrue(stats.queryStatsToWriteToTimely.stream().anyMatch(metric -> metric.contains(" HOST=host_name")));
        assertTrue(stats.queryStatsToWriteToTimely.stream().anyMatch(metric -> metric.contains(" USER=user_name")));
        assertTrue(stats.queryStatsToWriteToTimely.stream().anyMatch(metric -> metric.contains(" QUERY_LOGIC=logic_name")));

        stats.queueTimelyMetrics(newMetric(BaseQueryMetric.Lifecycle.INITIALIZED, "host name", "user name", "logic name"));
        stats.queueTimelyMetrics(newMetric(BaseQueryMetric.Lifecycle.INITIALIZED, "host_name", "user_name", "logic_name"));

        assertEquals(Long.valueOf(2L), stats.hostCountMap.get("host_name"));
        assertEquals(Long.valueOf(2L), stats.userCountMap.get("user_name"));
        assertEquals(Long.valueOf(2L), stats.logicCountMap.get("logic_name"));
    }

    private static QueryMetric newMetric(BaseQueryMetric.Lifecycle lifecycle, String host, String user, String logic) {
        QueryMetric metric = new QueryMetric();
        metric.setQueryType("RunningQuery");
        metric.setLifecycle(lifecycle);
        metric.setHost(host);
        metric.setUser(user);
        metric.setQueryLogic(logic);
        return metric;
    }

}
