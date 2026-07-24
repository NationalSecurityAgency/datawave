package datawave.microservice.querymetric;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.microservice.querymetric.config.TimelyProperties;
import datawave.util.timely.TcpClient;
import datawave.util.timely.UdpClient;

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

    @Test
    void tcpMetricsRemainQueuedUntilFlushSucceeds() {
        TimelyProperties properties = new TimelyProperties();
        properties.setProtocol(TimelyProperties.Protocol.TCP);
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);
        TcpClient client = mock(TcpClient.class);
        doThrow(new IllegalStateException("flush failed")).doNothing().when(client).flush();
        ReflectionTestUtils.setField(stats, "timelyTcpClient", client);
        properties.setEnabled(true);
        stats.queryStatsToWriteToTimely.add("put first 1 1\n");
        stats.queryStatsToWriteToTimely.add("put second 1 2\n");

        stats.writeQueryStatsToTimely();
        assertEquals(2, stats.queryStatsToWriteToTimely.size());

        stats.writeQueryStatsToTimely();
        assertTrue(stats.queryStatsToWriteToTimely.isEmpty());
        verify(client, times(2)).flush();
    }

    @Test
    void tcpMetricsRemainQueuedAfterWriteFailure() throws IOException {
        TimelyProperties properties = new TimelyProperties();
        properties.setProtocol(TimelyProperties.Protocol.TCP);
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);
        TcpClient client = mock(TcpClient.class);
        doThrow(new IOException("write failed")).doNothing().when(client).write("first");
        ReflectionTestUtils.setField(stats, "timelyTcpClient", client);
        properties.setEnabled(true);
        stats.queryStatsToWriteToTimely.addAll(Arrays.asList("first", "second"));

        stats.writeQueryStatsToTimely();
        assertEquals(Arrays.asList("first", "second"), stats.queryStatsToWriteToTimely);

        stats.writeQueryStatsToTimely();
        assertTrue(stats.queryStatsToWriteToTimely.isEmpty());
        verify(client, times(2)).write("first");
        verify(client).write("second");
        verify(client).flush();
    }

    @Test
    void udpMetricsOnlyRequeueUnsentRemainder() throws IOException {
        TimelyProperties properties = new TimelyProperties();
        properties.setProtocol(TimelyProperties.Protocol.UDP);
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);
        UdpClient client = mock(UdpClient.class);
        doNothing().doThrow(new IOException("send failed")).when(client).write(anyString());
        ReflectionTestUtils.setField(stats, "timelyUdpClient", client);
        properties.setEnabled(true);
        stats.queryStatsToWriteToTimely.addAll(Arrays.asList("first", "second", "third"));

        stats.writeQueryStatsToTimely();

        assertEquals(Arrays.asList("second", "third"), stats.queryStatsToWriteToTimely);
    }

    @Test
    void udpServiceMetricsRetryFailedOpen() throws IOException {
        TimelyProperties properties = new TimelyProperties();
        properties.setProtocol(TimelyProperties.Protocol.UDP);
        QueryMetricOperationsStats stats = serviceStats(properties);
        UdpClient client = mock(UdpClient.class);
        doThrow(new IOException("open failed")).doNothing().when(client).open();
        ReflectionTestUtils.setField(stats, "timelyUdpClient", client);
        properties.setEnabled(true);

        stats.writeServiceStatsToTimely();
        stats.writeServiceStatsToTimely();

        verify(client, times(2)).open();
        verify(client).write(anyString());
    }

    @Test
    void udpQueryMetricsRemainQueuedUntilOpenSucceeds() throws IOException {
        TimelyProperties properties = new TimelyProperties();
        properties.setProtocol(TimelyProperties.Protocol.UDP);
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);
        UdpClient client = mock(UdpClient.class);
        doThrow(new IOException("open failed")).doNothing().when(client).open();
        ReflectionTestUtils.setField(stats, "timelyUdpClient", client);
        properties.setEnabled(true);
        stats.queryStatsToWriteToTimely.add("metric");

        stats.writeQueryStatsToTimely();
        assertEquals(Collections.singletonList("metric"), stats.queryStatsToWriteToTimely);

        stats.writeQueryStatsToTimely();
        assertTrue(stats.queryStatsToWriteToTimely.isEmpty());
        verify(client, times(2)).open();
        verify(client).write("metric");
    }

    @Test
    void oversizedUdpMetricDoesNotBlockValidMetrics() throws IOException {
        TimelyProperties properties = new TimelyProperties();
        properties.setProtocol(TimelyProperties.Protocol.UDP);
        QueryMetricOperationsStats stats = new QueryMetricOperationsStats(properties, null, null, null, null);
        UdpClient client = mock(UdpClient.class);
        ReflectionTestUtils.setField(stats, "timelyUdpClient", client);
        properties.setEnabled(true);
        String oversized = "\u00e9".repeat(32_754);
        assertTrue(oversized.length() < 65_507);
        assertTrue(oversized.getBytes(UTF_8).length > 65_507);
        stats.queryStatsToWriteToTimely.addAll(Arrays.asList(oversized, "valid"));

        stats.writeQueryStatsToTimely();

        assertTrue(stats.queryStatsToWriteToTimely.isEmpty());
        verify(client, never()).write(oversized);
        verify(client).write("valid");
    }

    private static QueryMetricOperationsStats serviceStats(TimelyProperties properties) {
        return new QueryMetricOperationsStats(properties, null, null, null, null) {
            @Override
            public Map<String,Double> getServiceStats() {
                return Collections.singletonMap("count", 1d);
            }

            @Override
            protected void addCacheStats(Map<String,Double> serviceStats) {}
        };
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
