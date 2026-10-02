package datawave.webservice.query.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.core.query.metric.QueryMetricHandler;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.QueryMetric;
import datawave.util.timely.UdpClient;

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

    @Test
    void refreshWaitsForTimelyBatchAndClosesClients() throws Exception {
        CountDownLatch releaseBatch = new CountDownLatch(1);
        CountDownLatch batchOpened = new CountDownLatch(1);
        CountDownLatch replacementCreated = new CountDownLatch(1);
        CountDownLatch releaseLateReplacement = new CountDownLatch(1);
        UdpClient original = mock(UdpClient.class);
        UdpClient replacement = mock(UdpClient.class);
        UdpClient lateReplacement = mock(UdpClient.class);
        doAnswer(invocation -> {
            batchOpened.countDown();
            if (!releaseBatch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting to send");
            }
            return null;
        }).when(original).open();
        QueryMetricsWriter writer = spy(new QueryMetricsWriter());
        doAnswer(invocation -> {
            replacementCreated.countDown();
            return replacement;
        }).when(writer).createUdpClient();
        QueryMetricsWriterConfiguration config = new QueryMetricsWriterConfiguration();
        config.setMaxShutdownMs(1);
        QueryMetricHandler<?> handler = mock(QueryMetricHandler.class);
        QueryMetric metric = initializedMetric();
        when(handler.getEventFields(metric)).thenReturn(Collections.emptyMap());
        ReflectionTestUtils.setField(writer, "writerConfig", config);
        ReflectionTestUtils.setField(writer, "queryMetricHandler", handler);
        ReflectionTestUtils.setField(writer, "timelyClient", original);
        LinkedBlockingQueue<QueryMetricHolder> queue = new LinkedBlockingQueue<>();
        ReflectionTestUtils.setField(writer, "blockingQueue", queue);
        QueryMetricsWriter.MetricProcessor processor = writer.new MetricProcessor(queue);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> send = executor.submit(() -> processor.sendMetricsToTimely(metric));
            assertTrue(batchOpened.await(5, TimeUnit.SECONDS));
            Future<?> refresh = executor.submit(() -> writer.onRefresh(null, null));
            assertTrue(replacementCreated.await(5, TimeUnit.SECONDS));
            assertFalse(refresh.isDone());

            releaseBatch.countDown();
            send.get(5, TimeUnit.SECONDS);
            refresh.get(5, TimeUnit.SECONDS);

            CountDownLatch lateReplacementCreated = new CountDownLatch(1);
            doAnswer(invocation -> {
                lateReplacementCreated.countDown();
                releaseLateReplacement.await(5, TimeUnit.SECONDS);
                return lateReplacement;
            }).when(writer).createUdpClient();
            Future<?> lateRefresh = executor.submit(() -> writer.onRefresh(null, null));
            assertTrue(lateReplacementCreated.await(5, TimeUnit.SECONDS));

            writer.shutdown();
            releaseLateReplacement.countDown();
            lateRefresh.get(5, TimeUnit.SECONDS);
        } finally {
            releaseBatch.countDown();
            releaseLateReplacement.countDown();
            executor.shutdownNow();
        }

        InOrder order = inOrder(original);
        order.verify(original).open();
        order.verify(original, times(2)).write(anyString());
        order.verify(original).close();
        verify(replacement, never()).write(anyString());
        verify(replacement).close();
        verify(lateReplacement).close();
        assertNull(ReflectionTestUtils.getField(writer, "timelyClient"));
    }

    private static QueryMetric initializedMetric() {
        QueryMetric metric = new QueryMetric();
        metric.setQueryType("RunningQuery");
        metric.setQueryId("query-id");
        metric.setLifecycle(Lifecycle.INITIALIZED);
        metric.setCreateDate(new Date(123L));
        metric.setCreateCallTime(5L);
        return metric;
    }
}
