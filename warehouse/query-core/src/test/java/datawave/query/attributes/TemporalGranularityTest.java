package datawave.query.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.util.AssertionErrors.assertFalse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TemporalGranularityTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testAll() {
        assertEquals("ALL", TemporalGranularity.ALL.getName());
        assertNull(TemporalGranularity.ALL.transform(null));
        assertEquals("nonNullValue", TemporalGranularity.ALL.transform("nonNullValue"));
    }

    @Test
    public void testTruncateTemporalToDay() {
        assertEquals("DAY", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("nonDateValue"));
        assertEquals("2019-01-15T00:00:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToHour() {
        assertEquals("HOUR", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12:00:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToMinute() {
        assertEquals("MINUTE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToSecond() {
        assertEquals("SECOND", TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:15.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToMillisecond() {
        assertEquals("MILLISECOND", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform("nonDateValue"));
        assertEquals("2022-11-03T12:30:00.976", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform("2022-11-03T12:30:00.976Z"));
    }

    @Test
    public void testTruncateTemporalToMonth() {
        assertEquals("MONTH", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("nonDateValue"));
        assertEquals("2019-01-00T00:00:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToYear() {
        assertEquals("YEAR", TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR.transform("nonDateValue"));
        assertEquals("2019-00-00T00:00:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testMinuteTruncation() {
        assertEquals("MINUTE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
        assertEquals("2022-11-03T12:30:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2022-11-03T12:30:00.976Z"));
        assertEquals("2019-01-15T12:03:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:03:15"));
        assertEquals("2022-11-03T12:03:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2022-11-03T12:03:00.976Z"));
    }

    @Test
    public void testTenthMinuteTruncation() {
        assertEquals("TENTH_OF_HOUR", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 12:30:15"));
        assertEquals("2019-01-15T12:00:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 12:03:15"));
        assertEquals("2019-01-15T03:10:00.000", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 3:10:15"));
    }

    /**
     * Verify that no duplicate names are used for {@link TemporalGranularity}.
     */
    @Test
    public void testNamesForUniqueness() {
        Set<String> names = new HashSet<>();
        for (TemporalGranularity transformer : TemporalGranularity.values()) {
            assertFalse("Duplicate name found: " + transformer.getName(), names.contains(transformer.getName()));
            names.add(transformer.getName());
        }
    }

    /**
     * Verify that {@link TemporalGranularity#of(String)} returns the correct granularity for each name.
     */
    @Test
    public void testStaticOf() {
        for (TemporalGranularity transformer : TemporalGranularity.values()) {
            TemporalGranularity actual = TemporalGranularity.of(transformer.getName());
            assertEquals(transformer, actual, "Incorrect transformer " + actual + " returned for name " + transformer.getName());
        }
    }

    /**
     * Verify that the shared formatter in each {@link TemporalGranularity} produces correct results when used concurrently by many threads.
     */
    @Test
    public void testConcurrentTransform() throws Exception {
        List<String> inputs = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            inputs.add(String.format("20%02d-%02d-%02d %02d:%02d:%02d", 10 + (i % 20), 1 + (i % 12), 1 + (i % 28), i % 24, i % 60, (i * 7) % 60));
        }

        // Compute the expected results on a single thread first.
        TemporalGranularity[] granularities = TemporalGranularity.values();
        String[][] expected = new String[granularities.length][inputs.size()];
        for (int g = 0; g < granularities.length; g++) {
            for (int i = 0; i < inputs.size(); i++) {
                expected[g][i] = granularities[g].transform(inputs.get(i));
            }
        }

        int threads = 16;
        int iterations = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<List<String>>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    List<String> mismatches = new ArrayList<>();
                    start.await();
                    for (int n = 0; n < iterations; n++) {
                        for (int g = 0; g < granularities.length; g++) {
                            for (int i = 0; i < inputs.size(); i++) {
                                String actual = granularities[g].transform(inputs.get(i));
                                if (!expected[g][i].equals(actual)) {
                                    mismatches.add(granularities[g] + " " + inputs.get(i) + ": expected " + expected[g][i] + " but was " + actual);
                                }
                            }
                        }
                    }
                    return mismatches;
                }));
            }
            start.countDown();

            List<String> mismatches = new ArrayList<>();
            for (Future<List<String>> future : futures) {
                mismatches.addAll(future.get(60, TimeUnit.SECONDS));
            }
            assertTrue(mismatches.isEmpty(),
                            mismatches.size() + " incorrect concurrent transforms, e.g. " + mismatches.stream().limit(5).collect(Collectors.toList()));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Verify that each {@link TemporalGranularity} serializes to their name.
     */
    @Test
    public void testSerialization() throws JsonProcessingException {
        for (TemporalGranularity granularity : TemporalGranularity.values()) {
            assertEquals("\"" + granularity.getName() + "\"", objectMapper.writeValueAsString(granularity), "Incorrect serialization of " + granularity);
        }
    }

    /**
     * Verify that each {@link TemporalGranularity} can be deserialized from their name.
     */
    @Test
    public void testDeserialization() throws JsonProcessingException {
        for (TemporalGranularity granularity : TemporalGranularity.values()) {
            assertEquals(granularity, objectMapper.readValue("\"" + granularity.getName() + "\"", TemporalGranularity.class),
                            "Incorrect deserialization of " + granularity);
        }
    }
}
