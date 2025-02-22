package datawave.query.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.util.AssertionErrors.assertFalse;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
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
        assertEquals("2019-01-15", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToHour() {
        assertEquals("HOUR", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToMinute() {
        assertEquals("MINUTE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTruncateTemporalToSecond() {
        assertEquals("SECOND", TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:15", TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("2019-01-15 12:30:15"));
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
        assertEquals("2019-01", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testMinuteTruncation() {
        assertEquals("MINUTE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }

    @Test
    public void testTenthMinuteTruncation() {
        assertEquals("TENTH_OF_HOUR", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.getName());
        assertNull(TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform(null));
        assertEquals("nonDateValue", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12:3", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 12:30:15"));
        assertEquals("2019-01-15T03:1", TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 3:10:15"));
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
     * Verify that each {@link TemporalGranularity} serializes to their name.
     */
    @Test
    public void testSerialization() throws JsonProcessingException {
        for (TemporalGranularity granularity : TemporalGranularity.values()) {
            Assertions.assertEquals("\"" + granularity.getName() + "\"", objectMapper.writeValueAsString(granularity),
                            "Incorrect serialization of " + granularity);
        }
    }

    /**
     * Verify that each {@link TemporalGranularity} can be deserialized from their name.
     */
    @Test
    public void testDeserialization() throws JsonProcessingException {
        for (TemporalGranularity granularity : TemporalGranularity.values()) {
            Assertions.assertEquals(granularity, objectMapper.readValue("\"" + granularity.getName() + "\"", TemporalGranularity.class),
                            "Incorrect deserialization of " + granularity);
        }
    }
}
