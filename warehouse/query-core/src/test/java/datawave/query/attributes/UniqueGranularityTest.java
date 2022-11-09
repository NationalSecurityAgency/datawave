package datawave.query.attributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class UniqueGranularityTest {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testAll() {
        assertEquals("ALL", UniqueGranularity.ALL.getName());
        assertNull(UniqueGranularity.ALL.transform(null));
        assertEquals("nonNullValue", UniqueGranularity.ALL.transform("nonNullValue"));
    }
    
    @Test
    public void testTruncateTemporalToDay() {
        assertEquals("DAY", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("nonDateValue"));
        assertEquals("2019-01-15", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToAMPM() {
        assertEquals("AM_PM", UniqueGranularity.TRUNCATE_TEMPORAL_TO_AMPM.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_AMPM.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_AMPM.transform("nonDateValue"));
        assertEquals("2019-01-15T03:30:15AM", UniqueGranularity.TRUNCATE_TEMPORAL_TO_AMPM.transform("2019-01-15 03:30:15"));
        assertEquals("2019-01-15T15:30:15PM", UniqueGranularity.TRUNCATE_TEMPORAL_TO_AMPM.transform("2019-01-15 15:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToHour() {
        assertEquals("HOUR", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToMinute() {
        assertEquals("MINUTE", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToSecond() {
        assertEquals("SECOND", UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:15", UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToMillisecond() {
        assertEquals("MILLISECOND", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform("nonDateValue"));
        assertEquals("2022-11-03T12:30:00.976", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform("2022-11-03T12:30:00.976Z"));
    }
    
    @Test
    public void testTruncateTemporalToMonth() {
        assertEquals("MONTH", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MONTH.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("nonDateValue"));
        assertEquals("2019-01", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToEra() {
        assertEquals("ERA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_ERA.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_ERA.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_ERA.transform("nonDateValue"));
        assertEquals("AD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_ERA.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToDOWIM() {
        assertEquals("DAY_OF_WEEK_IN_MONTH", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK_IN_MONTH.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK_IN_MONTH.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK_IN_MONTH.transform("nonDateValue"));
        assertEquals("3", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK_IN_MONTH.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToDOW() {
        assertEquals("DAY_OF_WEEK", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK.transform("nonDateValue"));
        assertEquals("Wed", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK.transform("2022-11-02 12:58:15"));
        assertEquals("Fri", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY_OF_WEEK.transform("2022-11-04 12:58:15"));
    }
    
    @Test
    public void testTruncateTemporalToHOD() {
        assertEquals("15", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY.transform("2022-11-02 15:58:15"));
        assertEquals("03", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY.transform("2022-11-04 03:18:15"));
        assertEquals("12", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY.transform("2022-11-03T12:30:00.976Z"));
        
    }
    
    @Test
    public void testTruncateTemporalToWOM() {
        assertEquals("WEEK_OF_MONTH", UniqueGranularity.TRUNCATE_TEMPORAL_TO_WEEK_OF_MONTH.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_WEEK_OF_MONTH.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_WEEK_OF_MONTH.transform("nonDateValue"));
        assertEquals("1", UniqueGranularity.TRUNCATE_TEMPORAL_TO_WEEK_OF_MONTH.transform("2022-11-02 12:58:15"));
    }
    
    @Test
    public void testMinuteTruncation() {
        assertEquals("MINUTE", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testNamesForUniqueness() {
        Set<String> names = new HashSet<>();
        for (UniqueGranularity transformer : UniqueGranularity.values()) {
            assertFalse("Duplicate name found: " + transformer.getName(), names.contains(transformer.getName()));
            names.add(transformer.getName());
        }
    }
    
    @Test
    public void testStaticOf() {
        for (UniqueGranularity transformer : UniqueGranularity.values()) {
            UniqueGranularity actual = UniqueGranularity.of(transformer.getName());
            assertEquals("Incorrect transformer " + actual + " returned for name " + transformer.getName(), transformer, actual);
        }
    }
    
    @Test
    public void testSerialization() throws JsonProcessingException {
        assertEquals("\"" + UniqueGranularity.ALL.getName() + "\"", objectMapper.writeValueAsString(UniqueGranularity.ALL));
        assertEquals("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName() + "\"",
                        objectMapper.writeValueAsString(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY));
        assertEquals("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName() + "\"",
                        objectMapper.writeValueAsString(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR));
        assertEquals("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName() + "\"",
                        objectMapper.writeValueAsString(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE));
        assertEquals("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName() + "\"",
                        objectMapper.writeValueAsString(UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND));
        assertEquals("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName() + "\"",
                        objectMapper.writeValueAsString(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND));
        assertEquals("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY.getName() + "\"",
                        objectMapper.writeValueAsString(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY));
    }
    
    @Test
    public void testDeserialization() throws JsonProcessingException {
        assertEquals(UniqueGranularity.ALL, objectMapper.readValue("\"" + UniqueGranularity.ALL.getName() + "\"", UniqueGranularity.class));
        assertEquals(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY,
                        objectMapper.readValue("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName() + "\"", UniqueGranularity.class));
        assertEquals(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR,
                        objectMapper.readValue("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName() + "\"", UniqueGranularity.class));
        assertEquals(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE,
                        objectMapper.readValue("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName() + "\"", UniqueGranularity.class));
        assertEquals(UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND,
                        objectMapper.readValue("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName() + "\"", UniqueGranularity.class));
        assertEquals(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND,
                        objectMapper.readValue("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName() + "\"", UniqueGranularity.class));
        assertEquals(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY,
                        objectMapper.readValue("\"" + UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR_OF_DAY.getName() + "\"", UniqueGranularity.class));
    }
}
