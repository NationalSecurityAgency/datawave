package datawave.query.attributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class GroupByGranularityTest {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testAll() {
        assertEquals("ALL", GroupbyGranularity.ALL.getName());
        assertNull(GroupbyGranularity.ALL.transform(null));
        assertEquals("nonNullValue", GroupbyGranularity.ALL.transform("nonNullValue"));
    }
    
    @Test
    public void testTruncateTemporalToDay() {
        assertEquals("DAY", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("nonDateValue"));
        assertEquals("2019-01-15", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToHour() {
        assertEquals("HOUR", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToMinute() {
        assertEquals("MINUTE", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToSecond() {
        assertEquals("SECOND", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30:15", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToMillisecond() {
        assertEquals("MILLISECOND", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform("nonDateValue"));
        assertEquals("2022-11-03T12:30:00.976", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.transform("2022-11-03T12:30:00.976Z"));
    }
    
    @Test
    public void testTruncateTemporalToMonth() {
        assertEquals("MONTH", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MONTH.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("nonDateValue"));
        assertEquals("2019-01", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MONTH.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testMinuteTruncation() {
        assertEquals("MINUTE", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTenthMinuteTruncation() {
        assertEquals("TENTH_OF_HOUR", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.getName());
        assertNull(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform(null));
        assertEquals("nonDateValue", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12:3", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 12:30:15"));
        assertEquals("2019-01-15T03:1", GroupbyGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR.transform("2019-01-15 3:10:15"));
    }
    
    @Test
    public void testNamesForUniqueness() {
        Set<String> names = new HashSet<>();
        for (GroupbyGranularity transformer : GroupbyGranularity.values()) {
            assertFalse("Duplicate name found: " + transformer.getName(), names.contains(transformer.getName()));
            names.add(transformer.getName());
        }
    }
    
    @Test
    public void testStaticOf() {
        for (GroupbyGranularity transformer : GroupbyGranularity.values()) {
            GroupbyGranularity actual = GroupbyGranularity.of(transformer.getName());
            assertEquals("Incorrect transformer " + actual + " returned for name " + transformer.getName(), transformer, actual);
        }
    }
    
    @Test
    public void testSerialization() throws JsonProcessingException {
        assertEquals("\"" + GroupbyGranularity.ALL.getName() + "\"", objectMapper.writeValueAsString(GroupbyGranularity.ALL));
        assertEquals("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName() + "\"",
                        objectMapper.writeValueAsString(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY));
        assertEquals("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName() + "\"",
                        objectMapper.writeValueAsString(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR));
        assertEquals("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName() + "\"",
                        objectMapper.writeValueAsString(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE));
        assertEquals("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName() + "\"",
                        objectMapper.writeValueAsString(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND));
        assertEquals("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName() + "\"",
                        objectMapper.writeValueAsString(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND));
    }
    
    @Test
    public void testDeserialization() throws JsonProcessingException {
        assertEquals(GroupbyGranularity.ALL, objectMapper.readValue("\"" + GroupbyGranularity.ALL.getName() + "\"", GroupbyGranularity.class));
        assertEquals(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY,
                        objectMapper.readValue("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY.getName() + "\"", GroupbyGranularity.class));
        assertEquals(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR,
                        objectMapper.readValue("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR.getName() + "\"", GroupbyGranularity.class));
        assertEquals(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE,
                        objectMapper.readValue("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE.getName() + "\"", GroupbyGranularity.class));
        assertEquals(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND,
                        objectMapper.readValue("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND.getName() + "\"", GroupbyGranularity.class));
        assertEquals(GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND,
                        objectMapper.readValue("\"" + GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND.getName() + "\"", GroupbyGranularity.class));
    }
}
