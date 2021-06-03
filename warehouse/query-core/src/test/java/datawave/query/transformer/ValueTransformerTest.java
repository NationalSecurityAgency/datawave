package datawave.query.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ValueTransformerTest {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testOriginal() {
        assertEquals("ORIGINAL", ValueTransformer.ORIGINAL.getName());
        assertNull(ValueTransformer.ORIGINAL.transform(null));
        assertEquals("nonNullValue", ValueTransformer.ORIGINAL.transform("nonNullValue"));
    }
    
    @Test
    public void testTruncateTemporalToDay() {
        assertEquals("DAY", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY.getName());
        assertNull(ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY.transform(null));
        assertEquals("nonDateValue", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY.transform("nonDateValue"));
        assertEquals("2019-01-15", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testTruncateTemporalToHour() {
        assertEquals("HOUR", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR.getName());
        assertNull(ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR.transform(null));
        assertEquals("nonDateValue", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR.transform("nonDateValue"));
        assertEquals("2019-01-15T12", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testMinuteTruncation() {
        assertEquals("MINUTE", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE.getName());
        assertNull(ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE.transform(null));
        assertEquals("nonDateValue", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE.transform("nonDateValue"));
        assertEquals("2019-01-15T12:30", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE.transform("2019-01-15 12:30:15"));
    }
    
    @Test
    public void testNamesForUniqueness() {
        Set<String> names = new HashSet<>();
        for (ValueTransformer transformer : ValueTransformer.values()) {
            assertFalse("Duplicate name found: " + transformer.getName(), names.contains(transformer.getName()));
            names.add(transformer.getName());
        }
    }
    
    @Test
    public void testStaticOf() {
        for (ValueTransformer transformer : ValueTransformer.values()) {
            ValueTransformer actual = ValueTransformer.of(transformer.getName());
            assertEquals("Incorrect transformer " + actual + " returned for name " + transformer.getName(), transformer, actual);
        }
    }
    
    @Test
    public void testSerialization() throws JsonProcessingException {
        assertEquals("\"" + ValueTransformer.ORIGINAL.getName() + "\"", objectMapper.writeValueAsString(ValueTransformer.ORIGINAL));
        assertEquals("\"" + ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY.getName() + "\"",
                        objectMapper.writeValueAsString(ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY));
        assertEquals("\"" + ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR.getName() + "\"",
                        objectMapper.writeValueAsString(ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR));
        assertEquals("\"" + ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE.getName() + "\"",
                        objectMapper.writeValueAsString(ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE));
    }
    
    @Test
    public void testDeserialization() throws JsonProcessingException {
        assertEquals(ValueTransformer.ORIGINAL, objectMapper.readValue("\"" + ValueTransformer.ORIGINAL.getName() + "\"", ValueTransformer.class));
        assertEquals(ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY,
                        objectMapper.readValue("\"" + ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY.getName() + "\"", ValueTransformer.class));
        assertEquals(ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR,
                        objectMapper.readValue("\"" + ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR.getName() + "\"", ValueTransformer.class));
        assertEquals(ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE,
                        objectMapper.readValue("\"" + ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE.getName() + "\"", ValueTransformer.class));
    }
}
