package datawave.query.attributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class UniqueFieldsTest {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @BeforeClass
    public static void setUp() throws Exception {
        objectMapper.registerModule(new GuavaModule());
    }
    
    /**
     * Verify that {@link UniqueFields#isEmpty()} returns true for a {@link UniqueFields} with no fields.
     */
    @Test
    public void testIsEmptyForEmptyUniqueFields() {
        assertTrue(new UniqueFields().isEmpty());
    }
    
    /**
     * Verify that {@link UniqueFields#isEmpty()} returns false for a {@link UniqueFields} with at least one field.
     */
    @Test
    public void testIsEmptyForNonEmptyUniqueFields() {
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("fieldA", UniqueGranularity.ALL);
        
        assertFalse(uniqueFields.isEmpty());
    }
    
    /**
     * Verify formatting an empty {@link UniqueFields} returns an empty string.
     */
    @Test
    public void testEmptyUniqueFieldsToString() {
        UniqueFields uniqueFields = new UniqueFields();
        assertEquals("", uniqueFields.toString());
    }
    
    /**
     * Test formatting a non-empty {@link UniqueFields} to a string.
     */
    @Test
    public void testNonEmptyUniqueFieldsToString() {
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("fieldA", UniqueGranularity.ALL);
        uniqueFields.put("fieldB", UniqueGranularity.ALL);
        uniqueFields.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        uniqueFields.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        assertEquals("fieldA[ALL],fieldB[ALL,DAY],fieldC[HOUR],fieldD[HOUR,MINUTE]", uniqueFields.toString());
    }
    
    /**
     * Verify that {@link UniqueFields#from(String)} returns null when given a null input.
     */
    @Test
    public void testParsingFromNullString() {
        assertNull(UniqueFields.from(null));
    }
    
    /**
     * Verify that {@link UniqueFields#from(String)} returns an non-null, empty {@link UniqueFields} from a blank string.
     */
    @Test
    public void testParsingFromEmptyString() {
        assertTrue(UniqueFields.from(" ").isEmpty());
    }
    
    /**
     * Verify that a single field with no specified value granularity is added with an ALL granularity.
     */
    @Test
    public void testParsingSingleFieldWithoutValueGranularity() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        
        UniqueFields actual = UniqueFields.from("fieldA");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testParsingMultipleFieldsWithoutValueGranularities() {
        UniqueFields expected = new UniqueFields();
        expected.put("DEATH_DATE", UniqueGranularity.ALL);
        expected.put("$MAGIC", UniqueGranularity.ALL);
        expected.put("$BIRTH_DATE", UniqueGranularity.ALL);
        
        UniqueFields actual = UniqueFields.from("DEATH_DATE,$MAGIC,$BIRTH_DATE");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a single field with a value granularity is parsed correctly.
     */
    @Test
    public void testParsingSingleFieldWithValueGranularity() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        
        UniqueFields actual = UniqueFields.from("fieldA[HOUR]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with no granularity located at the start of a string with multiple fields is added with an ALL granularity.
     */
    @Test
    public void testParsingFieldWithNoGranularityAtStartOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA,fieldB[MINUTE]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with no granularity located at the end of a string with multiple fields is added with an ALL granularity.
     */
    @Test
    public void testParsingFieldWithNoGranularityAtEndOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldB[MINUTE],fieldA");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with no granularity located between two different fields in a string with multiple fields is added with an ALL granularity.
     */
    @Test
    public void testParsingFieldWithNoGranularityInMiddleOfEndOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        expected.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        
        UniqueFields actual = UniqueFields.from("fieldB[MINUTE],fieldA,fieldC[HOUR]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a single field with an empty granularity list is added with the ALL granularity.
     */
    @Test
    public void testParsingSingleFieldWithEmptyGranularityList() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        
        UniqueFields actual = UniqueFields.from("fieldA[]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with an empty granularity list located at the start of a string with multiple fields is added with the ALL granularity.
     */
    @Test
    public void testParsingFieldWithEmptyGranularityListAtStartOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA[],fieldB[MINUTE]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with an empty granularity list located at the end of a string with multiple fields is added with the ALL granularity.
     */
    @Test
    public void testParsingFieldWithEmptyGranularityListAtEndOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldB[MINUTE],fieldA[]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with an empty granularity list located between two different fields is added with the ALL granularity.
     */
    @Test
    public void testParsingFieldWithEmptyGranularityListInMiddleOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        expected.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        
        UniqueFields actual = UniqueFields.from("fieldB[MINUTE],fieldA[],fieldC[HOUR]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a string with a variety of fields and granularities is parsed correctly.
     */
    @Test
    public void testParsingMixedFieldsAndGranularities() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA[ALL],fieldB[ALL,DAY],fieldC[HOUR],fieldD[HOUR,MINUTE]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a string with fields that have {@link datawave.query.jexl.JexlASTHelper#IDENTIFIER_PREFIX} at the start are parsed correctly.
     */
    @Test
    public void testParsingNonDeconstructedIdentifiers() {
        UniqueFields expected = new UniqueFields();
        expected.put("$fieldA", UniqueGranularity.ALL);
        expected.put("$fieldB", UniqueGranularity.ALL);
        expected.put("$fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("$fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("$fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("$fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("$fieldA[ALL],$fieldB[ALL,DAY],$fieldC[HOUR],$fieldD[HOUR,MINUTE]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that whitespace does not cause parsing to fail.
     */
    @Test
    public void testParsingWithWhitespace() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA[ALL], fieldB[ALL, DAY], fieldC[HOUR],fieldD[HOUR, MINUTE]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that whitespace does not cause parsing to fail.
     */
    @Test
    public void testParsingGranularitiesIsCaseInsensitive() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA[all], fieldB[ALL, day], fieldC[Hour],fieldD[HOUR, minute]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that consecutive commas are ignored.
     */
    @Test
    public void testParsingConsecutiveCommas() {
        // Test consecutive commas at the start.
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        
        UniqueFields actual = UniqueFields.from(",,fieldA,fieldB[DAY]");
        
        assertEquals(expected, actual);
        
        // Test consecutive commas in the middle.
        expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        
        actual = UniqueFields.from("fieldA,,fieldB[DAY]");
        
        assertEquals(expected, actual);
        
        // Test consecutive commas at the end.
        expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        
        actual = UniqueFields.from("fieldA,fieldB[DAY],,");
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void testParsingInvalidGranularity() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> UniqueFields.from("fieldA[BAD]"));
        assertEquals("Invalid unique granularity given: BAD", exception.getMessage());
    }
    
    /**
     * Verify that when a {@link UniqueFields} is serialized, it is serialized as the result of {@link UniqueFields#toString()}.
     */
    @Test
    public void testSerialization() throws JsonProcessingException {
        SortedSetMultimap<String,UniqueGranularity> sortedFields = TreeMultimap.create();
        sortedFields.put("fieldA", UniqueGranularity.ALL);
        sortedFields.put("fieldB", UniqueGranularity.ALL);
        sortedFields.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        sortedFields.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        sortedFields.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        sortedFields.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields uniqueFields = new UniqueFields(sortedFields);
        
        String json = objectMapper.writeValueAsString(uniqueFields);
        assertEquals("\"fieldA[ALL],fieldB[ALL,DAY],fieldC[HOUR],fieldD[HOUR,MINUTE]\"", json);
    }
    
    /**
     * Verify that a formatted string can be deserialized into a {@link UniqueFields}.
     */
    @Test
    public void testDeserialization() throws JsonProcessingException {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.ALL);
        expected.put("fieldB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        String json = "\"fieldA[ALL],fieldB[ALL,DAY],fieldC[HOUR],fieldD[HOUR,MINUTE]\"";
        UniqueFields actual = objectMapper.readValue(json, UniqueFields.class);
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that passing in a set of values to be transformed for a particular field returns a set that contains the transformations for each granularity
     * found for the field.
     */
    @Test
    public void testValueTransformation() {
        SortedSet<String> expected = Sets.newTreeSet();
        expected.add("2020-01-12");
        expected.add("2020-01-12T15");
        expected.add("2020-01-12T15:30");
        expected.add("2020-01-12 15:30:45");
        expected.add("nonDateValue");
        
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("fieldA", UniqueGranularity.ALL);
        uniqueFields.put("fieldA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        uniqueFields.put("fieldA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("fieldA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
        Set<String> values = Sets.newHashSet("2020-01-12 15:30:45", "nonDateValue");
        
        SortedSet<String> actual = Sets.newTreeSet(uniqueFields.transformValues("fieldA", values));
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that when applying a function to the fields of a {@link UniqueFields}, that the fields and underlying map are replaced.
     */
    @Test
    public void testDeconstructIdentifierFields() {
        SortedSet<String> expected = Sets.newTreeSet();
        expected.add("FIELDA");
        expected.add("FIELDB");
        expected.add("FIELDC");
        
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("$FIELDA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("$FIELDB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("FIELDC", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        
        uniqueFields.deconstructIdentifierFields();
        
        SortedSet<String> actual = Sets.newTreeSet(uniqueFields.getFields());
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that when applying a function to the fields of a {@link UniqueFields}, that the fields and underlying map are replaced.
     */
    @Test
    public void testRemapFields() {
        UniqueFields actual = new UniqueFields();
        actual.put("fieldA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        
        Multimap<String,String> model = HashMultimap.create();
        model.put("FIELDA", "FIELDB");
        model.put("FIELDA", "fieldc");
        
        actual.remapFields(model);
        
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("FIELDB", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldc", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        
        assertEquals(expected, actual);
    }
}
