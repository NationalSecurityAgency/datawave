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
        uniqueFields.put("fieldA", ValueTransformer.ORIGINAL);
        
        assertFalse(uniqueFields.isEmpty());
    }
    
    /**
     * Verify formatting an empty {@link UniqueFields} returns an empty string.
     */
    @Test
    public void testFormattingEmptyUniqueFieldsToString() {
        UniqueFields uniqueFields = new UniqueFields();
        assertEquals("", uniqueFields.toFormattedString());
    }
    
    /**
     * Test formatting a non-empty {@link UniqueFields} to a string.
     */
    @Test
    public void testFormattingNonEmptyUniqueFieldsToString() {
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("fieldA", ValueTransformer.ORIGINAL);
        uniqueFields.put("fieldB", ValueTransformer.ORIGINAL);
        uniqueFields.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        uniqueFields.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        assertEquals("fieldA:[ORIGINAL],fieldB:[ORIGINAL,DAY],fieldC:[HOUR],fieldD:[HOUR,MINUTE]", uniqueFields.toFormattedString());
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
     * Verify that a single field with no specified value transformer is added with an ORIGINAL transformer.
     */
    @Test
    public void testParsingSingleFieldWithoutValueTransformer() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        
        UniqueFields actual = UniqueFields.from("fieldA");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testParsingMultipleFieldsWithoutValueTransformers() {
        UniqueFields expected = new UniqueFields();
        expected.put("DEATH_DATE", ValueTransformer.ORIGINAL);
        expected.put("$MAGIC", ValueTransformer.ORIGINAL);
        expected.put("$BIRTH_DATE", ValueTransformer.ORIGINAL);
        
        UniqueFields actual = UniqueFields.from("DEATH_DATE,$MAGIC,$BIRTH_DATE");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a single field with a value transformer is parsed correctly.
     */
    @Test
    public void testParsingSingleFieldWithValueTransformer() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        
        UniqueFields actual = UniqueFields.from("fieldA:[HOUR]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with no transformer located at the start of a string with multiple fields is added with an ORIGINAL transformer.
     */
    @Test
    public void testParsingFieldWithNoTransformerAtStartOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA,fieldB:[MINUTE]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with no transformer located at the end of a string with multiple fields is added with an ORIGINAL transformer.
     */
    @Test
    public void testParsingFieldWithNoTransformerAtEndOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldB:[MINUTE],fieldA");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with no transformer located between two different fields in a string with multiple fields is added with an ORIGINAL transformer.
     */
    @Test
    public void testParsingFieldWithNoTransformerInMiddleOfEndOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        expected.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        
        UniqueFields actual = UniqueFields.from("fieldB:[MINUTE],fieldA,fieldC:[HOUR]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a single field with an empty transformer list is added with the ORIGINAL transformer.
     */
    @Test
    public void testParsingSingleFieldWithEmptyTransformerList() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        
        UniqueFields actual = UniqueFields.from("fieldA:[]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with an empty transformer list located at the start of a string with multiple fields is added with the ORIGINAL transformer.
     */
    @Test
    public void testParsingFieldWithEmptyTransformerListAtStartOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA:[],fieldB:[MINUTE]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with an empty transformer list located at the end of a string with multiple fields is added with the ORIGINAL transformer.
     */
    @Test
    public void testParsingFieldWithEmptyTransformerListAtEndOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldB:[MINUTE],fieldA:[]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a field with an empty transformer list located between two different fields is added with the ORIGINAL transformer.
     */
    @Test
    public void testParsingFieldWithEmptyTransformerListInMiddleOfMixedFields() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        expected.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        
        UniqueFields actual = UniqueFields.from("fieldB:[MINUTE],fieldA:[],fieldC:[HOUR]");
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a string with a variety of fields and transformers is parsed correctly.
     */
    @Test
    public void testParsingMixedFieldsAndTransformers() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA:[ORIGINAL],fieldB:[ORIGINAL,DAY],fieldC:[HOUR],fieldD:[HOUR,MINUTE]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that a string with fields that have {@link datawave.query.jexl.JexlASTHelper#IDENTIFIER_PREFIX} at the start are parsed correctly.
     */
    @Test
    public void testParsingNonDeconstructedIdentifiers() {
        UniqueFields expected = new UniqueFields();
        expected.put("$fieldA", ValueTransformer.ORIGINAL);
        expected.put("$fieldB", ValueTransformer.ORIGINAL);
        expected.put("$fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("$fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("$fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("$fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("$fieldA:[ORIGINAL],$fieldB:[ORIGINAL,DAY],$fieldC:[HOUR],$fieldD:[HOUR,MINUTE]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that whitespace does not cause parsing to fail.
     */
    @Test
    public void testParsingWithWhitespace() {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields actual = UniqueFields.from("fieldA:[ORIGINAL], fieldB:[ORIGINAL, DAY], fieldC:[HOUR],fieldD:[HOUR, MINUTE]");
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that when a {@link UniqueFields} is serialized, it is serialized as a formatted string.
     */
    @Test
    public void testSerialization() throws JsonProcessingException {
        SortedSetMultimap<String,ValueTransformer> sortedFields = TreeMultimap.create();
        sortedFields.put("fieldA", ValueTransformer.ORIGINAL);
        sortedFields.put("fieldB", ValueTransformer.ORIGINAL);
        sortedFields.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        sortedFields.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        sortedFields.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        sortedFields.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        UniqueFields uniqueFields = new UniqueFields(sortedFields);
        
        String json = objectMapper.writeValueAsString(uniqueFields);
        assertEquals("\"fieldA:[ORIGINAL],fieldB:[ORIGINAL,DAY],fieldC:[HOUR],fieldD:[HOUR,MINUTE]\"", json);
    }
    
    /**
     * Verify that a formatted string can be deserialized into a {@link UniqueFields}.
     */
    @Test
    public void testDeserialization() throws JsonProcessingException {
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.ORIGINAL);
        expected.put("fieldB", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("fieldC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldD", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
        
        String json = "\"fieldA:[ORIGINAL],fieldB:[ORIGINAL,DAY],fieldC:[HOUR],fieldD:[HOUR,MINUTE]\"";
        UniqueFields actual = objectMapper.readValue(json, UniqueFields.class);
        
        assertEquals(expected, actual);
    }
    
    /**
     * Verify that passing in a set of values to be transformed for a particular field returns a set that contains the transformations for each transformer
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
        uniqueFields.put("fieldA", ValueTransformer.ORIGINAL);
        uniqueFields.put("fieldA", ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY);
        uniqueFields.put("fieldA", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("fieldA", ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE);
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
        uniqueFields.put("$FIELDA", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("$FIELDB", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        uniqueFields.put("FIELDC", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        
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
        actual.put("fieldA", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        
        Multimap<String,String> model = HashMultimap.create();
        model.put("FIELDA", "FIELDB");
        model.put("FIELDA", "fieldc");
        
        actual.remapFields(model);
        
        UniqueFields expected = new UniqueFields();
        expected.put("fieldA", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("FIELDB", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("fieldc", ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR);
        
        assertEquals(expected, actual);
    }
}
