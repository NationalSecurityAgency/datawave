package datawave.query.attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.SortedSet;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class ExcerptFieldsTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Verify that {@link ExcerptFields#isEmpty()} returns true for a {@link ExcerptFields} with no fields.
     */
    @Test
    public void testIsEmpty() {
        assertTrue(new ExcerptFields().isEmpty());
    }

    /**
     * Verify that {@link ExcerptFields#isEmpty()} returns false for a {@link ExcerptFields} with fields.
     */
    @Test
    public void testIsNotEmpty() {
        ExcerptFields excerptFields = new ExcerptFields();
        excerptFields.put("CONTENT", 10);
        assertFalse(excerptFields.isEmpty());
    }

    /**
     * Verify formatting an empty {@link ExcerptFields} returns an empty string.
     */
    @Test
    public void testEmptyExcerptFieldsToString() {
        assertEquals("", new ExcerptFields().toString());
    }

    /**
     * Verify formatting a non-empty {@link ExcerptFields} to a string.
     */
    @Test
    public void testNonEmptyExcerptFieldsToString() {
        ExcerptFields excerptFields = new ExcerptFields();
        excerptFields.put("BODY", 10);
        excerptFields.put("CONTENT", 5);
        assertEquals("BODY/10,CONTENT/5", excerptFields.toString());
    }

    /**
     * Verify that {@link ExcerptFields#from(String)} returns null when given a null input.
     */
    @Test
    public void testParsingFromNullString() {
        assertNull(ExcerptFields.from(null));
    }

    /**
     * Verify that {@link ExcerptFields#from(String)} returns a non-null, empty {@link ExcerptFields} from a blank string.
     */
    @Test
    public void testParsingFromEmptyString() {
        assertTrue(ExcerptFields.from("  ").isEmpty());
    }

    /**
     * Verify that {@link ExcerptFields#from(String)} correctly parses a non-blank string.
     */
    @Test
    public void testParsingFromNonBlankString() {
        ExcerptFields expected = new ExcerptFields();
        expected.put("BODY", 10);
        expected.put("CONTENT", 5);

        ExcerptFields actual = ExcerptFields.from("BODY/10,CONTENT/5");
        assertEquals(expected, actual);
    }

    /**
     * Verify that when a {@link ExcerptFields} is serialized, it is serialized as the result of {@link ExcerptFields#toString()}
     */
    @Test
    public void testSerialization() throws JsonProcessingException {
        ExcerptFields excerptFields = new ExcerptFields();
        excerptFields.put("BODY", 10);
        excerptFields.put("CONTENT", 5);

        String json = objectMapper.writeValueAsString(excerptFields);
        assertEquals("\"BODY/10,CONTENT/5\"", json);
    }

    /**
     * Verify that a formatted string can be deserialized into a {@link ExcerptFields}.
     */
    @Test
    public void testDeserialization() throws JsonProcessingException {
        ExcerptFields expected = new ExcerptFields();
        expected.put("BODY", 10);
        expected.put("CONTENT", 5);

        String json = "\"BODY/10,CONTENT/5\"";
        ExcerptFields actual = objectMapper.readValue(json, ExcerptFields.class);

        assertEquals(expected, actual);
    }

    /**
     * Verify that when deconstructing the fields of an {@link ExcerptFields}, that they are replaced correctly.
     */
    @Test
    public void testDeconstructFields() {
        ExcerptFields actual = new ExcerptFields();
        actual.put("$FIELDA", 10);
        actual.put("$FIELDB", 5);
        actual.put("FIELDC", 1);

        ExcerptFields expected = new ExcerptFields();
        expected.put("FIELDA", 10);
        expected.put("FIELDB", 5);
        expected.put("FIELDC", 1);

        actual.deconstructFields();

        assertEquals(expected, actual);
    }

    /**
     * Verify that when expanding the fields of a {@link ExcerptFields}, that they are replaced correctly.
     */
    @Test
    public void testExpandFields() {
        ExcerptFields actual = new ExcerptFields();
        actual.put("fielda", 10);
        actual.put("fieldc", 1);

        ExcerptFields expected = new ExcerptFields();
        expected.put("FIELDA", 10);
        expected.put("FIELDAA", 10);
        expected.put("FIELDBB", 10);
        expected.put("FIELDC", 1);

        Multimap<String,String> model = HashMultimap.create();
        model.put("FIELDA", "FIELDAA");
        model.put("FIELDA", "FIELDBB");

        actual.expandFields(model);

        assertEquals(expected, actual);
    }
}
