package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

public class RegexFieldVisitorTest {
    @Test
    public void noRegexNodeTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields("FIELD == 'value'");
        assertNotNull(fields);
        assertTrue(fields.isEmpty());
    }

    @Test
    public void singleRegexNodeTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields("FIELD =~ 'value'");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(1, fields.size());
        assertTrue(fields.contains("FIELD"));
    }

    @Test
    public void doubleRegexNodeTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields("FIELD1 =~ 'value' && FIELD2 =~ 'otherValue'");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(2, fields.size());
        assertTrue(fields.contains("FIELD1"));
        assertTrue(fields.contains("FIELD2"));
    }

    @Test
    public void wrappedRegexNodeTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields("(((FIELD =~ 'value')))");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(1, fields.size());
        assertTrue(fields.contains("FIELD"));
    }

    @Test
    public void multiLayeredRegexTreeTest() {
        Set<String> fields = RegexFieldVisitor
                        .parseRegexFields("FIELD1 =~ 'value' || ((FIELD2 =~ 'value' && FIELD3 =~ 'value') || (FIELD4 == 'value' && FIELD5 =~ 'value'))");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(4, fields.size());
        assertTrue(fields.contains("FIELD1"));
        assertTrue(fields.contains("FIELD2"));
        assertTrue(fields.contains("FIELD3"));
        assertTrue(fields.contains("FIELD5"));
    }

    @Test
    public void notRegexNodeTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields("FIELD !~ 'value'");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(1, fields.size());
        assertTrue(fields.contains("FIELD"));
    }

    @Test
    public void mixedRegexNodeTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields("FIELD !~ 'value' && OTHER_FIELD =~ 'value'");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(2, fields.size());
        assertTrue(fields.contains("FIELD"));
        assertTrue(fields.contains("OTHER_FIELD"));
    }

    @Test
    public void duplicateRegexFieldTest() {
        Set<String> fields = RegexFieldVisitor.parseRegexFields(
                        "FIELD2 == 'value' && FIELD3 == 'value2' || (FIELD2 == 'value' && FIELD3 == 'value' && (FIELD =~ 'value' || FIELD2 == 'value2') && (FIELD !~ 'value' || FIELD3 == 'value'))");
        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        assertEquals(1, fields.size());
        assertTrue(fields.contains("FIELD"));
    }
}
