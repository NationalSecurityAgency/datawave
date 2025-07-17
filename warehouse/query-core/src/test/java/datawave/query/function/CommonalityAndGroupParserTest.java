package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CommonalityAndGroupParserTest {
    private CommonalityAndGroupParser fieldParser;

    @BeforeEach
    public void setUp() {
        fieldParser = new CommonalityAndGroupParser();
    }

    @Test
    public void testKeyAndGroupingParses() {
        CommonalityAndGroup token = fieldParser.parse("FOO_3.FOO.4.5");
        assertNotNull(token);
        assertEquals("FOO", token.getKeyCommonality());
        assertEquals("5", token.getGroup());

        token = fieldParser.parse("FOO_3");
        assertNull(token);

        token = fieldParser.parse("FOO_3_BAR.FOO.4");
        assertNotNull(token);
        assertEquals("FOO", token.getKeyCommonality());
        assertEquals("4", token.getGroup());

        token = fieldParser.parse("FOO_3_BAR.");
        assertNull(token);

        token = fieldParser.parse("FOO_3_BAR.FOO");
        assertNull(token);

        token = fieldParser.parse("FOO_3_BAR.FOO.");
        assertNull(token);

        token = fieldParser.parse("FOO_3_BAR..");
        assertNull(token);
    }

    @Test
    public void testTokenHashCodeAndEquals() {
        // Equal key/grouping (different field components)
        CommonalityAndGroup t1 = fieldParser.parse("FOO_3.FOO.4.3");
        CommonalityAndGroup t2 = fieldParser.parse("FOO_4.FOO.5.3");
        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());
        assertTrue(t1.hashCode() > 1);
        assertTrue(t2.hashCode() > 1);

        // Equal key/grouping (different field component
        t1 = fieldParser.parse("F_1.FOO.4.3");
        t2 = fieldParser.parse("FOO_4.FOO.5.3");
        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());
        assertTrue(t1.hashCode() > 1);
        assertTrue(t2.hashCode() > 1);

        // Mismatch key
        t1 = fieldParser.parse("FOO_3.FOO.4.3");
        t2 = fieldParser.parse("FOO_4.AB.5.3");
        assertNotEquals(t1, t2);
        assertNotEquals(t1.hashCode(), t2.hashCode());
        assertTrue(t1.hashCode() > 1);
        assertTrue(t2.hashCode() > 1);

        // Mismatch grouping
        t1 = fieldParser.parse("FOO_3.FOO.4.30");
        t2 = fieldParser.parse("FOO_3.FOO.5.40");
        assertNotEquals(t1, t2);
        assertNotEquals(t1.hashCode(), t2.hashCode());
        assertTrue(t1.hashCode() > 1);
        assertTrue(t2.hashCode() > 1);

        // Mismatch grouping
        t1 = fieldParser.parse("FOO_3.FOO.4.30");
        t2 = fieldParser.parse("FOO_3.FOO.5.31");
        assertNotEquals(t1, t2);
        assertNotEquals(t1.hashCode(), t2.hashCode());
        assertTrue(t1.hashCode() > 1);
        assertTrue(t2.hashCode() > 1);
    }
}
