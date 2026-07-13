package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import datawave.query.function.FieldName.GroupAndInstance;

public class FieldNameTest {

    @Test
    public void testGroupedFieldParses() {
        FieldName fieldName = FieldName.of("FOO_3.FOO.4.5");
        assertTrue(fieldName.isGrouped());
        assertEquals("FOO_3.FOO.4.5", fieldName.getName());
        assertEquals("FOO", fieldName.getGroupAndInstance().getGroup());
        assertEquals("5", fieldName.getGroupAndInstance().getInstance());

        fieldName = FieldName.of("FOO_3_BAR.FOO.4");
        assertTrue(fieldName.isGrouped());
        assertEquals("FOO", fieldName.getGroupAndInstance().getGroup());
        assertEquals("4", fieldName.getGroupAndInstance().getInstance());
    }

    /**
     * Pins the group/instance mapping against a realistic grouping-context field name (the "commonality token" shape used by the limit.fields integration
     * tests): the group is the second token, the instance is the trailing token.
     */
    @Test
    public void testGroupIsSecondTokenInstanceIsLast() {
        FieldName fieldName = FieldName.of("CAT.PET.0");
        assertTrue(fieldName.isGrouped());
        assertEquals("CAT.PET.0", fieldName.getName());
        assertEquals("PET", fieldName.getGroupAndInstance().getGroup());
        assertEquals("0", fieldName.getGroupAndInstance().getInstance());
    }

    /**
     * Trailing empty tokens are ignored, so a well-formed field with trailing periods still parses as grouped. The case an earlier index-based parser got
     * wrong.
     */
    @Test
    public void testTrailingPeriodsAreIgnored() {
        FieldName fieldName = FieldName.of("A.B.C.");
        assertTrue(fieldName.isGrouped());
        assertEquals("B", fieldName.getGroupAndInstance().getGroup());
        assertEquals("C", fieldName.getGroupAndInstance().getInstance());

        fieldName = FieldName.of("A.B.C..");
        assertTrue(fieldName.isGrouped());
        assertEquals("B", fieldName.getGroupAndInstance().getGroup());
        assertEquals("C", fieldName.getGroupAndInstance().getInstance());
    }

    @Test
    public void testUngroupedFieldsHaveNoGroupAndInstance() {
        for (String field : new String[] {"FOO_3", "FOO_3_BAR.", "FOO_3_BAR.FOO", "FOO_3_BAR.FOO.", "FOO_3_BAR.."}) {
            FieldName fieldName = FieldName.of(field);
            assertFalse(fieldName.isGrouped(), field);
            assertEquals(field, fieldName.getName());
            assertNull(fieldName.getGroupAndInstance(), field);
        }
    }

    /**
     * The whole point of the split: a {@link FieldName} is equal only to another with the same name, while its {@link GroupAndInstance} identity matches across
     * sibling fields with different names but the same group and instance (e.g. NAME.PERSON.1 and AGE.PERSON.1).
     */
    @Test
    public void testFieldNameEqualityVersusGroupAndInstanceEquality() {
        FieldName name = FieldName.of("NAME.PERSON.1");
        FieldName age = FieldName.of("AGE.PERSON.1");

        // FieldName equality is over the full name - different names are not equal
        assertNotEquals(name, age);
        assertEquals(name, FieldName.of("NAME.PERSON.1"));
        assertEquals(name.hashCode(), FieldName.of("NAME.PERSON.1").hashCode());

        // GroupAndInstance identity is shared across the two sibling fields
        assertEquals(name.getGroupAndInstance(), age.getGroupAndInstance());
        assertEquals(name.getGroupAndInstance().hashCode(), age.getGroupAndInstance().hashCode());
    }

    @Test
    public void testGroupAndInstanceHashCodeAndEquals() {
        // Equal group/instance (different field components)
        GroupAndInstance t1 = FieldName.of("FOO_3.FOO.4.3").getGroupAndInstance();
        GroupAndInstance t2 = FieldName.of("FOO_4.FOO.5.3").getGroupAndInstance();
        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());

        // Equal group/instance (different field component)
        t1 = FieldName.of("F_1.FOO.4.3").getGroupAndInstance();
        t2 = FieldName.of("FOO_4.FOO.5.3").getGroupAndInstance();
        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());

        // Mismatch group
        t1 = FieldName.of("FOO_3.FOO.4.3").getGroupAndInstance();
        t2 = FieldName.of("FOO_4.AB.5.3").getGroupAndInstance();
        assertNotEquals(t1, t2);

        // Mismatch instance
        t1 = FieldName.of("FOO_3.FOO.4.30").getGroupAndInstance();
        t2 = FieldName.of("FOO_3.FOO.5.40").getGroupAndInstance();
        assertNotEquals(t1, t2);

        // Mismatch instance
        t1 = FieldName.of("FOO_3.FOO.4.30").getGroupAndInstance();
        t2 = FieldName.of("FOO_3.FOO.5.31").getGroupAndInstance();
        assertNotEquals(t1, t2);
    }

    /**
     * The static {@link FieldName#baseName(String)} strips the grouping context (everything from the first period on), or returns the field itself when there
     * is no period. The all-dot inputs pin the indexOf-not-split requirement: a split-based implementation would throw an ArrayIndexOutOfBoundsException on
     * these because {@code ".".split("\\.")} returns a length-0 array (trailing empty tokens are stripped).
     */
    @Test
    public void testStaticBaseName() {
        assertEquals("FOO", FieldName.baseName("FOO.BAR.1"));
        assertEquals("FOO_3_BAR", FieldName.baseName("FOO_3_BAR.FOO")); // dotted but ungrouped
        assertEquals("FOO", FieldName.baseName("FOO")); // no dot
        assertEquals("", FieldName.baseName(""));
        assertEquals("FOO", FieldName.baseName("FOO."));
        assertEquals("", FieldName.baseName(".FOO"));
        // All-dot inputs: a splits[0]-based implementation would throw ArrayIndexOutOfBoundsException here.
        assertEquals("", FieldName.baseName("."));
        assertEquals("", FieldName.baseName(".."));
    }

    /**
     * The instance {@link FieldName#getBaseName()} agrees with the static method, and is memoized: repeated calls return the same reference.
     */
    @Test
    public void testInstanceBaseNameAgreesAndIsMemoized() {
        FieldName grouped = FieldName.of("FOO.BAR.1");
        assertEquals(FieldName.baseName("FOO.BAR.1"), grouped.getBaseName());

        FieldName dottedUngrouped = FieldName.of("FOO_3_BAR.FOO");
        assertEquals(FieldName.baseName("FOO_3_BAR.FOO"), dottedUngrouped.getBaseName());

        FieldName noDot = FieldName.of("FOO");
        assertEquals(FieldName.baseName("FOO"), noDot.getBaseName());

        // Memoization: the same reference is returned across calls.
        assertSame(grouped.getBaseName(), grouped.getBaseName());
    }

    /**
     * Group/instance resolution is memoized for grouped fields (same reference across calls), and the ungrouped sentinel path is exercised twice to confirm it
     * consistently yields a null group/instance and a false isGrouped.
     */
    @Test
    public void testGroupAndInstanceIsMemoized() {
        FieldName grouped = FieldName.of("NAME.PERSON.1");
        assertSame(grouped.getGroupAndInstance(), grouped.getGroupAndInstance());

        FieldName ungrouped = FieldName.of("FOO_3_BAR.FOO");
        assertNull(ungrouped.getGroupAndInstance());
        assertNull(ungrouped.getGroupAndInstance());
        assertFalse(ungrouped.isGrouped());
        assertFalse(ungrouped.isGrouped());
    }
}
