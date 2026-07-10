package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import datawave.query.function.FieldName.GroupAndInstance;

public class FieldNameTest {

    @Test
    public void testGroupedFieldParses() {
        FieldName fieldName = FieldName.parse("FOO_3.FOO.4.5");
        assertTrue(fieldName.isGrouped());
        assertEquals("FOO_3.FOO.4.5", fieldName.getName());
        assertEquals("FOO", fieldName.getGroupAndInstance().getGroup());
        assertEquals("5", fieldName.getGroupAndInstance().getInstance());

        fieldName = FieldName.parse("FOO_3_BAR.FOO.4");
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
        FieldName fieldName = FieldName.parse("CAT.PET.0");
        assertTrue(fieldName.isGrouped());
        assertEquals("CAT.PET.0", fieldName.getName());
        assertEquals("PET", fieldName.getGroupAndInstance().getGroup());
        assertEquals("0", fieldName.getGroupAndInstance().getInstance());
    }

    /**
     * Trailing empty tokens are ignored, so a well-formed field with trailing periods still parses as grouped. The case an earlier index-based parser got wrong.
     */
    @Test
    public void testTrailingPeriodsAreIgnored() {
        FieldName fieldName = FieldName.parse("A.B.C.");
        assertTrue(fieldName.isGrouped());
        assertEquals("B", fieldName.getGroupAndInstance().getGroup());
        assertEquals("C", fieldName.getGroupAndInstance().getInstance());

        fieldName = FieldName.parse("A.B.C..");
        assertTrue(fieldName.isGrouped());
        assertEquals("B", fieldName.getGroupAndInstance().getGroup());
        assertEquals("C", fieldName.getGroupAndInstance().getInstance());
    }

    @Test
    public void testUngroupedFieldsHaveNoGroupAndInstance() {
        for (String field : new String[] {"FOO_3", "FOO_3_BAR.", "FOO_3_BAR.FOO", "FOO_3_BAR.FOO.", "FOO_3_BAR.."}) {
            FieldName fieldName = FieldName.parse(field);
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
        FieldName name = FieldName.parse("NAME.PERSON.1");
        FieldName age = FieldName.parse("AGE.PERSON.1");

        // FieldName equality is over the full name - different names are not equal
        assertNotEquals(name, age);
        assertEquals(name, FieldName.parse("NAME.PERSON.1"));
        assertEquals(name.hashCode(), FieldName.parse("NAME.PERSON.1").hashCode());

        // GroupAndInstance identity is shared across the two sibling fields
        assertEquals(name.getGroupAndInstance(), age.getGroupAndInstance());
        assertEquals(name.getGroupAndInstance().hashCode(), age.getGroupAndInstance().hashCode());
    }

    @Test
    public void testGroupAndInstanceHashCodeAndEquals() {
        // Equal group/instance (different field components)
        GroupAndInstance t1 = FieldName.parse("FOO_3.FOO.4.3").getGroupAndInstance();
        GroupAndInstance t2 = FieldName.parse("FOO_4.FOO.5.3").getGroupAndInstance();
        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());

        // Equal group/instance (different field component)
        t1 = FieldName.parse("F_1.FOO.4.3").getGroupAndInstance();
        t2 = FieldName.parse("FOO_4.FOO.5.3").getGroupAndInstance();
        assertEquals(t1, t2);
        assertEquals(t1.hashCode(), t2.hashCode());

        // Mismatch group
        t1 = FieldName.parse("FOO_3.FOO.4.3").getGroupAndInstance();
        t2 = FieldName.parse("FOO_4.AB.5.3").getGroupAndInstance();
        assertNotEquals(t1, t2);

        // Mismatch instance
        t1 = FieldName.parse("FOO_3.FOO.4.30").getGroupAndInstance();
        t2 = FieldName.parse("FOO_3.FOO.5.40").getGroupAndInstance();
        assertNotEquals(t1, t2);

        // Mismatch instance
        t1 = FieldName.parse("FOO_3.FOO.4.30").getGroupAndInstance();
        t2 = FieldName.parse("FOO_3.FOO.5.31").getGroupAndInstance();
        assertNotEquals(t1, t2);
    }
}
