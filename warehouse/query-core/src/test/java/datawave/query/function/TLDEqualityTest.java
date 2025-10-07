package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

class TLDEqualityTest {
    private final Key key = new Key("row", "datatype\0-7m7uk9.oz9qpy.-nfahrv");
    private final Key keyChildOne = new Key("row", "datatype\0-7m7uk9.oz9qpy.-nfahrv.123.987");
    private final Key keyChildTwo = new Key("row", "datatype\0-7m7uk9.oz9qpy.-nfahrv.123.777");
    private final Key otherKey = new Key("row", "datatype\0-7m7uk9.oz9qpy.-aaazzz");
    private final Key otherKeyChildOne = new Key("row", "datatype\0-7m7uk9.oz9qpy.-aaazzz.123.987");

    private final TLDEquality equality = new TLDEquality();

    @Test
    void testSameParent() {
        assertTrue(equality.partOf(key, key));
        assertTrue(equality.partOf(otherKey, otherKey));
    }

    @Test
    void testDifferentParents() {
        assertFalse(equality.partOf(key, otherKey));
        assertFalse(equality.partOf(otherKey, key));
    }

    @Test
    void testKeysOfDifferentDepths() {
        assertTrue(equality.partOf(key, keyChildOne));
        assertTrue(equality.partOf(keyChildOne, key));

        assertTrue(equality.partOf(otherKey, otherKeyChildOne));
        assertTrue(equality.partOf(otherKeyChildOne, otherKey));
    }

    @Test
    void testSameParentSameChildren() {
        assertTrue(equality.partOf(keyChildOne, keyChildOne));
        assertTrue(equality.partOf(keyChildTwo, keyChildTwo));

        assertTrue(equality.partOf(otherKeyChildOne, otherKeyChildOne));
    }

    @Test
    void testSameParentDifferentChildren() {
        assertTrue(equality.partOf(keyChildOne, keyChildTwo));
        assertTrue(equality.partOf(keyChildTwo, keyChildOne));
    }

    @Test
    void testDifferentParentSameChildren() {
        assertFalse(equality.partOf(keyChildOne, otherKeyChildOne));
        assertFalse(equality.partOf(otherKeyChildOne, keyChildOne));
    }

    @Test
    void testDifferentParentDifferentChildren() {
        assertFalse(equality.partOf(keyChildTwo, otherKeyChildOne));
        assertFalse(equality.partOf(otherKeyChildOne, keyChildTwo));
    }

    @Test
    void testEdgeCases() {
        assertFalse(equality.partOf(key, new Key("", "")));
        assertFalse(equality.partOf(new Key("", ""), key));

        // in practice this should never happen
        Key malformedUid = new Key("row", "datatype\0-7m7uk9.oz9qpy.-");
        assertTrue(equality.partOf(key, malformedUid));
        assertTrue(equality.partOf(malformedUid, key));
    }
}
