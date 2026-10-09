package datawave.next.scanner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

/**
 * Verifies the compare key used to order candidates on the sorted candidate queue.
 * <p>
 * The column family is normally {@code datatype\0uid}, which is inverted so that candidates sort by uid first and spread across datatypes. A column family that
 * carries no separator must not break construction.
 */
public class KeyWithContextTest {

    private static final String NULL_BYTE = "\0";

    @Test
    public void testCompareKeyInvertsDatatypeAndUid() {
        KeyWithContext keyWithContext = new KeyWithContext(new Key("row", "datatype" + NULL_BYTE + "uid-1"), null, true);

        // uid first, then datatype
        KeyWithContext other = new KeyWithContext(new Key("row", "datatype" + NULL_BYTE + "uid-0"), null, true);
        assertTrue(keyWithContext.compareTo(other) > 0, "uid-1 must sort after uid-0");
    }

    /**
     * A column family with no null byte previously threw a StringIndexOutOfBoundsException while building the compare key.
     */
    @Test
    public void testColumnFamilyWithoutSeparatorDoesNotThrow() {
        assertDoesNotThrow(() -> new KeyWithContext(new Key("row", "no-separator"), null, true));
    }

    @Test
    public void testColumnFamilyWithoutSeparatorStillOrders() {
        KeyWithContext first = new KeyWithContext(new Key("row", "aaa"), null, true);
        KeyWithContext second = new KeyWithContext(new Key("row", "bbb"), null, true);

        assertTrue(first.compareTo(second) < 0, "column families without a separator must still order");
        assertTrue(second.compareTo(first) > 0);
        assertEquals(0, first.compareTo(new KeyWithContext(new Key("row", "aaa"), null, true)));
    }

    /**
     * An empty column family is the degenerate case of the same problem.
     */
    @Test
    public void testEmptyColumnFamilyDoesNotThrow() {
        assertDoesNotThrow(() -> new KeyWithContext(new Key("row"), null, true));
    }

    /**
     * Mixing separated and unseparated column families must still produce a usable ordering rather than throwing.
     */
    @Test
    public void testSortingMixedColumnFamilies() {
        List<KeyWithContext> keys = new ArrayList<>();
        keys.add(new KeyWithContext(new Key("row", "datatype" + NULL_BYTE + "uid-2"), null, true));
        keys.add(new KeyWithContext(new Key("row", "malformed"), null, true));
        keys.add(new KeyWithContext(new Key("row", "datatype" + NULL_BYTE + "uid-1"), null, true));

        assertDoesNotThrow(() -> keys.sort(KeyWithContext::compareTo));
        assertEquals(3, keys.size());
    }

    /**
     * When the compare key is not built the natural key ordering is used instead.
     */
    @Test
    public void testWithoutCompareKeyFallsBackToKeyOrder() {
        KeyWithContext first = new KeyWithContext(new Key("row", "datatype" + NULL_BYTE + "uid-1"), null, false);
        KeyWithContext second = new KeyWithContext(new Key("row", "datatype" + NULL_BYTE + "uid-2"), null, false);

        assertTrue(first.compareTo(second) < 0);
    }
}
