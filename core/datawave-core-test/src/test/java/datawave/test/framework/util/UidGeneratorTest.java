package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.junit.jupiter.api.Test;

import datawave.table.hash.UID;

public class UidGeneratorTest {

    @Test
    public void testConsistency() {
        String id = "uid-a";
        String uid = UidGenerator.uid(id);
        String next = UidGenerator.uid(id);
        assertEquals(uid, next);
    }

    /**
     * The id is encoded explicitly rather than through {@code String.getBytes()}, so a non-ASCII id produces the same uid regardless of the platform default
     * charset the test happens to run under.
     */
    @Test
    public void testIdIsEncodedAsUtf8() {
        String id = "uid-éü";
        String expected = UID.builder().newId(id.getBytes(StandardCharsets.UTF_8), (Date) null).toString();
        assertEquals(expected, UidGenerator.uid(id));
    }

    @Test
    public void testNullOrEmptyIdIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> UidGenerator.uid(null));
        assertThrows(IllegalArgumentException.class, () -> UidGenerator.uid(""));
    }
}
