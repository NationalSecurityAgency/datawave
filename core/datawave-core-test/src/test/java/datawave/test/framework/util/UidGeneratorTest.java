package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class UidGeneratorTest {

    @Test
    public void testConsistency() {
        String id = "uid-a";
        String uid = UidGenerator.uid(id);
        String next = UidGenerator.uid(id);
        assertEquals(uid, next);
    }
}
