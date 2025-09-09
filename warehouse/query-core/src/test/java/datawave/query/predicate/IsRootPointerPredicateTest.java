package datawave.query.predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import datawave.data.hash.HashUID;
import datawave.data.hash.HashUIDBuilder;

public class IsRootPointerPredicateTest {

    @Test
    public void testIsRootPointerSansContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        HashUID uid = new HashUIDBuilder().newId(getRandomBytes(20), null);
        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString())));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString() + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString() + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString() + ".1.2.3.1.43.1")));

        assertTrue(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString())));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString() + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString() + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString() + ".1.2.3.1.43.1")));

        assertTrue(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + "\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + ".\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + ".1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + ".1.2.3.1.43.1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
    }

    @Test
    public void testIsRootPointerWithContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        HashUID uid = new HashUIDBuilder().newId(getRandomBytes(20), null);
        isRootPointer.startNewDocument(new Key("shard", "dt\u0000" + uid.toString()));

        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString())));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString() + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString() + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + uid.toString() + ".1.2.3.1.43.1")));

        assertTrue(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString())));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString() + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString() + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + uid.toString() + ".1.2.3.1.43.1")));

        assertTrue(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + "\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + ".\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + ".1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + uid.toString() + ".1.2.3.1.43.1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
    }

    private byte[] getRandomBytes(int len) {
        Random rand = new Random();
        byte[] bytes = new byte[len];
        rand.nextBytes(bytes);
        return bytes;
    }
}
