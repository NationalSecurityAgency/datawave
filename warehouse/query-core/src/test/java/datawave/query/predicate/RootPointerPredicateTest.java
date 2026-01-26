package datawave.query.predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class RootPointerPredicateTest {

    private static final String UID = "2943f2.w98ueh.10328";
    private static final String SHORT_UID = "3f2.w98ueh.10328";

    @Test
    public void testEventKeyIsRootPointerSansContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + UID)));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + UID + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + UID + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + UID + ".1.2.3.1.43.1")));
    }

    @Test
    public void testFIKeyIsRootPointerSansContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        assertTrue(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID)));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID + ".1.2.3.1.43.1")));
    }

    @Test
    public void testTFKeyIsRootPointerSansContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        assertTrue(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + "\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + ".\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + ".1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + ".1.2.3.1.43.1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
    }

    @Test
    public void testEventKeyIsRootPointerWithContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        isRootPointer.startNewDocument(new Key("shard", "dt\u0000" + UID));

        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + UID)));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + UID + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + UID + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + UID + ".1.2.3.1.43.1")));
    }

    @Test
    public void testFIKeyIsRootPointerWithContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        isRootPointer.startNewDocument(new Key("shard", "dt\u0000" + UID));

        assertTrue(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID)));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "fi\u0000FIELD_A", "VALUE_A\u0000dt\u0000" + UID + ".1.2.3.1.43.1")));
    }

    @Test
    public void testTFKeyIsRootPointerWithContext() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        isRootPointer.startNewDocument(new Key("shard", "dt\u0000" + UID));

        assertTrue(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + "\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + ".\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + ".1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
        assertFalse(isRootPointer.apply(new Key("shard", "tf", "dt\u0000" + UID + ".1.2.3.1.43.1\u0000TOKEN_A\u0000TOKEN_FIELD_A")));
    }

    @Test
    public void testEventKeyIsRootPointerSansContextFailure() {
        RootPointerPredicate isRootPointer = new RootPointerPredicate();

        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID)));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".1.2.3.1.43.1")));

        // The following four cases are the failure cases that cannot be handled without context
        // and are incorrectly identified as root pointers
        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".")));
        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".1")));
        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".1.")));
        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".10")));

        // now try it with context and prove it works
        isRootPointer.startNewDocument(new Key("shard", "dt\u0000" + SHORT_UID));

        assertTrue(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID)));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".1.2.3.1.43.1")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".1")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".1.")));
        assertFalse(isRootPointer.apply(new Key("shard", "dt\u0000" + SHORT_UID + ".10")));
    }

}
