package datawave.table.hash;

import static datawave.table.hash.UIDConstants.TIME_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class HashUIDTest {

    private final String data = "20100901: the quick brown fox jumped over the lazy dog";
    private final String data2 = "20100831: the quick brown fox jumped over the lazy dog";

    @Test
    public void testParsing() {
        String uidString = "12a52.23b52.12c65";
        HashUID uid = HashUID.parse(uidString);
        assertEquals("12a52", uid.getOptionPrefix());
        assertEquals(Integer.parseInt("12a52", Character.MAX_RADIX), uid.getH0());
        assertEquals(Integer.parseInt("23b52", Character.MAX_RADIX), uid.getH1());
        assertEquals(Integer.parseInt("12c65", Character.MAX_RADIX), uid.getH2());
        assertEquals(-1, uid.getTime());
        assertNull(uid.getExtra());
        assertEquals(uidString, uid.toString());

        uidString = "12a52.23b52.12c65.something_extra";
        uid = UID.parse(uidString);
        assertEquals("12a52", uid.getOptionPrefix());
        assertEquals(Integer.parseInt("12a52", Character.MAX_RADIX), uid.getH0());
        assertEquals(Integer.parseInt("23b52", Character.MAX_RADIX), uid.getH1());
        assertEquals(Integer.parseInt("12c65", Character.MAX_RADIX), uid.getH2());
        assertEquals(-1, uid.getTime());
        assertEquals("something_extra", uid.getExtra());
        assertEquals(uidString, uid.toString());
    }

    @Test
    public void testParsingWithTime() {
        String uidString = "12a52.23b52.12c65+42c";
        HashUID uid = HashUID.parse(uidString);
        assertEquals("12a52", uid.getOptionPrefix());
        assertEquals(Integer.parseInt("12a52", Character.MAX_RADIX), uid.getH0());
        assertEquals(Integer.parseInt("23b52", Character.MAX_RADIX), uid.getH1());
        assertEquals(Integer.parseInt("12c65", Character.MAX_RADIX), uid.getH2());
        assertEquals(Integer.parseInt("42c", Character.MAX_RADIX), uid.getTime());
        assertNull(uid.getExtra());
        assertEquals(uidString, uid.toString());

        uidString = "12a52.23b52.12c65+42c.something_extra";
        uid = UID.parse(uidString);
        assertEquals("12a52", uid.getOptionPrefix());
        assertEquals(Integer.parseInt("12a52", Character.MAX_RADIX), uid.getH0());
        assertEquals(Integer.parseInt("23b52", Character.MAX_RADIX), uid.getH1());
        assertEquals(Integer.parseInt("12c65", Character.MAX_RADIX), uid.getH2());
        assertEquals(Integer.parseInt("42c", Character.MAX_RADIX), uid.getTime());
        assertEquals("something_extra", uid.getExtra());
        assertEquals(uidString, uid.toString());
    }

    @Test
    public void testEquals() {
        UID a = new HashUID(data.getBytes(), null) {};
        UID b = new HashUID(data.getBytes(), null) {};
        assertEquals(a, b);
        assertEquals(b, a);
        assertNull(a.getExtra());
        a = new HashUID(data.getBytes(), null, "blabla.blabla.blabla") {};
        b = new HashUID(data.getBytes(), null, "blabla.blabla.blabla") {};
        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals("blabla.blabla.blabla", a.getExtra());
    }

    @Test
    public void testDifference() {
        UID a = new HashUID(data.getBytes(), null) {};
        UID b = new HashUID(data2.getBytes(), null) {};
        assertNotEquals(a, b);
        assertNotEquals(b, a);
        a = new HashUID(data.getBytes(), null, "blabla.blabla.blabla.blabla.blabla.blabla.blabla") {};
        b = new HashUID(data2.getBytes(), null, "blabla.blabla.blabla.blabla.blabla.blabla.blabla") {};
        assertNotEquals(a, b);
        assertNotEquals(b, a);
        a = new HashUID(data.getBytes(), null, "blabla.blabla.blabla.blabla.blabla.blabla.blabla") {};
        b = new HashUID(data.getBytes(), null, "blebla.blabla.blabla.blabla.blabla.blabla.blabla") {};
        assertNotEquals(a, b);
        assertNotEquals(b, a);
    }

    @Test
    public void testComparisons() {
        UID a = new HashUID(data.getBytes(), null) {};
        UID b = new HashUID(data2.getBytes(), null) {};
        assertTrue(a.compareTo(b) != 0);
        a = new HashUID(data.getBytes(), null, "blabla.blabla") {};
        b = new HashUID(data2.getBytes(), null, "blabla.blabla") {};
        assertTrue(a.compareTo(b) != 0);
        a = new HashUID(data.getBytes(), null, "blabla.blabla") {};
        b = new HashUID(data.getBytes(), null, "blebla.blabla") {};
        assertTrue(a.compareTo(b) != 0);
        assertTrue(a.compareTo(null) != 0);
        assertEquals(0, a.compare(null, null));
        assertTrue(a.compare(a, null) != 0);
        assertTrue(a.compare(null, a) != 0);
    }

    @Test
    public void testParse() {
        UID a = new HashUID(data.getBytes(), null) {};
        UID b = UID.parse(a.toString());
        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(0, a.compareTo(b));
        assertEquals(0, a.compare(a, b));
        a = new HashUID(data.getBytes(), null, "blabla") {};
        b = UID.parse(a.toString());
        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(0, a.compareTo(b));
        assertEquals(0, a.compare(a, b));
        a = HashUID.parseBase(a.toString());
        b = HashUID.parseBase(b.toString());
        assertEquals(a, b);
        Exception exception = null;
        try {
            HashUID.parseBase("less_than_3_parts_makes_this_invalid");
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        assertNotNull(exception);
    }

    @Test
    public void testWritable() throws IOException {
        UID a = new HashUID(data.getBytes(), null) {};

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        a.write(out);
        out.close();

        UID b = new HashUID() {};
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        b.readFields(in);
        in.close();
        baos.close();

        assertEquals(a, b);
        assertEquals(b, a);

        a = new HashUID(data.getBytes(), null) {};

        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        a.write(out);
        out.close();

        b = new HashUID() {};
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        b.readFields(in);
        in.close();
        baos.close();

        assertEquals(a, b);
        assertEquals(b, a);

        a = new HashUID(data.getBytes(), null, "blabla") {};

        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        a.write(out);
        out.close();

        b = new HashUID() {};
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        b.readFields(in);
        in.close();
        baos.close();

        assertEquals(a, b);
        assertEquals(b, a);
    }

    @Test
    public void testExtra() {
        UID a = new HashUID(data.getBytes(), null) {};
        UID b = new HashUID(data.getBytes(), null, "blabla.blabla.blabla") {};
        assertTrue(b.toString().startsWith(a.toString()));
        assertEquals(a.getShardedPortion(), b.getShardedPortion());
        assertEquals(a.toString(), b.getShardedPortion());

        assertEquals(b, UID.parse(b.toString(), 4));
        assertEquals(UID.parse(b.toString(), 3), b);
        assertNotEquals(UID.parse(b.toString(), 2), b);
        assertEquals(UID.parse(b.toString(), 2).toString(), a + ".blabla.blabla");
        assertNotEquals(UID.parse(b.toString(), 1), b);
        assertEquals(UID.parse(b.toString(), 1).toString(), a + ".blabla");
        assertNotEquals(UID.parse(b.toString(), 0), b);
        assertEquals(UID.parse(b.toString(), 0), a);
        assertEquals(UID.parse(b.toString(), -1), b);
        assertEquals(UID.parse(b.toString(), -2), b);
        assertEquals(UID.parse(b.toString(), 3), b);
        assertNotEquals(UID.parse(b.toString(), 2), b);
        assertEquals(UID.parse(b.toString(), 2).toString(), a + ".blabla.blabla");
        assertNotEquals(UID.parse(b.toString(), 1), b);
        assertEquals(UID.parse(b.toString(), 1).toString(), a + ".blabla");
        assertNotEquals(UID.parse(b.toString(), 0), b);
        assertEquals(UID.parse(b.toString(), 0), a);
        assertEquals(UID.parse(b.toString(), -1), b);
        assertEquals(UID.parse(b.toString(), -2), b);

    }

    @Test
    public void testTime() {
        Date date = new Date(123412341);
        UID nodate = new HashUID(data.getBytes(), null) {};
        UID a = new HashUID(data.getBytes(), date) {};
        UID b = new HashUID(data.getBytes(), date, "blabla.blabla.blabla") {};
        assertTrue(b.toString().startsWith(a.toString()));
        assertEquals(a.getShardedPortion(), b.getShardedPortion());
        assertEquals(a.toString(), b.getShardedPortion());
        assertNotEquals(nodate.getShardedPortion(), a.getShardedPortion());

        assertTrue(a.getTime() >= 0);
        assertTrue(b.getTime() >= 0);
        assertEquals(a.getTime(), b.getTime());
        assertEquals(-1, nodate.getTime());
        assertTrue(a.getBaseUid().indexOf(TIME_SEPARATOR) > 0);
        assertTrue(nodate.getBaseUid().indexOf(TIME_SEPARATOR) < 0);

        assertEquals(UID.parse(b.toString(), 4), b);
        assertEquals(UID.parse(b.toString(), 3), b);
        assertNotEquals(UID.parse(b.toString(), 2), b);
        assertEquals(UID.parse(b.toString(), 2).toString(), a + ".blabla.blabla");
        assertNotEquals(UID.parse(b.toString(), 1), b);
        assertEquals(UID.parse(b.toString(), 1).toString(), a + ".blabla");
        assertNotEquals(UID.parse(b.toString(), 0), b);
        assertEquals(UID.parse(b.toString(), 0), a);
        assertEquals(UID.parse(b.toString(), -1), b);
        assertEquals(UID.parse(b.toString(), -2), b);
        assertEquals(UID.parse(b.toString(), 4), b);
        assertEquals(UID.parse(b.toString(), 3), b);
        assertNotEquals(UID.parse(b.toString(), 2), b);
        assertEquals(UID.parse(b.toString(), 2).toString(), a + ".blabla.blabla");
        assertNotEquals(UID.parse(b.toString(), 1), b);
        assertEquals(UID.parse(b.toString(), 1).toString(), a + ".blabla");
        assertNotEquals(UID.parse(b.toString(), 0), b);
        assertEquals(UID.parse(b.toString(), 0), a);
        assertEquals(UID.parse(b.toString(), -1), b);
        assertEquals(UID.parse(b.toString(), -2), b);
    }

    @Test
    public void testMiscellaneous() {
        HashUIDBuilder builder = new HashUIDBuilder();
        HashUID result1 = builder.newId((byte[]) null);
        assertNotNull(result1);

        HashUID result2 = HashUIDBuilder.newId((HashUID) null, (String) null);
        assertNull(result2);

        HashUID result3 = new HashUID(null);
        assertTrue(result3.getTime() < 0);
    }

}
