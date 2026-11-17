package datawave.ingest.mapreduce.handler.shard.content;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 *
 */
public class TermAndZoneTest {

    @Test
    public void testConstructor() {

        String term = "hello";
        String zone = "world";
        String token = String.format("%s:%s", term, zone);

        TermAndZone uut = new TermAndZone(token);

        assertEquals(term, uut.term, "TermAndZone Constructor failed to parse 'term' correctly from the token.");
        assertEquals(zone, uut.zone, "TermAndZone Constructor failed to parse 'zone' correctly from the token.");

        try {

            token = "Good-bye";

            uut = new TermAndZone(token);

            fail("TermAndZone Constructor failed to throw expected exception.");

        } catch (IllegalArgumentException iae) {

            String msg = iae.getMessage();

            assertTrue(msg.startsWith("Missing zone in token: "), "TermAndZone Constructor threw the expected exception but it contained the wrong message.");
        }
    }

    @Test
    public void testEquals() {

        String token = "Hello:World";
        String otherToken = "term:zone";

        TermAndZone uut = new TermAndZone(token);
        TermAndZone match = new TermAndZone(token);
        TermAndZone other = new TermAndZone(otherToken);
        Object obj = new Object();

        assertTrue(uut.equals(match), "Equals failed to recognize a matching instance");
        assertFalse(uut.equals(other), "Equals incorrectly matched different instance");
        assertFalse(uut.equals(obj), "Equals incorrectly matched a non-instance");
    }

    @Test
    public void testCompare() {

        String token = "Hello:World";
        String lessZonetoken = "Hello:world";
        String greaterZonetoken = "Hello:WORLD";
        String lessTermToken = "term:World";
        String greaterTermToken = "Ello:World";

        TermAndZone uut = new TermAndZone(token);
        TermAndZone match = new TermAndZone(token);
        TermAndZone lessZone = new TermAndZone(lessZonetoken);
        TermAndZone greaterZone = new TermAndZone(greaterZonetoken);

        TermAndZone lessTerm = new TermAndZone(lessTermToken);
        TermAndZone greaterTerm = new TermAndZone(greaterTermToken);

        assertTrue((0 == uut.compareTo(match)), "CompareTo failed to recognize a matching instance.");
        assertTrue((0 > uut.compareTo(lessZone)), "CompareTo failed to recognize an instance containing a less than Zone value.");
        assertTrue((0 > uut.compareTo(lessTerm)), "CompareTo failed to recognize an instance containing a less than Term value.");
        assertTrue((0 < uut.compareTo(greaterZone)), "CompareTo failed to recognize an instance containing a greater than Zone value.");
        assertTrue((0 < uut.compareTo(greaterTerm)), "CompareTo failed to recognize an instance containing a greater than Term value.");
    }
}
