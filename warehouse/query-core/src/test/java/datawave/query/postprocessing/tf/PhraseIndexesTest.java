package datawave.query.postprocessing.tf;

import org.javatuples.Triplet;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PhraseIndexesTest {
    
    private static final String EVENT_ID_1 = "shard1\u0000dt\u0000uid1";
    private static final String EVENT_ID_2 = "shard2\u0000dt\u0000uid2";
    
    /**
     * Verify formatting an empty {@link PhraseIndexes} returns an empty string.
     */
    @Test
    public void testEmptyPhraseOffsetsToString() {
        assertEquals("", new PhraseIndexes().toString());
    }
    
    /**
     * Verify formatting a non-empty {@link PhraseIndexes} to a string.
     */
    @Test
    public void testNonEmptyPhraseOffsetsToString() {
        PhraseIndexes phraseIndexes = new PhraseIndexes();
        phraseIndexes.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        phraseIndexes.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        phraseIndexes.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        phraseIndexes.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);
        
        assertEquals("BODY:" + EVENT_ID_1 + ",1,3:" + EVENT_ID_2 + ",10,11/CONTENT:" + EVENT_ID_1 + ",3,4:" + EVENT_ID_2 + ",12,17", phraseIndexes.toString());
    }
    
    /**
     * Verify that {@link PhraseIndexes#from(String)} returns null when given a null input.
     */
    @Test
    public void testParsingFromNullString() {
        assertNull(PhraseIndexes.from(null));
    }
    
    /**
     * Verify that {@link PhraseIndexes#from(String)} returns a non-null, empty {@link PhraseIndexes} from a blank string.
     */
    @Test
    public void testParsingFromEmpytString() {
        assertTrue(PhraseIndexes.from("   ").isEmpty());
    }
    
    /**
     * Verify that {@link PhraseIndexes#from(String)} correctly parses a non-blank string.
     */
    @Test
    public void testParsingFromNonBlankString() {
        PhraseIndexes expected = new PhraseIndexes();
        expected.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        expected.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        expected.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        expected.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);
        
        PhraseIndexes actual = PhraseIndexes.from("BODY:" + EVENT_ID_1 + ",1,3:" + EVENT_ID_2 + ",10,11/CONTENT:" + EVENT_ID_1 + ",3,4:" + EVENT_ID_2
                        + ",12,17");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testOverlappingPhraseOffsets() {
        PhraseIndexes expected = new PhraseIndexes();
        expected.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        expected.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        expected.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        expected.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);
        
        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 2);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 2, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 11, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 4, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 13);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 14, 15);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 15, 17);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 13, 14);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void testToMap() {
        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 5, 8);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 19, 20);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);
        
        Map<String,Map<String,PhraseIndexes>> map = actual.toMap();
        
        assertEquals(2, map.size());
        assertEquals(2, map.get("BODY").size());
        assertEquals(2, map.get("CONTENT").size());
        assertEquals("BODY:" + EVENT_ID_1 + ",1,3:" + EVENT_ID_1 + ",5,8", map.get("BODY").get(EVENT_ID_1).toString());
        assertEquals("BODY:" + EVENT_ID_2 + ",10,11", map.get("BODY").get(EVENT_ID_2).toString());
        assertEquals("CONTENT:" + EVENT_ID_1 + ",3,4", map.get("CONTENT").get(EVENT_ID_1).toString());
        assertEquals("CONTENT:" + EVENT_ID_2 + ",12,17:" + EVENT_ID_2 + ",19,20", map.get("CONTENT").get(EVENT_ID_2).toString());
    }
    
    @Test
    public void testOverlap() {
        PhraseIndexes actual = new PhraseIndexes();
        actual.addIndexTriplet("BODY", EVENT_ID_1, 1, 3);
        actual.addIndexTriplet("BODY", EVENT_ID_1, 5, 8);
        actual.addIndexTriplet("BODY", EVENT_ID_2, 10, 11);
        actual.addIndexTriplet("CONTENT", EVENT_ID_1, 3, 4);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 19, 20);
        actual.addIndexTriplet("CONTENT", EVENT_ID_2, 12, 17);
        
        assertEquals(null, actual.getOverlap("BODY", EVENT_ID_1, 4, 4));
        assertEquals(new Triplet(EVENT_ID_1, 1, 3), actual.getOverlap("BODY", EVENT_ID_1, 3, 3));
        assertEquals(new Triplet(EVENT_ID_1, 1, 3), actual.getOverlap("BODY", EVENT_ID_1, 0, 1));
        assertEquals(new Triplet(EVENT_ID_2, 19, 20), actual.getOverlap("CONTENT", EVENT_ID_2, 18, 21));
        assertEquals(new Triplet(EVENT_ID_2, 12, 17), actual.getOverlap("CONTENT", EVENT_ID_2, 17, 21));
    }
}
