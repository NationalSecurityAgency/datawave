package datawave.query.postprocessing.tf;

import org.junit.Test;

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
}
