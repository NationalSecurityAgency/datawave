package datawave.query.postprocessing.tf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PhraseIndexesTest {
    
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
        phraseIndexes.addIndexPair("BODY", 1, 3);
        phraseIndexes.addIndexPair("BODY", 10, 11);
        phraseIndexes.addIndexPair("CONTENT", 3, 4);
        phraseIndexes.addIndexPair("CONTENT", 12, 17);
        
        assertEquals("BODY:1,3:10,11/CONTENT:3,4:12,17", phraseIndexes.toString());
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
        expected.addIndexPair("BODY", 1, 3);
        expected.addIndexPair("BODY", 10, 11);
        expected.addIndexPair("CONTENT", 3, 4);
        expected.addIndexPair("CONTENT", 12, 17);
        
        PhraseIndexes actual = PhraseIndexes.from("BODY:1,3:10,11/CONTENT:3,4:12,17");
        assertEquals(expected, actual);
    }
}
