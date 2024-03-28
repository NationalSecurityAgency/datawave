package datawave.query.iterator.logic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class WordsAndScoresTest {

    @Test
    public void testSingleWordScoreAdd() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 100);
        assertEquals("test", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testSingleWordAdd() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test");
        assertEquals("test", ws.getWordToOutput());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testReturnSmallestScore() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 34);
        ws.addTerm("austin", 72);
        ws.addTerm("was", 50);
        ws.addTerm("here", 76);
        ws.addTerm("datawave", 6);
        ws.addTerm("cat", 63);
        assertEquals("datawave", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testReturnLongestWord() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test");
        ws.addTerm("austin");
        ws.addTerm("was");
        ws.addTerm("here");
        ws.addTerm("datawave");
        ws.addTerm("cat");
        assertEquals("datawave", ws.getWordToOutput());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testReturnMixedAddScoreFirst() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 65);
        ws.addTerm("austin");
        ws.addTerm("was");
        ws.addTerm("here", 34);
        ws.addTerm("datawave");
        ws.addTerm("cat", 71);
        assertEquals("here", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testReturnMixedAddNoScoreFirst() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test");
        ws.addTerm("austin", 65);
        ws.addTerm("was");
        ws.addTerm("here", 34);
        ws.addTerm("datawave");
        ws.addTerm("cat", 71);
        assertEquals("here", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testReturnAddNegativeScores() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test");
        ws.addTerm("austin", -1);
        ws.addTerm("was");
        ws.addTerm("here", -1);
        ws.addTerm("datawave");
        ws.addTerm("cat", -1);
        assertEquals("datawave", ws.getWordToOutput());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testGetWordWithNothingAdded() {
        WordsAndScores ws = new WordsAndScores();
        assertEquals("reportmetodatawave", ws.getWordToOutput());
    }

    @Test
    public void testSetWordsList() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("add");
        ws.addTerm("more");
        ws.addTerm("things");
        int before = ws.getScoresList().size();
        ws.setWordsList(new ArrayList<>(List.of("stuff", "things")));
        assertNotEquals(before, ws.getScoresList().size());
        ws.addTerm("three");
        assertEquals(before, ws.getScoresList().size());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testSetWordsAndScoresLists() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("add", 86);
        ws.addTerm("more", 6);
        ws.addTerm("things", 34);
        assertThrows(IllegalArgumentException.class,
                        () -> ws.setWordsAndScoresList((new ArrayList<>(List.of("stuff", "things"))), new ArrayList<>(List.of(1, 2, 3))));
        assertDoesNotThrow(() -> ws.setWordsAndScoresList((new ArrayList<>(List.of("stuff", "things"))), new ArrayList<>(List.of(1, 2))));
        assertTrue(ws.getUseScores());
        assertDoesNotThrow(() -> ws.setWordsAndScoresList((new ArrayList<>(List.of("stuff", "things"))), new ArrayList<>(List.of(-1, -1))));
        assertFalse(ws.getUseScores());
    }
}
