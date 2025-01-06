package datawave.query.iterator.logic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class WordsAndScoresTest {

    List<String> hitTermsList = List.of("hit");

    @Test
    public void testSingleWordScoreAdd() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 100, hitTermsList);
        assertEquals("test(100)", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testSingleWordAdd() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", hitTermsList);
        assertEquals("test", ws.getWordToOutput());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testReturnSmallestScore() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 37654470, hitTermsList);
        ws.addTerm("austin", 47325112, hitTermsList);
        ws.addTerm("was", 26381694, hitTermsList);
        ws.addTerm("here", 49883548, hitTermsList);
        ws.addTerm("datawave", 24734968, hitTermsList);
        ws.addTerm("cat", 4999951, hitTermsList);
        assertEquals("cat(61)", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testReturnLongestWord() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", hitTermsList);
        ws.addTerm("austin", hitTermsList);
        ws.addTerm("was", hitTermsList);
        ws.addTerm("here", hitTermsList);
        ws.addTerm("datawave", hitTermsList);
        ws.addTerm("cat", hitTermsList);
        assertEquals("datawave", ws.getWordToOutput());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testReturnMixedAddScoreFirst() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 3835388, hitTermsList);
        ws.addTerm("austin", hitTermsList);
        ws.addTerm("was", hitTermsList);
        ws.addTerm("here", 5239977, hitTermsList);
        ws.addTerm("datawave", hitTermsList);
        ws.addTerm("cat", 9707535, hitTermsList);
        assertEquals("test(68)", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testReturnMixedAddNoScoreFirst() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", hitTermsList);
        ws.addTerm("austin", 22255500, hitTermsList);
        ws.addTerm("was", hitTermsList);
        ws.addTerm("here", 730921, hitTermsList);
        ws.addTerm("datawave", hitTermsList);
        ws.addTerm("cat", 11232252, hitTermsList);
        assertEquals("here(93)", ws.getWordToOutput());
        assertTrue(ws.getUseScores());
    }

    @Test
    public void testReturnAddNegativeScores() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", hitTermsList);
        ws.addTerm("austin", -1, hitTermsList);
        ws.addTerm("was", hitTermsList);
        ws.addTerm("here", -1, hitTermsList);
        ws.addTerm("datawave", hitTermsList);
        ws.addTerm("cat", -1, hitTermsList);
        assertEquals("datawave", ws.getWordToOutput());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testGetWordWithNothingAdded() {
        WordsAndScores ws = new WordsAndScores();
        assertEquals("REPORTMETODATAWAVE", ws.getWordToOutput());
    }

    @Test
    public void testSetWordsList() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("add", hitTermsList);
        ws.addTerm("more", hitTermsList);
        ws.addTerm("things", hitTermsList);
        int before = ws.getScoresList().size();
        ws.setWordsList(new ArrayList<>(List.of("stuff", "things")), hitTermsList);
        assertNotEquals(before, ws.getScoresList().size());
        ws.addTerm("three", hitTermsList);
        assertEquals(before, ws.getScoresList().size());
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testSetWordsAndScoresLists() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("add", 86, hitTermsList);
        ws.addTerm("more", 6, hitTermsList);
        ws.addTerm("things", 34, hitTermsList);
        assertThrows(IllegalArgumentException.class,
                        () -> ws.setWordsAndScoresList((new ArrayList<>(List.of("stuff", "things"))), new ArrayList<>(List.of(1, 2, 3)), hitTermsList));
        assertDoesNotThrow(() -> ws.setWordsAndScoresList((new ArrayList<>(List.of("stuff", "things"))), new ArrayList<>(List.of(1, 2)), hitTermsList));
        assertTrue(ws.getUseScores());
        assertDoesNotThrow(() -> ws.setWordsAndScoresList((new ArrayList<>(List.of("stuff", "things"))), new ArrayList<>(List.of(-1, -1)), hitTermsList));
        assertFalse(ws.getUseScores());
    }

    @Test
    public void testReturnSingleHit() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("hit", hitTermsList);
        assertEquals("[hit]", ws.getWordToOutput());
        ws.addTerm("try", hitTermsList);
        assertEquals("[hit]", ws.getWordToOutput());
        ws.reset();
        ws.addTerm("hello", hitTermsList);
        assertEquals("hello", ws.getWordToOutput());
        ws.addTerm("hit", hitTermsList);
        assertEquals("[hit]", ws.getWordToOutput());
    }

    @Test
    public void testReturnSingleHitWithScore() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("hit", 9120447, hitTermsList);
        assertEquals("[hit(40)]", ws.getWordToOutput());
        ws.addTerm("try", 41315662, hitTermsList);
        assertEquals("[hit(40)]", ws.getWordToOutput());
        ws.reset();
        ws.addTerm("hello", 31334736, hitTermsList);
        assertEquals("hello(4)", ws.getWordToOutput());
        ws.addTerm("hit", 29938894, hitTermsList);
        assertEquals("[hit(5)]", ws.getWordToOutput());
    }

    @Test
    public void testReturnSingleHitMixedScore() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", hitTermsList);
        ws.addTerm("austin", 41096759, hitTermsList);
        ws.addTerm("hit", hitTermsList);
        ws.addTerm("here", 33072572, hitTermsList);
        assertEquals("[hit]", ws.getWordToOutput());
        ws.reset();
        ws.addTerm("test", 21719522, hitTermsList);
        ws.addTerm("austin", hitTermsList);
        ws.addTerm("hit", hitTermsList);
        ws.addTerm("here", 43027819, hitTermsList);
        assertEquals("[hit]", ws.getWordToOutput());
    }

    @Test
    public void testReturnScoreNotLongestMultipleHit() {
        WordsAndScores ws = new WordsAndScores();
        List<String> temp = List.of("hit", "term");
        ws.addTerm("test", 49703356, temp);
        ws.addTerm("hit", 33698372, temp);
        ws.addTerm("was", temp);
        ws.addTerm("here", 15201307, temp);
        ws.addTerm("term", temp);
        ws.addTerm("cat", temp);
        assertEquals("[hit(3)]", ws.getWordToOutput());
    }

    @Test
    public void testReturnScoreMultipleHit() {
        WordsAndScores ws = new WordsAndScores();
        List<String> temp = List.of("hit", "term");
        ws.addTerm("test", 6788348, temp);
        ws.addTerm("hit", 15257973, temp);
        ws.addTerm("was", temp);
        ws.addTerm("here", 17286266, temp);
        ws.addTerm("term", 37536662, temp);
        ws.addTerm("cat", temp);
        assertEquals("[hit(22)]", ws.getWordToOutput());
    }

    @Test
    public void testReturnStopWord() {
        WordsAndScores ws = new WordsAndScores();
        List<String> temp = List.of("<eps>");
        ws.addTerm("<eps>", 5, temp);
        ws.addTerm("hi", 5, temp);
        assertNull(ws.getWordToOutput());
    }

    @Test
    public void testOverride() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", hitTermsList);
        ws.addTerm("austin", 41096759, hitTermsList);
        ws.addTerm("hit", hitTermsList);
        ws.addTerm("here", 33072572, hitTermsList);
        ws.setOverride(3, 1);
        assertEquals("[here(4)", ws.getWordToOutput());
        ws.reset();
        ws.addTerm("test", 21719522, hitTermsList);
        ws.addTerm("austin", hitTermsList);
        ws.addTerm("hit", hitTermsList);
        ws.addTerm("here", 43027819, hitTermsList);
        ws.setOverride(1, 2);
        assertEquals("austin", ws.getWordToOutput());
        ws.reset();
        ws.addTerm("test", 21719522, hitTermsList);
        ws.addTerm("austin", hitTermsList);
        ws.addTerm("hit", hitTermsList);
        ws.addTerm("here", 43027819, hitTermsList);
        ws.setOverride(3, 3);
        assertEquals("here(1)]", ws.getWordToOutput());
    }

    @Test
    public void testOnebestExcerpt() {
        WordsAndScores ws = new WordsAndScores();
        ws.setOneBestExcerpt(true);
        ws.addTerm("test", 37654470, hitTermsList);
        ws.addTerm("austin", 47325112, hitTermsList);
        ws.addTerm("was", 26381694, hitTermsList);
        ws.addTerm("here", 49883548, hitTermsList);
        ws.addTerm("datawave", 24734968, hitTermsList);
        ws.addTerm("hit", 4999951, hitTermsList);
        assertEquals("[hit]", ws.getWordToOutput());
        ws.addTerm("cat", 12548, hitTermsList);
        assertEquals("cat", ws.getWordToOutput());
    }

    @Test
    public void testOutputScores() {
        WordsAndScores ws = new WordsAndScores();
        ws.addTerm("test", 37654470, hitTermsList);
        ws.addTerm("austin", 47325112, hitTermsList);
        ws.addTerm("was", 26381694, hitTermsList);
        ws.addTerm("here", 49883548, hitTermsList);
        ws.addTerm("datawave", 24734968, hitTermsList);
        ws.addTerm("cat", 4999951, hitTermsList);
        ws.setOutputScores(false);
        assertEquals("cat", ws.getWordToOutput());
        ws.setOutputScores(true);
        assertEquals("cat(61)", ws.getWordToOutput());
    }
}
