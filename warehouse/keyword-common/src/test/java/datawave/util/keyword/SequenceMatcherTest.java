package datawave.util.keyword;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * JUnit 5 tests for SequenceMatcher, ported from Python difflib test_difflib.py.
 * <p/>
 * This test suite covers the core functionality of the SequenceMatcher class including: - Basic sequence matching and difference detection - Junk filtering
 * functionality - AutoJunk heuristic for popular characters - Various ratio calculations (ratio, quickRatio, realQuickRatio) - Edge cases and bug regression
 * tests - Static utility methods like getCloseMatches
 * <p/>
 * Comments and test structure preserved from the original Python test suite where appropriate.
 */
public class SequenceMatcherTest {

    /**
     * Tests with ASCII characters - ported from TestWithAscii
     */
    @Nested
    class TestWithAscii {

        @Test
        public void testOneInsert() {
            // Test one insert at beginning
            SequenceMatcher sm = new SequenceMatcher(null, "b".repeat(100), "a" + "b".repeat(100), true);
            assertEquals(0.995, sm.ratio(), 0.001);

            List<SequenceMatcher.Opcode> opcodes = sm.getOpcodes();
            assertEquals(2, opcodes.size());
            assertEquals(SequenceMatcher.OpcodeTag.INSERT, opcodes.get(0).tag);
            assertEquals(0, opcodes.get(0).i1);
            assertEquals(0, opcodes.get(0).i2);
            assertEquals(0, opcodes.get(0).j1);
            assertEquals(1, opcodes.get(0).j2);
            assertEquals(SequenceMatcher.OpcodeTag.EQUAL, opcodes.get(1).tag);
            assertEquals(0, opcodes.get(1).i1);
            assertEquals(100, opcodes.get(1).i2);
            assertEquals(1, opcodes.get(1).j1);
            assertEquals(101, opcodes.get(1).j2);

            // Test one insert in middle
            sm = new SequenceMatcher(null, "b".repeat(100), "b".repeat(50) + "a" + "b".repeat(50), true);
            assertEquals(0.995, sm.ratio(), 0.001);

            opcodes = sm.getOpcodes();
            assertEquals(3, opcodes.size());
            assertEquals(SequenceMatcher.OpcodeTag.EQUAL, opcodes.get(0).tag);
            assertEquals(0, opcodes.get(0).i1);
            assertEquals(50, opcodes.get(0).i2);
            assertEquals(0, opcodes.get(0).j1);
            assertEquals(50, opcodes.get(0).j2);
            assertEquals(SequenceMatcher.OpcodeTag.INSERT, opcodes.get(1).tag);
            assertEquals(50, opcodes.get(1).i1);
            assertEquals(50, opcodes.get(1).i2);
            assertEquals(50, opcodes.get(1).j1);
            assertEquals(51, opcodes.get(1).j2);
            assertEquals(SequenceMatcher.OpcodeTag.EQUAL, opcodes.get(2).tag);
            assertEquals(50, opcodes.get(2).i1);
            assertEquals(100, opcodes.get(2).i2);
            assertEquals(51, opcodes.get(2).j1);
            assertEquals(101, opcodes.get(2).j2);
        }

        @Test
        public void testOneDelete() {
            SequenceMatcher sm = new SequenceMatcher(null, "a".repeat(40) + "c" + "b".repeat(40), "a".repeat(40) + "b".repeat(40), true);
            assertEquals(0.994, sm.ratio(), 0.001);

            List<SequenceMatcher.Opcode> opcodes = sm.getOpcodes();
            assertEquals(3, opcodes.size());
            assertEquals(SequenceMatcher.OpcodeTag.EQUAL, opcodes.get(0).tag);
            assertEquals(0, opcodes.get(0).i1);
            assertEquals(40, opcodes.get(0).i2);
            assertEquals(0, opcodes.get(0).j1);
            assertEquals(40, opcodes.get(0).j2);
            assertEquals(SequenceMatcher.OpcodeTag.DELETE, opcodes.get(1).tag);
            assertEquals(40, opcodes.get(1).i1);
            assertEquals(41, opcodes.get(1).i2);
            assertEquals(40, opcodes.get(1).j1);
            assertEquals(40, opcodes.get(1).j2);
            assertEquals(SequenceMatcher.OpcodeTag.EQUAL, opcodes.get(2).tag);
            assertEquals(41, opcodes.get(2).i1);
            assertEquals(81, opcodes.get(2).i2);
            assertEquals(40, opcodes.get(2).j1);
            assertEquals(80, opcodes.get(2).j2);
        }

        @Test
        public void testBJunk() {
            // Test with no junk
            SequenceMatcher sm = new SequenceMatcher(ch -> ch == ' ', "a".repeat(40) + "b".repeat(40), "a".repeat(44) + "b".repeat(40), true);
            Assertions.assertTrue(sm.getBJunk().isEmpty());

            // Test with junk characters
            sm = new SequenceMatcher(ch -> ch == ' ', "a".repeat(40) + "b".repeat(40), "a".repeat(44) + "b".repeat(40) + " ".repeat(20), true);
            Set<Character> expectedJunk = new HashSet<>();
            expectedJunk.add(' ');
            assertEquals(expectedJunk, sm.getBJunk());

            // Test with multiple junk characters
            sm = new SequenceMatcher(ch -> ch == ' ' || ch == 'b', "a".repeat(40) + "b".repeat(40), "a".repeat(44) + "b".repeat(40) + " ".repeat(20), true);
            expectedJunk = new HashSet<>();
            expectedJunk.add(' ');
            expectedJunk.add('b');
            assertEquals(expectedJunk, sm.getBJunk());
        }
    }

    /**
     * Tests for the autoJunk parameter - ported from TestAutoJunk
     */
    @Nested
    public class TestAutoJunk {

        @Test
        public void testOneInsertHomogenousSequence() {
            // By default, autoJunk = True and the heuristic kicks in for a sequence of length 200+
            String seq1 = "b".repeat(200);
            String seq2 = "a" + "b".repeat(200);

            SequenceMatcher sm = new SequenceMatcher(null, seq1, seq2, true);
            assertEquals(0.0, sm.ratio(), 0.001);

            // Now turn the heuristic off
            sm = new SequenceMatcher(null, seq1, seq2, false);
            assertEquals(0.9975, sm.ratio(), 0.001);
        }
    }

    /**
     * Tests for various bug fixes - ported from TestSFBugs
     */
    @Nested
    class TestSFBugs {

        @Test
        public void testRatioForNullSequence() {
            // Check clearing of SF bug 763023
            SequenceMatcher s = new SequenceMatcher(null, "", "", true);
            assertEquals(1.0, s.ratio());
            assertEquals(1.0, s.quickRatio());
            assertEquals(1.0, s.realQuickRatio());
        }

        @Test
        public void testMatchingBlocksCache() {
            // Issue #21635 - test that matching blocks are cached properly
            SequenceMatcher s = new SequenceMatcher(null, "abxcd", "abcd", true);
            List<SequenceMatcher.Match> first = s.getMatchingBlocks();
            List<SequenceMatcher.Match> second = s.getMatchingBlocks();
            assertEquals(2, second.get(0).size);
            assertEquals(2, second.get(1).size);
            assertEquals(0, second.get(2).size);
        }
    }

    /**
     * Tests for find_longest_match functionality - ported from TestFindLongest
     */
    @Nested
    class TestFindLongest {

        private boolean longerMatchExists(String a, String b, int n) {
            // Check if there's a longer match than n
            for (int i = 0; i <= b.length() - n - 1; i++) {
                String bPart = b.substring(i, i + n + 1);
                if (a.contains(bPart)) {
                    return true;
                }
            }
            return false;
        }

        @Test
        public void testDefaultArgs() {
            String a = "foo bar";
            String b = "foo baz bar";
            SequenceMatcher sm = new SequenceMatcher(null, a, b, true);
            SequenceMatcher.Match match = sm.findLongestMatch(0, a.length(), 0, b.length());
            assertEquals(0, match.aOffset);
            assertEquals(0, match.bOffset);
            assertEquals(6, match.size);
            assertEquals(a.substring(match.aOffset, match.aOffset + match.size), b.substring(match.bOffset, match.bOffset + match.size));
            Assertions.assertFalse(longerMatchExists(a, b, match.size));

            match = sm.findLongestMatch(2, a.length(), 4, b.length());
            assertEquals(3, match.aOffset);
            assertEquals(7, match.bOffset);
            assertEquals(4, match.size);
            assertEquals(a.substring(match.aOffset, match.aOffset + match.size), b.substring(match.bOffset, match.bOffset + match.size));
            Assertions.assertFalse(longerMatchExists(a.substring(2), b.substring(4), match.size));

            match = sm.findLongestMatch(0, a.length(), 1, 5);
            assertEquals(1, match.aOffset);
            assertEquals(1, match.bOffset);
            assertEquals(4, match.size);
            assertEquals(a.substring(match.aOffset, match.aOffset + match.size), b.substring(match.bOffset, match.bOffset + match.size));
            Assertions.assertFalse(longerMatchExists(a, b.substring(1, 5), match.size));
        }

        @Test
        public void testLongestMatchWithPopularChars() {
            String a = "dabcd";
            String b = "d".repeat(100) + "abc" + "d".repeat(100); // length over 200 so popular used
            SequenceMatcher sm = new SequenceMatcher(null, a, b, true);
            SequenceMatcher.Match match = sm.findLongestMatch(0, a.length(), 0, b.length());
            assertEquals(0, match.aOffset);
            assertEquals(99, match.bOffset);
            assertEquals(5, match.size);
            assertEquals(a.substring(match.aOffset, match.aOffset + match.size), b.substring(match.bOffset, match.bOffset + match.size));
            Assertions.assertFalse(longerMatchExists(a, b, match.size));
        }
    }

    /**
     * Test get_close_matches static method
     */
    @Test
    public void testGetCloseMatches() {
        List<String> possibilities = Arrays.asList("apple", "peach", "orange", "grape", "apricot", "pineapple");

        // Test basic functionality - exact match should be included
        List<String> matches = SequenceMatcher.getCloseMatches("apple", possibilities, 3, 0.6);
        Assertions.assertFalse(matches.isEmpty()); // should have at least the exact match
        Assertions.assertTrue(matches.contains("apple")); // exact match should be included

        // Test with lower cutoff to get more matches
        matches = SequenceMatcher.getCloseMatches("apple", possibilities, 6, 0.1);
        Assertions.assertTrue(matches.contains("apple")); // exact match should be included
        Assertions.assertTrue(matches.contains("pineapple")); // partial match should be included with lower cutoff

        // Test with different word
        matches = SequenceMatcher.getCloseMatches("app", possibilities, 3, 0.1);
        Assertions.assertTrue(matches.contains("apple"));

        // Test with n=1
        matches = SequenceMatcher.getCloseMatches("apple", possibilities, 1, 0.6);
        assertEquals(1, matches.size());
        Assertions.assertTrue(matches.contains("apple"));
    }

    /**
     * Test exception handling for getCloseMatches
     */
    @Test
    public void testGetCloseMatchesExceptions() {
        List<String> possibilities = Arrays.asList("apple", "peach");

        // Test n <= 0
        Assertions.assertThrows(IllegalArgumentException.class, () -> SequenceMatcher.getCloseMatches("apple", possibilities, 0, 0.6));

        // Test invalid cutoff
        Assertions.assertThrows(IllegalArgumentException.class, () -> SequenceMatcher.getCloseMatches("apple", possibilities, 3, -0.1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> SequenceMatcher.getCloseMatches("apple", possibilities, 3, 1.1));
    }

    @Test
    public void testRatioCalculationsIdentical() {
        // Test identical strings should have ratio of 1.0
        SequenceMatcher sm = new SequenceMatcher(null, "hello", "hello", true);
        assertEquals(1.0, sm.ratio(), 0.001);

        // Test with different identical strings
        sm = new SequenceMatcher(null, "abc", "abc", true);
        assertEquals(1.0, sm.ratio(), 0.001);

        // Test with empty strings
        sm = new SequenceMatcher(null, "", "", true);
        assertEquals(1.0, sm.ratio(), 0.001);

        // Test with single character
        sm = new SequenceMatcher(null, "a", "a", true);
        assertEquals(1.0, sm.ratio(), 0.001);
    }

    /**
     * Test various ratio calculations
     */
    @Test
    public void testRatioCalculations() {
        // Test that all ratio methods return values between 0 and 1
        SequenceMatcher sm = new SequenceMatcher(null, "abcd", "bcda", true);

        double ratio = sm.ratio();
        double quickRatio = sm.quickRatio();
        double realQuickRatio = sm.realQuickRatio();

        Assertions.assertTrue(ratio >= 0.0 && ratio <= 1.0);
        Assertions.assertTrue(quickRatio >= 0.0 && quickRatio <= 1.0);
        Assertions.assertTrue(realQuickRatio >= 0.0 && realQuickRatio <= 1.0);

        // Quick ratios should be upper bounds on ratio
        Assertions.assertTrue(quickRatio >= ratio);
        Assertions.assertTrue(realQuickRatio >= ratio);
    }

    /**
     * Test sequence setting methods
     */
    @Test
    public void testSequenceSetting() {
        SequenceMatcher sm = new SequenceMatcher(null, "abc", "def", true);

        // Test setSeqs
        sm.setSequences("hello", "world");
        Assertions.assertNotEquals(1.0, sm.ratio()); // Should not be identical

        // Test setSeqA
        sm.setSequenceA("world");
        assertEquals(1.0, sm.ratio()); // Should be identical now

        // Test setSeqB
        sm.setSequenceB("hello");
        Assertions.assertNotEquals(1.0, sm.ratio()); // Should not be identical again
    }
}
