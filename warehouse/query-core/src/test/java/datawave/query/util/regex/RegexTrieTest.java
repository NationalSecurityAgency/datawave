package datawave.query.util.regex;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

public class RegexTrieTest {
    @Test
    public void testTrie() {
        String[] strings = new String[] {"", "A", "AB", "ABCDEF", "BC", "BB"};
        String[] other = new String[] {"AA", "B", "BBB", "BCB", "ABC", "ABCD", "ABCDE", "ABCDEFF", "C"};
        RegexTrie trie = new RegexTrie(Arrays.asList(strings));
        for (String string : strings) {
            Assertions.assertTrue(trie.contains(string));
        }
        for (String string : other) {
            Assertions.assertFalse(trie.contains(string));
        }
    }
    
    @Test
    public void testRegex() {
        String[] strings = new String[] {"", "A", "AB", "ABCDEF", "BC", "BB"};
        String[] other = new String[] {"AA", "B", "BBB", "BCB", "ABC", "ABCD", "ABCDE", "ABCDEFF", "C"};
        String regex = new RegexTrie(Arrays.asList(strings)).toRegex();
        Pattern pattern = Pattern.compile(regex);
        for (String string : strings) {
            Assertions.assertTrue(pattern.matcher(string).matches());
        }
        for (String string : other) {
            Assertions.assertFalse(pattern.matcher(string).matches());
        }
    }
}
