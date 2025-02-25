package datawave.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class StringUtilsTest {
    
    @Test
    public void testSplit() {
        String[] strings = new String[] {"a,what,is,this,b", "a,,what,,,is,,,this,,b", ",,a,what,is,this,b,,"};
        String[][] noEmpties = new String[][] {new String[] {"a", "what", "is", "this", "b"}, new String[] {"a", "what", "is", "this", "b"},
                new String[] {"a", "what", "is", "this", "b"}};
        String[][] withEmpties = new String[][] {new String[] {"a", "what", "is", "this", "b"},
                new String[] {"a", "", "what", "", "", "is", "", "", "this", "", "b"}, new String[] {"", "", "a", "what", "is", "this", "b", "", ""}};
        for (int i = 0; i < strings.length; i++) {
            verify(strings[i], noEmpties[i], StringUtils.split(strings[i], ',', false));
            verify(strings[i], withEmpties[i], StringUtils.split(strings[i], ',', true));
            verify(strings[i], strings[i].split(","), StringUtils.split(strings[i], ','));
        }
    }
    
    private void verify(String str, String[] expected, String[] utils) {
        assertEquals(expected.length, utils.length, "Wrong length ('" + str + "') : expected " + Arrays.asList(expected) + " but got " + Arrays.asList(utils));
        for (int j = 0; j < expected.length; j++) {
            assertEquals(expected[j], utils[j]);
        }
    }
    
    @Test
    public void testReservedChars() {
        String[] expectedA = StringUtils.split(getExpression('A', ','), ',');
        String[] expectedB = StringUtils.split(getExpression('B', ','), ',');
        
        for (int i = 0; i < 256; i++) {
            char c = (char) i;
            String value = getExpression((c == 'A' ? 'B' : 'A'), c);
            String[] expected = (c == 'A' ? expectedB : expectedA);
            
            boolean parsedAsSingleChar = false;
            boolean parsedAsEscapedChar = false;
            
            try {
                String[] splits = value.split(String.valueOf(c));
                if (!Arrays.asList(splits).equals(Arrays.asList(expected))) {
                    throw new Exception("wrong number of splits");
                }
                parsedAsSingleChar = true;
            } catch (Exception e) {
                parsedAsSingleChar = false;
            }
            
            try {
                String[] splits = value.split("\\" + String.valueOf(c));
                if (!Arrays.asList(splits).equals(Arrays.asList(expected))) {
                    throw new Exception("wrong number of splits");
                }
                parsedAsEscapedChar = true;
            } catch (Exception e2) {
                parsedAsEscapedChar = false;
            }
            
            if (StringUtils.isEscapeRequired(c)) {
                assertFalse(parsedAsSingleChar, "Expected " + c + " to not split as a single character regex");
            } else {
                assertTrue(parsedAsSingleChar, "Expected " + c + " to split as a single character regex");
            }
            
            if (StringUtils.isEscapableLiteral(c)) {
                assertTrue(parsedAsEscapedChar, "Expected " + (int) c + " to split as an escaped character regex");
            } else {
                assertFalse(parsedAsEscapedChar, "Expected " + (int) c + " to not split as an escaped character regex");
            }
        }
    }
    
    @Test
    public void testSplitRegex() {
        String[] expectedA = StringUtils.split(getExpression('A', ','), ',');
        String[] expectedB = StringUtils.split(getExpression('B', ','), ',');
        
        for (int i = 0; i < 256; i++) {
            char c = (char) i;
            String value = getExpression((c == 'A' ? 'B' : 'A'), c);
            String[] expected = (c == 'A' ? expectedB : expectedA);
            
            if (StringUtils.isEscapeRequired(c)) {
                String[] splits = StringUtils.split(value, "\\" + String.valueOf(c));
                assertEquals(Arrays.asList(expected), Arrays.asList(splits), "Failed to split " + value);
            } else {
                String[] splits = StringUtils.split(value, String.valueOf(c));
                assertEquals(Arrays.asList(expected), Arrays.asList(splits), "Failed to split " + value);
            }
        }
    }
    
    private String getExpression(char x, char s) {
        StringBuilder value = new StringBuilder();
        for (int j = 0; j < 10; j++) {
            if ((j % 2) == 0) {
                value.append(x);
            } else {
                value.append(s);
            }
        }
        return value.toString();
    }
    
    @Test
    public void testTrimAndRemove() {
        evaluateTrimAndRemove(new String[0], new String[0]);
        evaluateTrimAndRemove(new String[] {"a", "b", "c", "asdfasdf"}, new String[] {"a", "b", "c", "asdfasdf"});
        evaluateTrimAndRemove(new String[] {"a ", " b", "\tc\n", " asdfasdf "}, new String[] {"a", "b", "c", "asdfasdf"});
        evaluateTrimAndRemove(new String[] {"a ", " b", "\tc\n", " asdfasdf "}, new String[] {"a", "b", "c", "asdfasdf"});
        evaluateTrimAndRemove(new String[] {"", " b", "", " asdfasdf "}, new String[] {"b", "asdfasdf"});
        evaluateTrimAndRemove(new String[] {"   ", "  \n\t\n\r   ", "", ""}, new String[0]);
    }
    
    private void evaluateTrimAndRemove(String[] test, String[] expected) {
        String[] value = StringUtils.trimAndRemoveEmptyStrings(test);
        assertEquals(Arrays.asList(expected), Arrays.asList(value));
    }
    
    @Test
    public void testSubSplit() {
        String[] strings = new String[] {"a,what,is,this,b", "a,,what,,,is,,,this,,b", ",,a,what,is,this,b,,"};
        int[][] indexesToReturn = new int[][] {new int[] {0, 1, 4}, new int[] {0, 2, 4, 8}, new int[] {2, 6}};
        String[][] noEmpties = new String[][] {new String[] {"a", "what", "b"}, new String[] {"a", "is", "b", null}, new String[] {"is", null}};
        String[][] withEmpties = new String[][] {new String[] {"a", "what", "b"}, new String[] {"a", "what", "", "this"}, new String[] {"a", "b"}};
        for (int i = 0; i < strings.length; i++) {
            verify(strings[i], noEmpties[i], StringUtils.split(strings[i], ',', false, indexesToReturn[i]));
            verify(strings[i], withEmpties[i], StringUtils.split(strings[i], ',', true, indexesToReturn[i]));
            verify(strings[i], withEmpties[i], StringUtils.split(strings[i], ',', indexesToReturn[i]));
        }
    }
    
    @Test
    public void testCompareWithGuavaSplitter() {
        String[] strings = new String[] {"a,what,is,this,b", "a,,what,,,is,,,this,,b", ",,a,what,is,this,b,,"};
        String[][] noEmpties = new String[][] {new String[] {"a", "what", "is", "this", "b"}, new String[] {"a", "what", "is", "this", "b"},
                new String[] {"a", "what", "is", "this", "b"}};
        String[][] withEmpties = new String[][] {new String[] {"a", "what", "is", "this", "b"},
                new String[] {"a", "", "what", "", "", "is", "", "", "this", "", "b"}, new String[] {"", "", "a", "what", "is", "this", "b", "", ""}};
        for (int i = 0; i < strings.length; i++) {
            verify(strings[i], noEmpties[i], StringUtils.split(strings[i], ',', false));
            verify(strings[i], noEmpties[i], Iterables.toArray(Splitter.on(',').omitEmptyStrings().split(strings[i]), String.class));
            verify(strings[i], withEmpties[i], StringUtils.split(strings[i], ',', true));
            verify(strings[i], withEmpties[i], Iterables.toArray(Splitter.on(',').split(strings[i]), String.class));
        }
    }
    
    @Test
    public void testDeDupStringArray() {
        String[] strings = new String[] {"string 1", "string 2", "string 3", "string 2", "string 1"};
        String[] stringsNoDups = new String[] {"string 1", "string 2", "string 3"};
        
        // check deDup functionality
        strings = StringUtils.deDupStringArray(strings);
        Set<String> stringsSet = new HashSet<>(Arrays.asList(strings));
        Set<String> stringsNoSupSet = new HashSet<>(Arrays.asList(stringsNoDups));
        assertEquals(stringsSet, stringsNoSupSet,
                        "String array was not deduped. Expected: " + Arrays.asList(stringsNoDups) + " But have: " + Arrays.asList(strings) + ".");
        
        // Check null array
        strings = null;
        assertNull(StringUtils.deDupStringArray(strings));
        
        // Check empty array
        strings = new String[] {};
        assertEquals(0, StringUtils.deDupStringArray(strings).length);
        
        // Check array with empty strings
        strings = new String[] {"", "string 1", ""};
        String[] deDupedStrings = StringUtils.deDupStringArray(strings);
        assertEquals(strings.length - 1, deDupedStrings.length, "String array with empty string was not deduped. Expected: " + Arrays.asList(deDupedStrings)
                        + " But have: " + Arrays.asList(strings) + ".");
        
        // Check array with string that only have case differences
        String[] stringsWithCaseDifferences = new String[] {"string 1", "string 2", "String 2"};
        deDupedStrings = StringUtils.deDupStringArray(stringsWithCaseDifferences);
        assertEquals(stringsWithCaseDifferences.length, deDupedStrings.length,
                        "String array with strings that only have case differences should not have been deduped. Expected: "
                                        + Arrays.asList(stringsWithCaseDifferences) + " But have: " + Arrays.asList(deDupedStrings) + ".");
    }
    
    @Test
    public void testSubstringAfterLast() {
        String test1 = "/something/something/something/darkside";
        String test2 = "something.something.something.complete";
        
        assertEquals("darkside", StringUtils.substringAfterLast(test1, "/"));
        assertEquals("complete", StringUtils.substringAfterLast(test2, "."));
        
    }
}
