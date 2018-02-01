package datawave.util;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import static org.junit.Assert.*;

/**
 * 
 */
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
        assertEquals("Wrong length ('" + str + "') : expected " + Arrays.asList(expected) + " but got " + Arrays.asList(utils), expected.length, utils.length);
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
                assertFalse("Expected " + c + " to not split as a single character regex", parsedAsSingleChar);
            } else {
                assertTrue("Expected " + c + " to split as a single character regex", parsedAsSingleChar);
            }
            
            if (StringUtils.isEscapableLiteral(c)) {
                assertTrue("Expected " + (int) c + " to split as an escaped character regex", parsedAsEscapedChar);
            } else {
                assertFalse("Expected " + (int) c + " to not split as an escaped character regex", parsedAsEscapedChar);
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
                assertEquals("Failed to split " + value, Arrays.asList(expected), Arrays.asList(splits));
            } else {
                String[] splits = StringUtils.split(value, String.valueOf(c));
                assertEquals("Failed to split " + value, Arrays.asList(expected), Arrays.asList(splits));
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
}
