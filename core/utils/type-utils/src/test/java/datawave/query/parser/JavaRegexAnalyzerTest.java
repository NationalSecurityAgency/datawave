package datawave.query.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;

/**
 * 
 * 
 */
public class JavaRegexAnalyzerTest {
    
    private static final Logger log = Logger.getLogger(JavaRegexAnalyzerTest.class);
    
    @BeforeAll
    public static void setUpClass() {
        Logger.getRootLogger().setLevel(Level.OFF);
    }
    
    @AfterAll
    public static void tearDownClass() {}
    
    @BeforeEach
    public void setUp() {
        log.setLevel(Level.OFF);
        Logger.getLogger(JavaRegexAnalyzer.class).setLevel(Level.OFF);
    }
    
    @AfterEach
    public void tearDown() {}
    
    public void enableLogging() {
        log.setLevel(Level.DEBUG);
        Logger.getLogger(JavaRegexAnalyzer.class).setLevel(Level.TRACE);
    }
    
    @Test
    public void testRegexAnalyzer01() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer01");
        String value = "abc.xyz";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abc", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abc", wcd.getLeadingLiteral());
        assertEquals("xyz", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer02() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer02");
        String value = "abc\\.xyz";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abc.xyz", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abc.xyz", wcd.getLeadingLiteral());
        assertEquals("abc.xyz", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer03() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer03");
        String value = "abcxy.*z";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abcxy", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abcxy", wcd.getLeadingLiteral());
        assertEquals("z", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer04() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer04");
        String value = "abc\\.\\*xyz";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abc.*xyz", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abc.*xyz", wcd.getLeadingLiteral());
        assertEquals("abc.*xyz", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer05() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer05");
        String value = "abcxy.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abcxy", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abcxy", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer06() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer06");
        String value = "abcxyz\\.\\*\\?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abcxyz.*?", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abcxyz.*?", wcd.getLeadingLiteral());
        assertEquals("abcxyz.*?", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer07() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer07");
        String value = "abc.xyz.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abc", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abc", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer08() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer08");
        String value = "abc.*xyz.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abc", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abc", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer09() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer09");
        String value = "abc\\.\\*xyz.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("abc.*xyz", wcd.getLeadingOrTrailingLiteral());
        assertEquals("abc.*xyz", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer10() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer10");
        String value = ".*something\\.com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("something.com", wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertEquals("something.com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer11() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer11");
        String value = "something.com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("something", wcd.getLeadingOrTrailingLiteral());
        assertEquals("something", wcd.getLeadingLiteral());
        assertEquals("com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer12() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer12");
        String value = "something\\.com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("something.com", wcd.getLeadingOrTrailingLiteral());
        assertEquals("something.com", wcd.getLeadingLiteral());
        assertEquals("something.com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer13() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer13");
        String value = ".*dude.*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertTrue(wcd.isNgram());
        assertNull(wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer14() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer14");
        String value = ".*dude.*com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("com", wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertEquals("com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer15() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer15");
        String value = ".*dude.*com\\.";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("com.", wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertEquals("com.", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer16() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer16");
        String value = "128\\.0\\.1\\.16";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("128.0.1.16", wcd.getLeadingOrTrailingLiteral());
        assertEquals("128.0.1.16", wcd.getLeadingLiteral());
        assertEquals("128.0.1.16", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer17() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer17");
        String value = "128\\.0\\.1\\..*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("128.0.1.", wcd.getLeadingOrTrailingLiteral());
        assertEquals("128.0.1.", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer17_1() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer17");
        String value = "128\\.0\\.1\\..*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("128.0.1.", wcd.getLeadingOrTrailingLiteral());
        assertEquals("128.0.1.", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer19() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer19");
        String value = "\\[I=2077c64e4eb655.*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("[I=2077c64e4eb655", wcd.getLeadingOrTrailingLiteral());
        assertEquals("[I=2077c64e4eb655", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer20() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer20");
        String value = "\\\\\\\\some\\\\\\\\file\\\\\\\\path.*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("\\\\some\\\\file\\\\path", wcd.getLeadingOrTrailingLiteral());
        assertEquals("\\\\some\\\\file\\\\path", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer21() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer21");
        String value = "bla?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bl", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer22() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer22");
        String value = "bla*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bl", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer23() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer23");
        String value = "bla+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bla", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bla", wcd.getLeadingLiteral());
        assertEquals("a", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer24() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer24");
        String value = "bla{2}bla*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bla", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bla", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer25() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer25");
        String value = "bla{0,3}";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bl", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer26() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer26");
        String value = "bla{0}bla+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bl", wcd.getLeadingLiteral());
        assertEquals("a", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer27() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer27");
        String value = "(bla)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bla", wcd.getLeadingOrTrailingLiteral());
        assertEquals("bla", wcd.getLeadingLiteral());
        assertEquals("bla", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer28() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer28");
        String value = "(bla)*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertTrue(wcd.isNgram());
        assertNull(wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer29() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer29");
        String value = "((foo)+(bar)+)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foo", wcd.getLeadingLiteral());
        assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer30() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer30");
        String value = "((foo)+(bar*)+)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foo", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer31() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer31");
        String value = "((bar*)+)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("ba", wcd.getLeadingOrTrailingLiteral());
        assertEquals("ba", wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer32() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer32");
        String value = "((bar*)+)*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertTrue(wcd.isNgram());
        assertNull(wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer33() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer33");
        String value = "foo|bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertTrue(wcd.isNgram());
        assertNull(wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer34() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer34");
        String value = "(foo|bar)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("bar", wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer35() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer35");
        String value = "foo(foo|bar)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foo", wcd.getLeadingLiteral());
        assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer36() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer36");
        String value = "foo(foo)|(bar)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertTrue(wcd.isLeadingRegex());
        assertTrue(wcd.isTrailingRegex());
        assertFalse(wcd.isLeadingLiteral());
        assertFalse(wcd.isTrailingLiteral());
        assertTrue(wcd.isNgram());
        assertNull(wcd.getLeadingOrTrailingLiteral());
        assertNull(wcd.getLeadingLiteral());
        assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer37() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer37");
        String value = "foo.(?<!\\$)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foo", wcd.getLeadingLiteral());
        assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer38() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer38");
        String value = "foo(?>\\$)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foobar", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foobar", wcd.getLeadingLiteral());
        assertEquals("foobar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer39() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer39");
        String value = "(foo(x)?bar)";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foo", wcd.getLeadingLiteral());
        assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer40() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer40");
        String value = "foo{1,}";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        assertTrue(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        assertEquals("foo", wcd.getLeadingLiteral());
        assertEquals("o", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer41() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer41");
        String value = "(?-icu)Friendly";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        assertEquals(value, wcd.getRegex());
        assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        assertFalse(wcd.hasWildCard());
        assertFalse(wcd.isLeadingRegex());
        assertFalse(wcd.isTrailingRegex());
        assertTrue(wcd.isLeadingLiteral());
        assertTrue(wcd.isTrailingLiteral());
        assertFalse(wcd.isNgram());
        assertEquals("Friendly", wcd.getLeadingOrTrailingLiteral());
        assertEquals("Friendly", wcd.getLeadingLiteral());
        assertEquals("Friendly", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer42() {
        log.debug("---testRegexAnalyzer42");
        String value = "(?#icu)Friendly";
        
        assertThrows(JavaRegexParseException.class, () -> new JavaRegexAnalyzer(value));
    }
    
    @Test
    public void testRegexAnalyzerQuoting() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzerQuoting");
        Map<String,String> values = new HashMap<>();
        values.put("\\Q+ae4\\E", "+ae4");
        values.put("abc\\Q+ae4\\E", "abc+ae4");
        values.put("\\Q+ae4\\Edef", "+ae4def");
        values.put("abc\\Q+ae4\\Edef", "abc+ae4def");
        for (String value : values.keySet()) {
            JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
            assertEquals(value, wcd.getRegex());
            assertEquals(value, wcd.toString());
            log.debug("value: " + value);
            assertFalse(wcd.hasWildCard());
            assertFalse(wcd.isLeadingRegex());
            assertFalse(wcd.isTrailingRegex());
            assertTrue(wcd.isLeadingLiteral());
            assertTrue(wcd.isTrailingLiteral());
            assertFalse(wcd.isNgram());
            assertEquals(values.get(value), wcd.getLeadingOrTrailingLiteral());
            assertEquals(values.get(value), wcd.getLeadingLiteral());
            assertEquals(values.get(value), wcd.getTrailingLiteral());
        }
    }
    
    @Test
    public void testRegexAnalyzerBoundary() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzerQuoting");
        Map<String,String> values = new HashMap<>();
        values.put("\\Bae4\\b", "ae4");
        values.put("\\Zae4\\z", "ae4");
        values.put("abc\\Gae4", "abcae4");
        values.put("\\Bae4\\Zdef", "ae4def");
        values.put("abc\\Aae4\\Gdef", "abcae4def");
        values.put("^abc\\Aae4\\Gdef$", "abcae4def");
        for (String value : values.keySet()) {
            JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
            assertEquals(value, wcd.getRegex());
            assertEquals(value, wcd.toString());
            log.debug("value: " + value);
            assertFalse(wcd.hasWildCard());
            assertFalse(wcd.isLeadingRegex());
            assertFalse(wcd.isTrailingRegex());
            assertTrue(wcd.isLeadingLiteral());
            assertTrue(wcd.isTrailingLiteral());
            assertFalse(wcd.isNgram());
            assertEquals(values.get(value), wcd.getLeadingOrTrailingLiteral());
            assertEquals(values.get(value), wcd.getLeadingLiteral());
            assertEquals(values.get(value), wcd.getTrailingLiteral());
        }
    }
    
    @Test
    public void testZeroPadIpRegex() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer18");
        
        assertEquals("001\\.002\\.003\\.004", new JavaRegexAnalyzer("1\\.2\\.3\\.4").getZeroPadIpRegex());
        assertEquals("001\\.002\\.003\\.0{0,3}.*", new JavaRegexAnalyzer("1\\.2\\.3\\..*").getZeroPadIpRegex());
        assertEquals("001\\.002\\.0{0,3}.*", new JavaRegexAnalyzer("1\\.2\\..*").getZeroPadIpRegex());
        assertEquals("001\\.0{0,3}.*", new JavaRegexAnalyzer("1\\..*").getZeroPadIpRegex());
        
        assertEquals("001\\.122\\.013\\.004", new JavaRegexAnalyzer("1\\.122\\.13\\.4").getZeroPadIpRegex());
        assertEquals("001\\.122\\.013\\.0{0,3}.*", new JavaRegexAnalyzer("1\\.122\\.13\\..*").getZeroPadIpRegex());
        assertEquals("091\\.122\\.0{0,3}.*", new JavaRegexAnalyzer("91\\.122\\..*").getZeroPadIpRegex());
        assertEquals("012\\.0{0,3}.*", new JavaRegexAnalyzer("12\\..*").getZeroPadIpRegex());
        
        assertEquals("001\\.122\\.013\\.0{0,3}.*?", new JavaRegexAnalyzer("1\\.122\\.13\\..*?").getZeroPadIpRegex());
        assertEquals("091\\.122\\.0{0,3}.*+", new JavaRegexAnalyzer("91\\.122\\..*+").getZeroPadIpRegex());
        assertEquals("012\\.0{0,3}.*?", new JavaRegexAnalyzer("12\\..*?").getZeroPadIpRegex());
        
        try {
            assertEquals("00a\\.00b\\.00c\\.00d", new JavaRegexAnalyzer("a\\.b\\.c\\.d").getZeroPadIpRegex());
            fail("Expected letters to be invalid in an IP regex");
        } catch (JavaRegexParseException e) {
            // expected
        }
        
        assertEquals("001\\.027\\.0{0,3}.*\\.012", new JavaRegexAnalyzer("1\\.27\\..*\\.12").getZeroPadIpRegex());
        
        assertEquals("078\\.038\\.218\\.0{0,3}.*?", new JavaRegexAnalyzer("78\\.38\\.218\\..*?").getZeroPadIpRegex());
        assertEquals("078\\.038\\.218\\....", new JavaRegexAnalyzer("78\\.38\\.218\\....").getZeroPadIpRegex());
        assertEquals("078\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("78\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        
        assertEquals("0{0,3}8{0,3}\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("8{0,3}\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        assertEquals("0{1,3}8{0,2}\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("8{0,2}\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        assertEquals("0{0,2}08{0,2}\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("08{0,2}\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        
        assertEquals("0.3\\.02.\\.3.3\\...3", new JavaRegexAnalyzer(".3\\.2.\\.3.3\\...3").getZeroPadIpRegex());
        
        assertEquals("00\\x34\\.00\\ua425\\.00\\06\\.00\\p{Digit}", new JavaRegexAnalyzer("\\x34\\.\\ua425\\.\\06\\.\\p{Digit}").getZeroPadIpRegex());
        assertEquals("00\\0127\\.00\\063\\.00\\06\\.00\\P{Alpha}", new JavaRegexAnalyzer("\\0127\\.\\063\\.\\06\\.\\P{Alpha}").getZeroPadIpRegex());
        
        assertEquals("234\\.234\\.234\\.00[\\p{L}&&[^\\p{Lu}]]", new JavaRegexAnalyzer("234\\.234\\.234\\.[\\p{L}&&[^\\p{Lu}]]").getZeroPadIpRegex());
        
        assertEquals("0(3|4)2\\.0{0,1}(24|123)\\.0[234]4\\.123", new JavaRegexAnalyzer("(3|4)2\\.(24|123)\\.[234]4\\.123").getZeroPadIpRegex());
        
        assertEquals("012\\.012\\.012\\.012|023\\.023\\.023\\.023", new JavaRegexAnalyzer("12\\.12\\.12\\.12|23\\.23\\.23\\.23").getZeroPadIpRegex());
        assertEquals("(012\\.012\\.012\\.012|023\\.023\\.023\\.023)", new JavaRegexAnalyzer("(12\\.12\\.12\\.12|23\\.23\\.23\\.23)").getZeroPadIpRegex());
        assertEquals("012\\.(012\\.012|023\\.023)\\.012", new JavaRegexAnalyzer("12\\.(12\\.12|23\\.23)\\.12").getZeroPadIpRegex());
        
        assertEquals("012\\.(012\\.012|023\\.023)\\.0(1|2)2", new JavaRegexAnalyzer("12\\.(12\\.12|23\\.23)\\.(1|2)2").getZeroPadIpRegex());
        
        // These required redistributing parts of tuples across a set of nested alternatives which contain a separator
        // The best way to handle this is to redistribute the external parts within the grouped alternatives.
        // This will break things if back references are included. Decided this is not required and should instead fail normalization
        try {
            // if the assert fails, then we probably attempted to enable this distribution thing or the detection and subsequent throw JavaRegexParseException
            // failed
            assertEquals("012\\.(012\\.03(3|5)|123\\.23(3|5))\\.012", new JavaRegexAnalyzer("12\\.1(2\\.3|23\\.23)(3|5)\\.12").getZeroPadIpRegex());
        } catch (JavaRegexParseException jrpe) {
            // expected
        }
        try {
            assertEquals("012\\.(012\\.03(3|5)|123\\.23(3|5))\\.012", new JavaRegexAnalyzer("12\\.1(2(\\.3|3\\.23))(3|5)\\.12").getZeroPadIpRegex());
        } catch (JavaRegexParseException jrpe) {
            // expected
        }
        try {
            assertEquals("012\\.(012\\.03(3|5)|123\\.23(3|5))\\.012", new JavaRegexAnalyzer("12\\.1(2(\\.|3\\.2)3)(3|5)\\.12").getZeroPadIpRegex());
        } catch (JavaRegexParseException jrpe) {
            // expected
        }
    }
    
    // This will take at least 5 minutes to enumerate, not really something we want to run as unit test
    @Test
    @Disabled
    public void extensiveIpAddressRegexs() throws JavaRegexParseException {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        for (int i = 1; i < 256; i++) {
            for (int j = 1; j < 256; j++) {
                for (int k = 1; k < 256; k++) {
                    String expected = String.format("%03d\\.%03d\\.%03d\\.%s", i, j, k, ".*?");
                    String origIp = String.format("%d\\.%d\\.%d\\.%s", i, j, k, ".*?");
                    String paddedIp = new JavaRegexAnalyzer(origIp).getZeroPadIpRegex();
                    assertEquals(expected, paddedIp);
                }
            }
        }
        sw.stop();
    }
    
    @Test
    public void testDigitRegexs() throws JavaRegexParseException {
        // Try to generate a list of potentially edge-case octet values (from the digit regex in the JavaRegexAnalyzer#zeroPadIpRegex(String) method
        List<Integer> octetsToEnumerate = Lists.newArrayList(1, 10, 20, 70, 100, 101, 120, 170, 200, 201, 220, 255);
        
        // Then enumerate all combinations of them to make sure they all generate zero-padded 1 through 3 octets with the trailing wildcard
        for (Integer i : octetsToEnumerate) {
            for (Integer j : octetsToEnumerate) {
                for (Integer k : octetsToEnumerate) {
                    String expected = String.format("%03d\\.%03d\\.%03d\\.%s", i, j, k, "0{0,3}.*?");
                    String origIp = String.format("%d\\.%d\\.%d\\.%s", i, j, k, ".*?");
                    String paddedIp = new JavaRegexAnalyzer(origIp).getZeroPadIpRegex();
                    assertEquals(expected, paddedIp);
                }
            }
        }
    }
    
    @Test
    public void testRegexLowerCase() throws JavaRegexParseException {
        Map<String,String> testPatterns = new HashMap<>();
        testPatterns.put("No Wildcards", "no wildcards");
        testPatterns.put(".*No.*Escapes.*", ".*no.*escapes.*");
        testPatterns.put("\\\\Test \\\\Escapes\\\\\\D\\\\", "\\\\test \\\\escapes\\\\\\D\\\\");
        testPatterns.put("Test\\t\\nCharacter\\r\\f\\a\\e\\d\\D\\s\\S\\w\\W\\p{Print}\\p{XDigit}Classes",
                        "test\\t\\ncharacter\\r\\f\\a\\e\\d\\D\\s\\S\\w\\W\\p{Print}\\p{XDigit}classes");
        testPatterns.put("\\uFFFFTest\\x00\\x01\\05\\024\\0267Octal and Hex Character\\uFFFF\\uFE01\\xFE\\x0EClasses",
                        "\\uFFFFtest\\x00\\x01\\05\\024\\0267octal and hex character\\uFFFF\\uFE01\\xFE\\x0Eclasses");
        testPatterns.put("\\uFFFFTest\\Q\\uFFFF\\\\Quoted\\E\\uFFFFRegex", "\\uFFFFtest\\Q\\uffff\\\\quoted\\E\\uFFFFregex");
        testPatterns.put(
                        "\\p{Lower}\\p{Upper}[^\\p{Lower}][^\\p{Upper}]Test Upper And Lower Character Classes\\p{javaLowerCase}\\p{javaUpperCase}[^\\p{javaLowerCase}][^\\p{javaUpperCase}]",
                        "\\p{Lower}\\p{Lower}[\\p{Lower}][^\\p{Upper}]test upper and lower character classes\\p{javaLowerCase}\\p{javaLowerCase}[\\p{javaLowerCase}][^\\p{javaUpperCase}]");
        testPatterns.put(
                        "\\P{Lower}\\P{Upper}[^\\P{Lower}][^\\P{Upper}]Test Upper And Lower Negated Character Classes\\P{javaLowerCase}\\P{javaUpperCase}[^\\P{javaLowerCase}][^\\P{javaUpperCase}]",
                        "\\p{Lower}\\P{Upper}[^\\P{Lower}][\\P{Upper}]test upper and lower negated character classes\\p{javaLowerCase}\\P{javaUpperCase}[^\\P{javaLowerCase}][\\P{javaUpperCase}]");
        testPatterns.put("\\p{Lu}[^\\p{Lu}]Test Upper And Lower Character Classes[\\p{L}&&[^\\p{Lu}]]",
                        "\\p{L}[^\\p{Lu}]test upper and lower character classes[\\p{L}&&[^\\p{Lu}]]");
        testPatterns.put("\\P{Lu}[^\\P{Lu}]Test Upper And Lower Character Classes[\\p{L}&&[^\\P{Lu}]]",
                        "\\P{Lu}[\\P{Lu}]test upper and lower character classes[\\p{L}&&[\\P{Lu}]]");
        
        for (Map.Entry<String,String> testPattern : testPatterns.entrySet()) {
            JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(testPattern.getKey());
            assertEquals(testPattern.getKey(), analyzer.getRegex());
            assertEquals(testPattern.getKey(), analyzer.toString());
            analyzer.applyRegexCaseSensitivity(false);
            assertEquals(testPattern.getValue(), analyzer.getRegex());
        }
    }
    
    @Test
    public void testRegexUpperCase() throws JavaRegexParseException {
        Map<String,String> testPatterns = new HashMap<>();
        testPatterns.put("No Wildcards", "NO WILDCARDS");
        testPatterns.put(".*No.*Escapes.*", ".*NO.*ESCAPES.*");
        testPatterns.put("\\\\Test \\\\Escapes\\\\\\D\\\\", "\\\\TEST \\\\ESCAPES\\\\\\D\\\\");
        testPatterns.put("Test\\t\\nCharacter\\r\\f\\a\\e\\d\\D\\s\\S\\w\\W\\p{Print}\\p{XDigit}Classes",
                        "TEST\\t\\nCHARACTER\\r\\f\\a\\e\\d\\D\\s\\S\\w\\W\\p{Print}\\p{XDigit}CLASSES");
        testPatterns.put("\\uFFFFTest\\x00\\x01\\05\\024\\0267Octal and Hex Character\\uFFFF\\uFE01\\xFE\\x0EClasses",
                        "\\uFFFFTEST\\x00\\x01\\05\\024\\0267OCTAL AND HEX CHARACTER\\uFFFF\\uFE01\\xFE\\x0ECLASSES");
        testPatterns.put("\\uFFFFTest\\Q\\uFFFF\\\\Quoted\\E\\uFFFFRegex", "\\uFFFFTEST\\Q\\UFFFF\\\\QUOTED\\E\\uFFFFREGEX");
        testPatterns.put(
                        "\\p{Lower}\\p{Upper}[^\\p{Lower}][^\\p{Upper}]Test Upper And Lower Character Classes\\p{javaLowerCase}\\p{javaUpperCase}[^\\p{javaLowerCase}][^\\p{javaUpperCase}]",
                        "\\p{Upper}\\p{Upper}[^\\p{Lower}][\\p{Upper}]TEST UPPER AND LOWER CHARACTER CLASSES\\p{javaUpperCase}\\p{javaUpperCase}[^\\p{javaLowerCase}][\\p{javaUpperCase}]");
        testPatterns.put(
                        "\\P{Lower}\\P{Upper}[^\\P{Lower}][^\\P{Upper}]Test Upper And Lower Negated Character Classes\\P{javaLowerCase}\\P{javaUpperCase}[^\\P{javaLowerCase}][^\\P{javaUpperCase}]",
                        "\\P{Lower}\\p{Upper}[\\P{Lower}][^\\P{Upper}]TEST UPPER AND LOWER NEGATED CHARACTER CLASSES\\P{javaLowerCase}\\p{javaUpperCase}[\\P{javaLowerCase}][^\\P{javaUpperCase}]");
        testPatterns.put("\\p{Lu}[^\\p{Lu}]Test Upper And Lower Character Classes[\\p{L}&&[^\\p{Lu}]]",
                        "\\p{Lu}[\\p{Lu}]TEST UPPER AND LOWER CHARACTER CLASSES[\\p{L}&&[\\p{Lu}]]");
        testPatterns.put("\\P{Lu}[^\\P{Lu}]Test Upper And Lower Character Classes[\\p{L}&&[^\\P{Lu}]]",
                        "\\p{L}[^\\P{Lu}]TEST UPPER AND LOWER CHARACTER CLASSES[\\p{L}&&[^\\P{Lu}]]");
        
        for (Map.Entry<String,String> testPattern : testPatterns.entrySet()) {
            JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(testPattern.getKey());
            assertEquals(testPattern.getKey(), analyzer.getRegex());
            assertEquals(testPattern.getKey(), analyzer.toString());
            analyzer.applyRegexCaseSensitivity(true);
            assertEquals(testPattern.getValue(), analyzer.getRegex());
        }
    }
    
    @Test
    public void testRegexCaseFailures() {
        Set<String> testPatterns = new HashSet<>();
        testPatterns.add("Test\\p{Missing Curly Bracket");
        testPatterns.add("Test\\u\\wMissing Hex Digit");
        testPatterns.add("Test\\uF\\wMissing Hex Digit");
        testPatterns.add("Test\\uFF\\wMissing Hex Digit");
        testPatterns.add("Test\\uFFF\\wMissing Hex Digit");
        testPatterns.add("Test\\uFFFG\\wMissing Hex Digit");
        testPatterns.add("Test\\uGFFF\\wMissing Hex Digit");
        testPatterns.add("Test\\x\\wMissing Hex Digit");
        testPatterns.add("Test\\xF\\wMissing Hex Digit");
        testPatterns.add("Test\\xFG\\wMissing Hex Digit");
        testPatterns.add("Test\\xGF\\wMissing Hex Digit");
        testPatterns.add("Test\\c\\Missing Control Char");
        
        for (String testPattern : testPatterns) {
            try {
                new JavaRegexAnalyzer(testPattern);
                fail("Expected failure processing " + testPattern);
            } catch (JavaRegexParseException e) {
                // expected
            }
        }
    }
    
}
