package datawave.query.parser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * 
 * 
 */
public class JavaRegexAnalyzerTest {
    
    private static final Logger log = Logger.getLogger(JavaRegexAnalyzerTest.class);
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Logger.getRootLogger().setLevel(Level.OFF);
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {}
    
    @Before
    public void setUp() {
        log.setLevel(Level.OFF);
        Logger.getLogger(JavaRegexAnalyzer.class).setLevel(Level.OFF);
    }
    
    @After
    public void tearDown() {}
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    public void enableLogging() {
        log.setLevel(Level.DEBUG);
        Logger.getLogger(JavaRegexAnalyzer.class).setLevel(Level.TRACE);
    }
    
    @Test
    public void testRegexAnalyzer01() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer01");
        String value = "abc.xyz";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abc", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abc", wcd.getLeadingLiteral());
        Assert.assertEquals("xyz", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer02() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer02");
        String value = "abc\\.xyz";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abc.xyz", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abc.xyz", wcd.getLeadingLiteral());
        Assert.assertEquals("abc.xyz", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer03() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer03");
        String value = "abcxy.*z";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abcxy", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abcxy", wcd.getLeadingLiteral());
        Assert.assertEquals("z", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer04() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer04");
        String value = "abc\\.\\*xyz";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abc.*xyz", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abc.*xyz", wcd.getLeadingLiteral());
        Assert.assertEquals("abc.*xyz", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer05() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer05");
        String value = "abcxy.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abcxy", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abcxy", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer06() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer06");
        String value = "abcxyz\\.\\*\\?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abcxyz.*?", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abcxyz.*?", wcd.getLeadingLiteral());
        Assert.assertEquals("abcxyz.*?", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer07() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer07");
        String value = "abc.xyz.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abc", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abc", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer08() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer08");
        String value = "abc.*xyz.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abc", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abc", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer09() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer09");
        String value = "abc\\.\\*xyz.*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("abc.*xyz", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("abc.*xyz", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer10() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer10");
        String value = ".*something\\.com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("something.com", wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertEquals("something.com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer11() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer11");
        String value = "something.com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("something", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("something", wcd.getLeadingLiteral());
        Assert.assertEquals("com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer12() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer12");
        String value = "something\\.com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("something.com", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("something.com", wcd.getLeadingLiteral());
        Assert.assertEquals("something.com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer13() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer13");
        String value = ".*dude.*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertTrue(wcd.isNgram());
        Assert.assertNull(wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer14() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer14");
        String value = ".*dude.*com";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("com", wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertEquals("com", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer15() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer15");
        String value = ".*dude.*com\\.";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("com.", wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertEquals("com.", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer16() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer16");
        String value = "128\\.0\\.1\\.16";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("128.0.1.16", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("128.0.1.16", wcd.getLeadingLiteral());
        Assert.assertEquals("128.0.1.16", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer17() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer17");
        String value = "128\\.0\\.1\\..*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("128.0.1.", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("128.0.1.", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer17_1() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer17");
        String value = "128\\.0\\.1\\..*?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("128.0.1.", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("128.0.1.", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer19() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer19");
        String value = "\\[I=2077c64e4eb655.*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("[I=2077c64e4eb655", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("[I=2077c64e4eb655", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer20() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer20");
        String value = "\\\\\\\\some\\\\\\\\file\\\\\\\\path.*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("\\\\some\\\\file\\\\path", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("\\\\some\\\\file\\\\path", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer21() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer21");
        String value = "bla?";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bl", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer22() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer22");
        String value = "bla*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bl", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer23() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer23");
        String value = "bla+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bla", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bla", wcd.getLeadingLiteral());
        Assert.assertEquals("a", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer24() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer24");
        String value = "bla{2}bla*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bla", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bla", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer25() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer25");
        String value = "bla{0,3}";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bl", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer26() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer26");
        String value = "bla{0}bla+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bl", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bl", wcd.getLeadingLiteral());
        Assert.assertEquals("a", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer27() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer27");
        String value = "(bla)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bla", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("bla", wcd.getLeadingLiteral());
        Assert.assertEquals("bla", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer28() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer28");
        String value = "(bla)*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertTrue(wcd.isNgram());
        Assert.assertNull(wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer29() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer29");
        String value = "((foo)+(bar)+)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foo", wcd.getLeadingLiteral());
        Assert.assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer30() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer30");
        String value = "((foo)+(bar*)+)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foo", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer31() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer31");
        String value = "((bar*)+)+";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("ba", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("ba", wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer32() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer32");
        String value = "((bar*)+)*";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertTrue(wcd.isNgram());
        Assert.assertNull(wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer33() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer33");
        String value = "foo|bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertTrue(wcd.isNgram());
        Assert.assertNull(wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer34() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer34");
        String value = "(foo|bar)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("bar", wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer35() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer35");
        String value = "foo(foo|bar)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foo", wcd.getLeadingLiteral());
        Assert.assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer36() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer36");
        String value = "foo(foo)|(bar)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertTrue(wcd.isLeadingRegex());
        Assert.assertTrue(wcd.isTrailingRegex());
        Assert.assertFalse(wcd.isLeadingLiteral());
        Assert.assertFalse(wcd.isTrailingLiteral());
        Assert.assertTrue(wcd.isNgram());
        Assert.assertNull(wcd.getLeadingOrTrailingLiteral());
        Assert.assertNull(wcd.getLeadingLiteral());
        Assert.assertNull(wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer37() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer37");
        String value = "foo.(?<!\\$)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foo", wcd.getLeadingLiteral());
        Assert.assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer38() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer38");
        String value = "foo(?>\\$)bar";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foobar", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foobar", wcd.getLeadingLiteral());
        Assert.assertEquals("foobar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer39() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer39");
        String value = "(foo(x)?bar)";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foo", wcd.getLeadingLiteral());
        Assert.assertEquals("bar", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer40() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer40");
        String value = "foo{1,}";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        log.debug("wcd.hasWildCard(): " + wcd.hasWildCard());
        log.debug("wcd.getLeadingOrTrailingLiteral(): " + wcd.getLeadingOrTrailingLiteral());
        Assert.assertTrue(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("foo", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("foo", wcd.getLeadingLiteral());
        Assert.assertEquals("o", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer41() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer41");
        String value = "(?-icu)Friendly";
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
        Assert.assertEquals(value, wcd.getRegex());
        Assert.assertEquals(value, wcd.toString());
        log.debug("value: " + value);
        Assert.assertFalse(wcd.hasWildCard());
        Assert.assertFalse(wcd.isLeadingRegex());
        Assert.assertFalse(wcd.isTrailingRegex());
        Assert.assertTrue(wcd.isLeadingLiteral());
        Assert.assertTrue(wcd.isTrailingLiteral());
        Assert.assertFalse(wcd.isNgram());
        Assert.assertEquals("Friendly", wcd.getLeadingOrTrailingLiteral());
        Assert.assertEquals("Friendly", wcd.getLeadingLiteral());
        Assert.assertEquals("Friendly", wcd.getTrailingLiteral());
    }
    
    @Test
    public void testRegexAnalyzer42() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer42");
        String value = "(?#icu)Friendly";
        
        exception.expect(JavaRegexParseException.class);
        JavaRegexAnalyzer wcd = new JavaRegexAnalyzer(value);
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
            Assert.assertEquals(value, wcd.getRegex());
            Assert.assertEquals(value, wcd.toString());
            log.debug("value: " + value);
            Assert.assertFalse(wcd.hasWildCard());
            Assert.assertFalse(wcd.isLeadingRegex());
            Assert.assertFalse(wcd.isTrailingRegex());
            Assert.assertTrue(wcd.isLeadingLiteral());
            Assert.assertTrue(wcd.isTrailingLiteral());
            Assert.assertFalse(wcd.isNgram());
            Assert.assertEquals(values.get(value), wcd.getLeadingOrTrailingLiteral());
            Assert.assertEquals(values.get(value), wcd.getLeadingLiteral());
            Assert.assertEquals(values.get(value), wcd.getTrailingLiteral());
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
            Assert.assertEquals(value, wcd.getRegex());
            Assert.assertEquals(value, wcd.toString());
            log.debug("value: " + value);
            Assert.assertFalse(wcd.hasWildCard());
            Assert.assertFalse(wcd.isLeadingRegex());
            Assert.assertFalse(wcd.isTrailingRegex());
            Assert.assertTrue(wcd.isLeadingLiteral());
            Assert.assertTrue(wcd.isTrailingLiteral());
            Assert.assertFalse(wcd.isNgram());
            Assert.assertEquals(values.get(value), wcd.getLeadingOrTrailingLiteral());
            Assert.assertEquals(values.get(value), wcd.getLeadingLiteral());
            Assert.assertEquals(values.get(value), wcd.getTrailingLiteral());
        }
    }
    
    @Test
    public void testZeroPadIpRegex() throws JavaRegexParseException {
        log.debug("---testRegexAnalyzer18");
        
        Assert.assertEquals("001\\.002\\.003\\.004", new JavaRegexAnalyzer("1\\.2\\.3\\.4").getZeroPadIpRegex());
        Assert.assertEquals("001\\.002\\.003\\.0{0,3}.*", new JavaRegexAnalyzer("1\\.2\\.3\\..*").getZeroPadIpRegex());
        Assert.assertEquals("001\\.002\\.0{0,3}.*", new JavaRegexAnalyzer("1\\.2\\..*").getZeroPadIpRegex());
        Assert.assertEquals("001\\.0{0,3}.*", new JavaRegexAnalyzer("1\\..*").getZeroPadIpRegex());
        
        Assert.assertEquals("001\\.122\\.013\\.004", new JavaRegexAnalyzer("1\\.122\\.13\\.4").getZeroPadIpRegex());
        Assert.assertEquals("001\\.122\\.013\\.0{0,3}.*", new JavaRegexAnalyzer("1\\.122\\.13\\..*").getZeroPadIpRegex());
        Assert.assertEquals("091\\.122\\.0{0,3}.*", new JavaRegexAnalyzer("91\\.122\\..*").getZeroPadIpRegex());
        Assert.assertEquals("012\\.0{0,3}.*", new JavaRegexAnalyzer("12\\..*").getZeroPadIpRegex());
        
        Assert.assertEquals("001\\.122\\.013\\.0{0,3}.*?", new JavaRegexAnalyzer("1\\.122\\.13\\..*?").getZeroPadIpRegex());
        Assert.assertEquals("091\\.122\\.0{0,3}.*+", new JavaRegexAnalyzer("91\\.122\\..*+").getZeroPadIpRegex());
        Assert.assertEquals("012\\.0{0,3}.*?", new JavaRegexAnalyzer("12\\..*?").getZeroPadIpRegex());
        
        try {
            Assert.assertEquals("00a\\.00b\\.00c\\.00d", new JavaRegexAnalyzer("a\\.b\\.c\\.d").getZeroPadIpRegex());
            Assert.fail("Expected letters to be invalid in an IP regex");
        } catch (JavaRegexParseException e) {
            // expected
        }
        
        Assert.assertEquals("001\\.027\\.0{0,3}.*\\.012", new JavaRegexAnalyzer("1\\.27\\..*\\.12").getZeroPadIpRegex());
        
        Assert.assertEquals("078\\.038\\.218\\.0{0,3}.*?", new JavaRegexAnalyzer("78\\.38\\.218\\..*?").getZeroPadIpRegex());
        Assert.assertEquals("078\\.038\\.218\\....", new JavaRegexAnalyzer("78\\.38\\.218\\....").getZeroPadIpRegex());
        Assert.assertEquals("078\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("78\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        
        Assert.assertEquals("0{0,3}8{0,3}\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("8{0,3}\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        Assert.assertEquals("0{1,3}8{0,2}\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("8{0,2}\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        Assert.assertEquals("0{0,2}08{0,2}\\.038\\.218\\.\\d\\d\\d", new JavaRegexAnalyzer("08{0,2}\\.38\\.218\\.\\d\\d\\d").getZeroPadIpRegex());
        
        Assert.assertEquals("0.3\\.02.\\.3.3\\...3", new JavaRegexAnalyzer(".3\\.2.\\.3.3\\...3").getZeroPadIpRegex());
        
        Assert.assertEquals("00\\x34\\.00\\ua425\\.00\\06\\.00\\p{Digit}", new JavaRegexAnalyzer("\\x34\\.\\ua425\\.\\06\\.\\p{Digit}").getZeroPadIpRegex());
        Assert.assertEquals("00\\0127\\.00\\063\\.00\\06\\.00\\P{Alpha}", new JavaRegexAnalyzer("\\0127\\.\\063\\.\\06\\.\\P{Alpha}").getZeroPadIpRegex());
        
        Assert.assertEquals("234\\.234\\.234\\.00[\\p{L}&&[^\\p{Lu}]]", new JavaRegexAnalyzer("234\\.234\\.234\\.[\\p{L}&&[^\\p{Lu}]]").getZeroPadIpRegex());
        
        Assert.assertEquals("0(3|4)2\\.0{0,1}(24|123)\\.0[234]4\\.123", new JavaRegexAnalyzer("(3|4)2\\.(24|123)\\.[234]4\\.123").getZeroPadIpRegex());
        
        Assert.assertEquals("012\\.012\\.012\\.012|023\\.023\\.023\\.023", new JavaRegexAnalyzer("12\\.12\\.12\\.12|23\\.23\\.23\\.23").getZeroPadIpRegex());
        Assert.assertEquals("(012\\.012\\.012\\.012|023\\.023\\.023\\.023)",
                        new JavaRegexAnalyzer("(12\\.12\\.12\\.12|23\\.23\\.23\\.23)").getZeroPadIpRegex());
        Assert.assertEquals("012\\.(012\\.012|023\\.023)\\.012", new JavaRegexAnalyzer("12\\.(12\\.12|23\\.23)\\.12").getZeroPadIpRegex());
        
        Assert.assertEquals("012\\.(012\\.012|023\\.023)\\.0(1|2)2", new JavaRegexAnalyzer("12\\.(12\\.12|23\\.23)\\.(1|2)2").getZeroPadIpRegex());
        
        // These required redistributing parts of tuples across a set of nested alternatives which contain a separator
        // The best way to handle this is to redistribute the external parts within the grouped alternatives.
        // This will break things if back references are included. Decided this is not required and should instead fail normalization
        try {
            // if the assert fails, then we probably attempted to enable this distribution thing or the detection and subsequent throw JavaRegexParseException
            // failed
            Assert.assertEquals("012\\.(012\\.03(3|5)|123\\.23(3|5))\\.012", new JavaRegexAnalyzer("12\\.1(2\\.3|23\\.23)(3|5)\\.12").getZeroPadIpRegex());
        } catch (JavaRegexParseException jrpe) {
            // expected
        }
        try {
            Assert.assertEquals("012\\.(012\\.03(3|5)|123\\.23(3|5))\\.012", new JavaRegexAnalyzer("12\\.1(2(\\.3|3\\.23))(3|5)\\.12").getZeroPadIpRegex());
        } catch (JavaRegexParseException jrpe) {
            // expected
        }
        try {
            Assert.assertEquals("012\\.(012\\.03(3|5)|123\\.23(3|5))\\.012", new JavaRegexAnalyzer("12\\.1(2(\\.|3\\.2)3)(3|5)\\.12").getZeroPadIpRegex());
        } catch (JavaRegexParseException jrpe) {
            // expected
        }
    }
    
    // This will take at least 5 minutes to enumerate, not really something we want to run as unit test
    // @Test
    public void extensiveIpAddressRegexs() throws JavaRegexParseException {
        Stopwatch sw = new Stopwatch();
        sw.start();
        for (int i = 1; i < 256; i++) {
            for (int j = 1; j < 256; j++) {
                for (int k = 1; k < 256; k++) {
                    String expected = String.format("%03d\\.%03d\\.%03d\\.%s", i, j, k, ".*?");
                    String origIp = String.format("%d\\.%d\\.%d\\.%s", i, j, k, ".*?");
                    String paddedIp = new JavaRegexAnalyzer(origIp).getZeroPadIpRegex();
                    Assert.assertEquals(expected, paddedIp);
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
                    Assert.assertEquals(expected, paddedIp);
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
            Assert.assertEquals(testPattern.getKey(), analyzer.getRegex());
            Assert.assertEquals(testPattern.getKey(), analyzer.toString());
            analyzer.applyRegexCaseSensitivity(false);
            Assert.assertEquals(testPattern.getValue(), analyzer.getRegex());
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
            Assert.assertEquals(testPattern.getKey(), analyzer.getRegex());
            Assert.assertEquals(testPattern.getKey(), analyzer.toString());
            analyzer.applyRegexCaseSensitivity(true);
            Assert.assertEquals(testPattern.getValue(), analyzer.getRegex());
        }
    }
    
    @Test
    public void testRegexCaseFailures() throws JavaRegexParseException {
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
                Assert.fail("Expected failure processing " + testPattern);
            } catch (JavaRegexParseException e) {
                // expected
            }
        }
    }
    
}
