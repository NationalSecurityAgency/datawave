package datawave.query.language.parser.jexl;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.language.parser.ParseException;
import datawave.query.language.processor.lucene.QueryNodeProcessorFactory;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLuceneToJexlQueryParser {
    
    private LuceneToJexlQueryParser parser;
    
    @Before
    public void setUp() {
        parser = new LuceneToJexlQueryParser();
        parser.setSkipTokenizeUnfieldedFields(Sets.newHashSet("noToken"));
        parser.setTokenizedFields(Sets.newHashSet("tokField"));
    }
    
    private static final String anyField = Constants.ANY_FIELD;
    
    @Test(expected = ParseException.class)
    public void testFunctionArgsWithMisMatchedQuotes_case01() throws ParseException {
        // mismatched quotes
        parseQuery("Field:Selector AND #INCLUDE('AND', 'FIELD1, ' regex')");
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionArgsWithMisMatchedQuotes_case02() throws ParseException {
        // mismatched quotes
        parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\)ex)'");
    }
    
    @Test
    public void testOccurrenceFunctionQuoting() throws ParseException {
        assertEquals("filter:occurrence(LOAD_DATE, '>', 1)", parseQuery("#OCCURRENCE(LOAD_DATE, >, 1)"));
        assertEquals("filter:occurrence(LOAD_DATE, '>', 1)", parseQuery("#OCCURRENCE(LOAD_DATE, '>', 1)"));
    }
    
    @Test
    public void testMatchesInGroupFunctionQuoting() throws ParseException {
        assertEquals("grouping:matchesInGroup(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP(FOO, 'foo', BAR, 'bar')"));
        assertEquals("grouping:matchesInGroup(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP(FOO, foo, BAR, bar)"));
        assertEquals("grouping:matchesInGroupLeft(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP_LEFT(FOO, 'foo', BAR, 'bar')"));
        assertEquals("grouping:matchesInGroupLeft(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP_LEFT(FOO, foo, BAR, bar)"));
    }
    
    @Test
    public void testComposableFunctions() throws ParseException {
        assertEquals("filter:includeRegex(foo,bar).size() > 0", parseQuery("#JEXL(\"filter:includeRegex(foo,bar).size() > 0\")"));
    }
    
    @Test
    public void testOneCharacterFunctionArgument() throws ParseException {
        assertEquals("F == 'S' && filter:includeRegex(F, 'test')", parseQuery("F:S AND #INCLUDE(F, 'test')"));
    }
    
    @Test
    public void testFunctionArgumentEscapingMultipleBackslashes() throws ParseException {
        
        // inside single quotes, backslashes can be escaped so when two backslashes are adjacent, they equal one backslash... which then gets escaped inside the
        // JEXL string literal
        // inside single quotes, double quotes can not be escaped, so if you have a \" it shows up as a \"
        // inside single quotes, single quotes must be escaped, the escape gets removed, but then it gets escaped inside a JEXL string literal
        // F:S AND #INCLUDE(FIELD, '"\\\'test\'\"') --> F == 'S' && (filter:includeRegex(FIELD, '"\\\\\'test\'\\"'))
        assertEquals("F == 'S' && filter:includeRegex(FIELD, '\"\\\\\\\\\\'test\\'\\\\\"')", parseQuery("F:S AND #INCLUDE(FIELD, '\"\\\\\\'test\\'\\\"')"));
        
        // if you want a backslash right before a closing single (or double) quote, you need to escape the backslash
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'test\\\\\\\\')", parseQuery("F:S AND #INCLUDE(FIELD, 'test\\\\')"));
        
        try {
            assertEquals("", parseQuery("F:S AND #INCLUDE(FIELD, 'test\\')"));
            Assert.fail("This test case should have failed with a ParseException");
        } catch (ParseException e) {
            // expected
        }
    }
    
    @Test
    public void testFunctionArgumentEscapingSingleQuotes() throws ParseException {
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 're\\'ge(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 're\\'ge(x)')"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 'rege(x)')"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'reg,e(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 'reg,e(x)')"));
        
        // escaped parentheses or commas remain in the result because they are quoted
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\\\\)ex')", parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\)ex')"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\\\\(ex')", parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\(ex')"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\\\\,ex')", parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\,ex')"));
        
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 're[a,b,c]g.*\\\\.\\\\,ex')", parseQuery("F:S AND #INCLUDE(FIELD, 're[a,b,c]g.*\\.\\,ex')"));
        
    }
    
    @Test
    public void testFunctionArgumentEscapingDoubleQuotes() throws ParseException {
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 're\"ge(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, \"re\\\"ge(x)\")"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, \"rege(x)\")"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'reg,e(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, \"reg,e(x)\")"));
    }
    
    @Test
    public void testFunctionArgumentEscapingNoQuotes() throws ParseException {
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'regex')", parseQuery("F:S AND #INCLUDE(FIELD, regex)"));
        
        // escaped the parentheses and commas get by the lucene parser, but gets removed because they are not quoted
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg)ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\\)ex)"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg(ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\\(ex)"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg,ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\\,ex)"));
        
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\"ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\"ex)"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\\\\\"ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\\\"ex)"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\\\\\\'ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\\'ex)"));
        assertEquals("F == 'S' && filter:includeRegex(FIELD, 'reg\\\\ex')", parseQuery("F:S AND #INCLUDE(FIELD, reg\\\\ex)"));
        assertEquals("F == 'S' && filter:getAllMatches(FIELD, 'reg\\\\ex')", parseQuery("F:S AND #GET_ALL_MATCHES(FIELD, reg\\\\ex)"));
    }
    
    @Test
    public void testFunctionArgumentEscapingMixedQuotes() throws ParseException {
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, \"rege(x)\")"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'reg,e(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, 'reg,e(x)')"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)' , FIELD2 , \"rege(x)\" )"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, ' rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2 ,\" rege(x)\" )"));
        assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'r\\\\x\\\\d*.ege(x)'))",
                        parseQuery("F:S AND #INCLUDE('AND', 'FIELD1', 'rege(x)', FIELD2 , \"r\\x\\d*.ege(x)\" )"));
    }
    
    @Test
    public void testBoolean() throws ParseException {
        assertEquals("FIELD1 =~ '99.99' && FIELD2 =~ '1111.*?'", parseQuery("FIELD1:99?99 AND FIELD2:1111*"));
        assertEquals("FIELD1 == '99999' && FIELD2 == '11111'", parseQuery("FIELD1:99999 AND FIELD2:11111"));
        assertEquals("FIELD1 =~ '99.99' && FIELD2 =~ '1111.*?' || FIELD3 == 'AAAA'", parseQuery("FIELD1:99?99 AND FIELD2:1111* OR FIELD3:AAAA"));
        assertEquals("FIELD1 =~ '99.99' && (FIELD2 =~ '1111.*?' || FIELD3 == 'AAAA')", parseQuery("FIELD1:99?99 AND (FIELD2:1111* OR FIELD3:AAAA)"));
        assertEquals("FIELD1 =~ '99.99' && (FIELD2 =~ '1111.*?' || FIELD3 == 'AAAA') && !(FIELD4 == '1234')",
                        parseQuery("FIELD1:99?99 AND (FIELD2:1111* OR FIELD3:AAAA) NOT FIELD4:1234"));
        assertEquals("A == '1' && B == '2' && C == '3' && !(D == '4')", parseQuery("A:1 B:2 C:3 NOT D:4"));
        assertEquals("(A == '1' && B == '2' && C == '3') && !(D == '4')", parseQuery("(A:1 B:2 C:3) NOT D:4"));
        
        assertEquals("A =~ '11\\.22.*?'", parseQuery("A:11.22*"));
        assertEquals("A =~ '11\\\\22.*?'", parseQuery("A:11\\\\22*"));
        assertEquals("A =~ 'TESTING.1\\\\22.*?'", parseQuery("A:TESTING?1\\\\22*"));
    }
    
    @Test
    public void testNot() throws ParseException {
        
        assertEquals("(F1 == 'A' && F2 == 'B') && !(F3 == 'C') && !(F4 == 'D')", parseQuery("(F1:A AND F2:B) NOT F3:C NOT F4:D"));
        assertEquals("(F1 == 'A' && F2 == 'B' || F3 == 'C') && !(F4 == 'D') && !(F5 == 'E') && !(F6 == 'F')",
                        parseQuery("(F1:A AND F2:B OR F3:C) NOT F4:D NOT F5:E NOT F6:F"));
    }
    
    @Test
    public void testCompare() throws ParseException {
        assertEquals("F1 == 'A' && F2 == 'B' && filter:compare(F1, '<', 'ALL', F2)", parseQuery("F1:A AND F2:B AND #COMPARE(F1, <, ALL, F2)"));
        assertEquals("F1 == 'A' && F2 == 'B' && filter:compare(F1, '>=', 'ANY', F2)", parseQuery("F1:A AND F2:B AND #COMPARE(F1, >=, ANY, F2)"));
    }
    
    @Test
    public void testCompareInvalidOpArg() {
        try {
            parseQuery("F1:A AND F2:B AND #COMPARE(F1, <>, ALL, F2)");
            fail("Expected an IllegalArgumentException");
        } catch (ParseException e) {
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().startsWith("#COMPARE function requires a valid op arg:"));
        }
    }
    
    @Test
    public void testCompareInvalidModeArg() {
        try {
            parseQuery("F1:A AND F2:B AND #COMPARE(F1, <, A FEW OR SEVERAL, F2)");
            fail("Expected an IllegalArgumentException");
        } catch (ParseException e) {
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().startsWith("#COMPARE function requires a valid mode arg:"));
        }
    }
    
    @Test
    public void testTokenizedFieldNoAnalyzer() throws ParseException {
        parser.setAnalyzer(null);
        assertEquals("TOKFIELD == '1234 5678 to bird'", parseQuery("TOKFIELD:1234\\ 5678\\ to\\ bird"));
        assertEquals("TOKFIELD == '1234 5678 to bird'", parseQuery("TOKFIELD:\"1234\\ 5678\\ to\\ bird\""));
    }
    
    @Test
    public void testTokenizedFieldMixedCase() throws ParseException {
        assertEquals("TOKFIELD == 'BIRD'", parseQuery("TOKFIELD:BIRD"));
    }
    
    @Test
    public void testTokenizedFieldUnfieldedEnabled() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        assertEquals("(_ANYFIELD_ == '1234 5678 to bird' || content:within(4, termOffsetMap, '1234', '5678', 'bird'))", parseQuery("1234\\ 5678\\ to\\ bird"));
        assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testTokenizedUnfieldedTerm() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        // verify that new slop is calculated properly.
        parser.setUseSlopForTokenizedTerms(false);
        assertEquals("(_ANYFIELD_ == 'wi-fi' || content:phrase(termOffsetMap, 'wi', 'fi'))", parseQuery("wi-fi"));
        parser.setUseSlopForTokenizedTerms(true);
        assertEquals("(_ANYFIELD_ == 'wi-fi' || content:within(2, termOffsetMap, 'wi', 'fi'))", parseQuery("wi-fi"));
    }
    
    @Test
    public void testTokenizedUnfieldedSlopPhrase() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        // verify that new slop is calculated properly.
        assertEquals("content:within(3, termOffsetMap, 'portable', 'document')", parseQuery("\"portable document\"~3"));
        assertEquals("(content:within(5, termOffsetMap, 'portable', 'wi-fi', 'access-point') || content:within(7, termOffsetMap, 'portable', 'wi', 'fi', 'access', 'point'))",
                        parseQuery("\"portable wi-fi access-point\"~5"));
    }
    
    @Test
    public void testTokenizedUnfieldedPhrase() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        // verifies that the slop setting has no impact on things that come in as phrases without slop.
        parser.setUseSlopForTokenizedTerms(false);
        assertEquals("content:phrase(termOffsetMap, 'portable', 'document')", parseQuery("\"portable document\""));
        assertEquals("(content:phrase(termOffsetMap, 'portable', 'wi-fi', 'access-point') || content:phrase(termOffsetMap, 'portable', 'wi', 'fi', 'access', 'point'))",
                        parseQuery("\"portable wi-fi access-point\""));
        parser.setUseSlopForTokenizedTerms(true);
        assertEquals("content:phrase(termOffsetMap, 'portable', 'document')", parseQuery("\"portable document\""));
        assertEquals("(content:phrase(termOffsetMap, 'portable', 'wi-fi', 'access-point') || content:phrase(termOffsetMap, 'portable', 'wi', 'fi', 'access', 'point'))",
                        parseQuery("\"portable wi-fi access-point\""));
    }
    
    @Test
    public void testTokenizedFieldUnfieldedEnabledNoAnalyzer() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        parser.setAnalyzer(null);
        assertEquals("_ANYFIELD_ == '1234 5678 to bird'", parseQuery("1234\\ 5678\\ to\\ bird"));
        assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testTokenizedFieldUnfieldedDisabled() throws ParseException {
        parser.setTokenizeUnfieldedQueries(false);
        assertEquals("_ANYFIELD_ == '1234 5678 to bird'", parseQuery("1234\\ 5678\\ to\\ bird"));
        assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testSkipTokenizedFields() throws ParseException {
        parser.setTokenizeUnfieldedQueries(false);
        assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
        // test for case independence of field name
        assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("notoken:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testSkipTokenizedFieldsNoAnalyzer() throws ParseException {
        parser.setAnalyzer(null);
        assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testTokenizedPhraseFail() throws ParseException {
        assertEquals("(TOKFIELD == 'joh.nny chicken' || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:\"joh.nny\\ chicken\""));
    }
    
    @Test
    public void testWildcards() throws ParseException {
        assertEquals("F1 == '*1234'", parseQuery("F1:\\*1234"));
        assertEquals("F1 =~ '\\*12.34'", parseQuery("F1:\\*12?34"));
        assertEquals("F1 == '*'", parseQuery("F1:\\*"));
        assertEquals("F1 =~ '\\\\.*?'", parseQuery("F1:\\\\*"));
        assertEquals("F1 == '\\\\*'", parseQuery("F1:\\\\\\*"));
        assertEquals("F1 =~ '\\\\.*?x.*?'", parseQuery("F1:\\\\*x*"));
    }
    
    @Test
    public void testRanges() throws ParseException {
        assertEquals("((_Bounded_ = true) && (fieldName >= 'aaa' && fieldName <= 'bbb'))", parseQuery("fieldName:[aaa TO bbb]"));
        assertEquals("((_Bounded_ = true) && (fieldName > 'aaa' && fieldName < 'bbb'))", parseQuery("fieldName:{aaa TO bbb}"));
        assertEquals("fieldName1 == 'value1' && ((_Bounded_ = true) && (fieldName2 >= 'aaa' && fieldName2 <= 'bbb')) && fieldName3 == 'value3'",
                        parseQuery("fieldName1:value1 AND fieldName2:[aaa TO bbb] AND fieldName3:value3"));
        assertEquals("((_Bounded_ = true) && (F >= 'A' && F <= 'B'))", parseQuery("F:[A TO B]"));
        assertEquals("((_Bounded_ = true) && (F >= 'A\\\\*' && F <= 'B'))", parseQuery("F:[A\\\\\\* TO B]"));
        assertEquals("((_Bounded_ = true) && (F > 'lower' && F <= 'upper'))", parseQuery("F:{lower TO upper]"));
        assertEquals("((_Bounded_ = true) && (F >= 'lower' && F < 'upper'))", parseQuery("F:[lower TO upper}"));
    }
    
    @Test
    public void testPhrases() throws ParseException {
        assertEquals("content:phrase(termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("\"quick brown fox\""));
        assertEquals("content:phrase(TOKFIELD, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("TOKFIELD:\"quick brown fox\""));
        
        // testing case independence of "TOKFIELD" This would return content:phrase if it were not case independent
        assertEquals("content:phrase(tokfield, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("tokfield:\"quick brown fox\""));
        
        assertEquals("TOKFIELD == 'value' && content:phrase(termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("TOKFIELD:value AND \"quick brown fox\""));
        assertEquals(anyField + " == 'quick'", parseQuery("\"quick\""));
        assertEquals("content:phrase(termOffsetMap, 'qui.ck', 'brown', 'fox')", parseQuery("\"qui\\.ck brown fox\""));
        
        // Test smart quote conversions
        assertEquals("content:phrase(termOffsetMap, 'quick', 'brown', 'fox1')", parseQuery("\u0093quick brown fox1\u0094"));
        assertEquals("content:phrase(termOffsetMap, 'quick', 'brown', 'fox2')", parseQuery("\u201cquick brown fox2\u201d"));
    }
    
    @Test
    public void testMultiWordPhraseCompatability() throws ParseException {
        assertEquals(anyField + " == 'alpha beta'", parseQuery("alpha\\ beta"));
        assertEquals(anyField + " == 'alpha beta'", parseQuery("\"alpha\\ beta\""));
        assertEquals(anyField + " == 'alpha beta' && " + anyField + " == 'gamma'", parseQuery("\"alpha\\ beta\" gamma"));
        assertEquals("content:phrase(termOffsetMap, 'alpha beta', 'gamma')", parseQuery("\"alpha\\ beta gamma\""));
    }
    
    @Test
    public void testMiscEscape() throws ParseException {
        // apostrophe escapes
        assertEquals(anyField + " == 'johnny\\'s'", parseQuery("johnny's"));
        assertEquals(anyField + " == 'johnny\\'s'", parseQuery("johnny\\'s")); // lucene drops single slash
        assertEquals(anyField + " == 'johnny\\'s'", parseQuery("\"johnny's\""));
        assertEquals("content:phrase(termOffsetMap, 'johnny\\'s', 'chicken')", parseQuery("\"johnny's chicken\""));
        assertEquals("A =~ '11\\'22.*?'", parseQuery("A:11'22*"));
        //
        // // F:[A'\\\* TO B'], searching from A'\\* ("A" followed by a single quote, a backslash, and an asterisk to "B" followed by a single quote
        assertEquals("((_Bounded_ = true) && (F >= 'A\\'\\\\*' && F <= 'B\\''))", parseQuery("F:[A'\\\\\\* TO B']"));
        //
        // // space escapes
        assertEquals(anyField + " == 'joh.nny chicken'", parseQuery("joh.nny\\ chicken"));
        assertEquals(anyField + " =~ 'joh\\.nny chick.*?'", parseQuery("joh.nny\\ chick*"));
        assertEquals(anyField + " == 'joh.nny chicken'", parseQuery("\"joh.nny\\ chicken\""));
        assertEquals("content:phrase(termOffsetMap, 'joh.nny', 'chic ken')", parseQuery("\"joh.nny chic\\ ken\""));
        assertEquals("content:phrase(termOffsetMap, 'joh.nny', 'chic k*')", parseQuery("\"joh.nny chic\\ k*\""));
        
        // quote escapes
        assertEquals("content:phrase(termOffsetMap, 'joh.nny', '\"chicken\"')", parseQuery("\"joh.nny \\\"chicken\\\"\""));
        
        // unicode escapes.
        assertEquals("FIELD =~ 'joh\\.nny.*?\\\\''", parseQuery("FIELD:joh.nny*\\\\'"));
        assertEquals("FIELD =~ 'joh\\.nny\\\\five.*?\\''", parseQuery("FIELD:joh.nny\\\\five*'"));
        
        // literal blackslashes
        assertEquals(anyField + " == 'yep\\\\yep'", parseQuery("yep\\\\yep"));
        assertEquals("content:phrase(termOffsetMap, 'yep\\\\yep', 'otherterm')", parseQuery("\"yep\\\\yep otherterm\""));
    }
    
    @Test
    public void testMiscEscapeTokenization() throws ParseException {
        // apostrophe escapes
        assertEquals("(TOKFIELD == 'johnny\\'s' || TOKFIELD == 'johnny')", parseQuery("TOKFIELD:johnny's"));
        assertEquals("(TOKFIELD == 'johnny\\'s' || TOKFIELD == 'johnny')", parseQuery("TOKFIELD:johnny\\'s")); // lucene drops single slash
        assertEquals("(TOKFIELD == 'johnny\\'s' || TOKFIELD == 'johnny')", parseQuery("TOKFIELD:\"johnny's\""));
        assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'johnny\\'s', 'chicken') || content:phrase(TOKFIELD, termOffsetMap, 'johnny', 'chicken'))",
                        parseQuery("TOKFIELD:\"johnny's chicken\""));
        assertEquals("TOKFIELD =~ '11\\'22.*?'", parseQuery("TOKFIELD:11'22*"));
        assertEquals("((_Bounded_ = true) && (TOKFIELD >= 'A\\'\\\\*' && TOKFIELD <= 'B\\''))", parseQuery("TOKFIELD:[A'\\\\\\* TO B']"));
        
        // space escapes
        assertEquals("(TOKFIELD == 'joh.nny chicken' || content:within(TOKFIELD, 2, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:joh.nny\\ chicken"));
        assertEquals("TOKFIELD =~ 'joh\\.nny chick.*?'", parseQuery("TOKFIELD:joh.nny\\ chick*"));
        assertEquals("(TOKFIELD == 'joh.nny chicken' || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:\"joh.nny\\ chicken\""));
        
        assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic ken') || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic', 'ken'))",
                        parseQuery("TOKFIELD:\"joh.nny chic\\ ken\""));
        assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic k*') || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic', 'k'))",
                        parseQuery("TOKFIELD:\"joh.nny chic\\ k*\""));
        
        // quote escapes
        assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', '\"chicken\"') || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:\"joh.nny \\\"chicken\\\"\""));
        
        // unicode escapes.
        assertEquals("TOKFIELD =~ 'joh\\.nny.*?\\\\''", parseQuery("TOKFIELD:joh.nny*\\\\'"));
        assertEquals("TOKFIELD =~ 'joh\\.nny\\\\five.*?\\''", parseQuery("TOKFIELD:joh.nny\\\\five*'"));
        
        // literal blackslashes
        assertEquals("(TOKFIELD == 'someone\\\\234someplace.com' || content:within(TOKFIELD, 2, termOffsetMap, 'someone', '234someplace.com'))",
                        parseQuery("TOKFIELD:someone\\\\234someplace.com"));
        assertEquals("(content:phrase(TOKFIELD, termOffsetMap, 'someone\\\\234someplace.com', 'otherterm') || content:phrase(TOKFIELD, termOffsetMap, 'someone', '234someplace.com', 'otherterm'))",
                        parseQuery("TOKFIELD:\"someone\\\\234someplace.com otherterm\""));
        // FIX THIS, trailing slashes in queries should be acceptable.
        // Assert.assertEquals("(TOKFIELD == 'someone\\234someplace.com\\' || content:within(TOKFIELD, 2, termOffsetMap, 'someone',
        // '234someplace.com'))",
        // parseQuery("TOKFIELD:someone\\\\234someplace.com\\\\"));
    }
    
    @Test
    public void testWithin() throws ParseException {
        assertEquals("content:within(10, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("\"quick brown fox\"~10"));
        assertEquals("content:within(FIELD, 20, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("FIELD:\"quick brown fox\"~20"));
        assertEquals("FIELD == 'value' && content:within(15, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("FIELD:value AND \"quick brown fox\"~15"));
        assertEquals(anyField + " == 'quick'", parseQuery("\"quick\"~10"));
    }
    
    @Test
    public void testUnbalancedParens() throws ParseException {
        assertEquals("(FIELD == 'value') && (content:phrase(termOffsetMap, 'term1', 'term2'))", parseQuery("(FIELD:value) AND (\"term1 term2\")"));
        assertEquals("content:phrase(termOffsetMap, 'term1', 'term2', 'term3') && " + anyField + " == 'term4'",
                        parseQuery("\"term1 term2 term3\" AND \"term4\""));
    }
    
    private String parseQuery(String query) throws ParseException {
        String parsedQuery = null;
        
        try {
            QueryNode node = parser.parse(query);
            if (node instanceof ServerHeadNode) {
                parsedQuery = node.getOriginalQuery();
            }
        } catch (UnsupportedOperationException e) {
            throw new ParseException(e);
        } catch (RuntimeException e) {
            throw new ParseException(e);
        }
        return parsedQuery;
    }
    
    @Test
    public void testFunctions() throws ParseException {
        testFunction("filter:isNull(nullfield)", "#isnull(nullfield)");
        testFunction("not(filter:isNull(field))", "#isnotnull(field)");
        testFunction("filter:includeRegex(_ANYFIELD_, 'r1')", "#include(r1)");
        testFunction("(filter:includeRegex(f1, 'r1') && filter:includeRegex(f2, 'r2'))", "#include(f1, r1, f2, r2)");
        testFunction("(filter:includeRegex(f1, 'r1') && filter:includeRegex(f2, 'r2'))", "#include(AND, f1, r1, f2, r2)");
        testFunction("(filter:includeRegex(f1, 'r1') || filter:includeRegex(f2, 'r2'))", "#include(OR, f1, r1, f2, r2)");
        testFunction("(not(filter:includeRegex(f1, 'see.*jane.*run')) || not(filter:includeRegex(f2, 'test,escaping')))",
                        "#exclude(f1, see.*jane.*run, f2, test\\,escaping)");
        testFunction("(not(filter:includeRegex(f1, 'see.*jane.*run')) && not(filter:includeRegex(f2, 'test,escaping')))",
                        "#exclude(OR, f1, see.*jane.*run, f2, test\\,escaping)");
        testFunction("(not(filter:includeRegex(f1, 'see.*jane.*run')) || not(filter:includeRegex(f2, 'test,escaping')))",
                        "#exclude(AND, f1, see.*jane.*run, f2, test\\,escaping)");
        testFunction("filter:includeText(_ANYFIELD_, 'r1')", "#text(r1)");
        testFunction("(filter:includeText(f1, 'r1') && filter:includeText(f2, 'r2'))", "#text(f1, r1, f2, r2)");
        testFunction("(filter:includeText(f1, 'r1') && filter:includeText(f2, 'r2'))", "#text(AND, f1, r1, f2, r2)");
        testFunction("(filter:includeText(f1, 'r1') || filter:includeText(f2, 'r2'))", "#text(OR, f1, r1, f2, r2)");
        
        testFunction("filter:afterLoadDate(LOAD_DATE, '20140101')", "#loaded(after, 20140101)");
        testFunction("filter:afterLoadDate(LOAD_DATE, '20140101', 'yyyyMMdd')", "#loaded(after, 20140101, yyyyMMdd)");
        testFunction("filter:beforeLoadDate(LOAD_DATE, '20140101')", "#loaded(before, 20140101)");
        testFunction("filter:beforeLoadDate(LOAD_DATE, '20140101', 'yyyyMMdd')", "#loaded(before, 20140101, yyyyMMdd)");
        testFunction("filter:betweenLoadDates(LOAD_DATE, '20140101', '20140102')", "#loaded(between, 20140101, 20140102)");
        testFunction("filter:betweenLoadDates(LOAD_DATE, '20140101', '20140102', 'yyyyMMdd')", "#loaded(between, 20140101, 20140102, yyyyMMdd)");
        testFunction("filter:betweenLoadDates(LOAD_DATE, '20140101', '20140102')", "#loaded(20140101, 20140102)");
        testFunction("filter:betweenLoadDates(LOAD_DATE, '20140101', '20140102', 'yyyyMMdd')", "#loaded(20140101, 20140102, yyyyMMdd)");
        
        testFunction("filter:afterDate(SOME_DATE, '20140101')", "#date(SOME_DATE, after, 20140101)");
        testFunction("filter:afterDate(SOME_DATE, '20140101', 'yyyyMMdd')", "#date(SOME_DATE, after, 20140101, yyyyMMdd)");
        testFunction("filter:beforeDate(SOME_DATE, '20140101')", "#date(SOME_DATE, before, 20140101)");
        testFunction("filter:beforeDate(SOME_DATE, '20140101', 'yyyyMMdd')", "#date(SOME_DATE, before, 20140101, yyyyMMdd)");
        testFunction("filter:betweenDates(SOME_DATE, '20140101', '20140102')", "#date(SOME_DATE, between, 20140101, 20140102)");
        testFunction("filter:betweenDates(SOME_DATE, '20140101', '20140102', 'yyyyMMdd')", "#date(SOME_DATE, between, 20140101, 20140102, yyyyMMdd)");
        testFunction("filter:betweenDates(SOME_DATE, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')",
                        "#date(SOME_DATE, between, yyyyMMddHHmmss, 20140101, 20140102, yyyyMMdd)");
        testFunction("filter:betweenDates(SOME_DATE, '20140101', '20140102')", "#date(SOME_DATE, 20140101, 20140102)");
        testFunction("filter:betweenDates(SOME_DATE, '20140101', '20140102', 'yyyyMMdd')", "#date(SOME_DATE, 20140101, 20140102, yyyyMMdd)");
        testFunction("filter:betweenDates(SOME_DATE, 'yyyyMMddHHmmss', '20140101', '20140102', 'yyyyMMdd')",
                        "#date(SOME_DATE, yyyyMMddHHmmss, 20140101, 20140102, yyyyMMdd)");
        
        testFunctionPlacement("#isnull(nullfield)");
        testFunctionPlacement("#isnotnull(nullfield)");
        testFunctionPlacement("#include(AND, f1, r1, f2, r2)");
        testFunctionPlacement("#exclude(AND, f1, r1, f2, r2)");
        testFunctionPlacement("#loaded(after, v1)");
        testFunctionPlacement("#date(f1, after, v1)");
        
    }
    
    public void testFunction(String jexl, String lucene) throws ParseException {
        assertEquals("fielda == 'selectora' && " + jexl, parseQuery("fielda:selectora " + lucene));
        assertEquals("fielda == 'selectora' && fieldb == 'selectorb' && " + jexl, parseQuery("fielda:selectora fieldb:selectorb " + lucene));
        assertEquals("(fielda == 'selectora' || fieldb == 'selectorb' || fieldc == 'selectorc') && " + jexl,
                        parseQuery("(fielda:selectora OR fieldb:selectorb OR fieldc:selectorc) " + lucene));
        
    }
    
    public void testFunctionPlacement(String func) {
        try {
            parseQuery(func);
        } catch (Exception e) {
            Assert.fail("Queries that are just post filters should now be allowed: " + func);
        }
        String query = null;
        try {
            query = "fielda:selectora or " + func;
            parseQuery(query);
        } catch (Exception e) {
            Assert.fail("Queries should now be able to have a query or'ed with a post filter: " + query);
        }
        try {
            query = "fielda:selectora or (fieldb:selecorb and #isnull(nullfield))";
            parseQuery(query);
        } catch (Exception e) {
            Assert.fail("Queries should now be able to have a query with a nested post filter: " + query);
        }
    }
    
    @Test
    public void testFunctionWrongDepth() throws ParseException {
        try {
            parseQuery("#isnotnull(nonnullfield)");
        } catch (ParseException ex) {
            Assert.fail("Should no longer throw ParseException");
        }
    }
    
    @Test
    public void testGrouping() throws ParseException {
        String s = "FOO.1:ABC AND FOO.2:DEF";
        assertEquals("FOO.1 == 'ABC' && FOO.2 == 'DEF'", parseQuery(s));
    }
    
    @Test
    public void testSmartQuoteReplacement() throws ParseException {
        String s = "\u0093see jane run\u0094";
        assertEquals("content:phrase(termOffsetMap, 'see', 'jane', 'run')", parseQuery(s));
    }
    
    @Test
    public void testParseSpaces() throws ParseException {
        assertEquals("FIELD == '  '", parseQuery("FIELD:\\ \\ "));
        assertEquals("FIELD == '  TEST'", parseQuery("FIELD:\\ \\ TEST"));
        assertEquals("FIELD == 'TEST  '", parseQuery("FIELD:TEST\\ \\ "));
    }
    
    @Test
    public void testIsNotNull() throws ParseException {
        assertEquals("F == 'S' && not(filter:isNull(FOO))", parser.parse("F:S #ISNOTNULL(FOO)").getOriginalQuery());
    }
    
    @Test
    public void testRegex() throws ParseException {
        assertEquals("F =~ 'test.[abc]?ing'", parser.parse("F:/test.[abc]?ing/").getOriginalQuery());
        assertEquals("F =~ 'test./[abc]?ing'", parser.parse("F:/test.\\/[abc]?ing/").getOriginalQuery());
        assertEquals("F =~ 'test.*[^abc]?ing'", parser.parse("F:/test.*[^abc]?ing/").getOriginalQuery());
        assertEquals("F == '/test/file/path'", parser.parse("F:\"/test/file/path\"").getOriginalQuery());
    }
    
    @Test
    public void testOpenEndedRanges() throws ParseException {
        assertEquals("(F >= 'lower')", parser.parse("F:[lower TO *]").getOriginalQuery());
        assertEquals("(F <= 'upper')", parser.parse("F:[* TO upper]").getOriginalQuery());
        assertEquals("(F > 'lower')", parser.parse("F:{lower TO *}").getOriginalQuery());
        assertEquals("(F < 'upper')", parser.parse("F:{* TO upper}").getOriginalQuery());
        assertEquals("(LO <= '2') && (HI >= '3') && FIELD == 'foobar'", parser.parse("LO:[* TO 2] HI:[3 TO *] FIELD:foobar").getOriginalQuery());
    }
    
    @Test
    public void testNumericRange() throws ParseException {
        assertEquals("((_Bounded_ = true) && (FOO >= '1' && FOO <= '5000'))", parser.parse("FOO:[1 TO 5000]").getOriginalQuery());
    }
    
    @Test
    public void testParseRegexsWithEscapes() throws ParseException {
        assertEquals("F == '6515' && FOOBAR =~ 'Foo/5\\.0 \\(ibar.*?'", parser.parse("F:6515 and FOOBAR:Foo\\/5.0\\ \\(ibar*").getOriginalQuery());
        assertEquals("FOO == 'bar' && BAZ =~ 'Foo Foo Foo.*?'", parser.parse("FOO:bar BAZ:Foo\\ Foo\\ Foo*").getOriginalQuery());
        assertEquals("FOO == 'bar' && BAZ =~ 'Foo Foo Foo.*'", parser.parse("FOO:bar BAZ:/Foo Foo Foo.*/").getOriginalQuery());
        assertEquals("FOO == 'bar' && BAZ =~ 'Foo Foo Foo.*?'", parser.parse("FOO:bar BAZ:/Foo Foo Foo.*?/").getOriginalQuery());
        
        assertEquals("FOO == 'bar' && BAZ =~ 'Foo/Foo Foo.*?'", parser.parse("FOO:bar BAZ:Foo\\/Foo\\ Foo*").getOriginalQuery());
        assertEquals("FOO == 'bar' && BAZ =~ 'Foo/Foo\\ Foo.*'", parser.parse("FOO:bar BAZ:/Foo\\/Foo\\ Foo.*/").getOriginalQuery());
        assertEquals("FOO == 'bar' && BAZ =~ 'Foo/Foo Foo.*?'", parser.parse("FOO:bar BAZ:/Foo\\/Foo Foo.*?/").getOriginalQuery());
    }
    
    @Test
    public void testUniqueFunctions() throws ParseException {
        assertEquals("f:unique('field1','field2','field3')", parser.parse("#unique(field1,field2,field3)").getOriginalQuery());
        assertEquals("f:unique('field1[ALL','DAY]','field2')", parser.parse("#unique(field1[ALL,DAY],field2)").getOriginalQuery());
        assertEquals("f:unique('field1[ALL','DAY]','field2[MINUTE]','field3[HOUR]')", parser.parse("#unique(field1[ALL,DAY],field2[MINUTE],field3[HOUR])")
                        .getOriginalQuery());
        
        assertEquals("f:unique_by_day('field1','field2','field3')", parser.parse("#unique_by_day(field1,field2,field3)").getOriginalQuery());
        assertEquals("f:unique_by_hour('field1','field2','field3')", parser.parse("#unique_by_hour(field1,field2,field3)").getOriginalQuery());
        assertEquals("f:unique_by_minute('field1','field2','field3')", parser.parse("#unique_by_minute(field1,field2,field3)").getOriginalQuery());
        
        Throwable exception = assertThrows(ParseException.class, () -> parser.parse("#unique_by_day('field[HOUR]')")).getCause();
        assertTrue(exception.getMessage().contains(
                        "unique_by_day does not support the advanced unique syntax, only a simple comma-delimited list of fields is allowed"));
        
        exception = assertThrows(ParseException.class, () -> parser.parse("#unique_by_hour('field[MINUTE>]')")).getCause();
        assertTrue(exception.getMessage().contains(
                        "unique_by_hour does not support the advanced unique syntax, only a simple comma-delimited list of fields is allowed"));
        
        exception = assertThrows(ParseException.class, () -> parser.parse("#unique_by_minute('field[HOUR]')")).getCause();
        assertTrue(exception.getMessage().contains(
                        "unique_by_minute does not support the advanced unique syntax, only a simple comma-delimited list of fields is allowed"));
    }
    
    private static class TestQueryNodeProcessorFactory extends QueryNodeProcessorFactory {
        @Override
        public QueryNodeProcessor create(QueryConfigHandler configHandler) {
            return new QueryNodeProcessorPipeline(configHandler);
        }
    }
    
    @Test
    public void testCustomQueryNodeProcessor() throws ParseException {
        String query = "TOKFIELD:\"quick wi-fi fox\"";
        Assert.assertEquals(
                        "(content:phrase(TOKFIELD, termOffsetMap, 'quick', 'wi-fi', 'fox') || content:phrase(TOKFIELD, termOffsetMap, 'quick', 'wi', 'fi', 'fox'))",
                        parseQuery(query));
        parser.setQueryNodeProcessorFactory(new TestQueryNodeProcessorFactory());
        Assert.assertEquals("content:phrase(TOKFIELD, termOffsetMap, 'quick', 'wi-fi', 'fox')", parseQuery(query));
    }
    
    @Test
    public void testParseIncludeFunctionPartOfUnion() throws ParseException {
        testFunction("(filter:includeRegex(f1, 'r1') || filter:includeRegex(f2, 'r2'))", "#include(OR, f1, r1, f2, r2)");
        testFunction("filter:includeRegex(f1, 'r1')", "#include(f1, r1)");
    }
}
