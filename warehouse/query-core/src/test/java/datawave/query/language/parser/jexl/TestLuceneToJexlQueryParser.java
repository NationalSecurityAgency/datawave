package datawave.query.language.parser.jexl;

import com.google.common.collect.Sets;
import datawave.query.language.parser.ParseException;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;
import datawave.query.Constants;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLuceneToJexlQueryParser {
    
    private LuceneToJexlQueryParser parser;
    
    @Before
    public void setUp() {
        parser = new LuceneToJexlQueryParser();
        parser.setSkipTokenizeUnfieldedFields(Sets.newHashSet("noToken"));
        parser.setTokenizedFields(Sets.newHashSet("tokField"));
    }
    
    private static final String anyField = Constants.ANY_FIELD;
    
    @Test
    public void testFunctionArgumentEscapingFailures() throws ParseException {
        try {
            // mismatched quotes
            parseQuery("Field:Selector AND #INCLUDE('AND', 'FIELD1, ' regex')");
            Assert.fail("Expected ParseException");
        } catch (ParseException e) {
            
        }
        
        try {
            // mismatched quotes
            parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\)ex)'");
            Assert.fail("Expected ParseException");
        } catch (ParseException e) {
            
        }
    }
    
    @Test
    public void testOccurrenceFunctionQuoting() throws ParseException {
        Assert.assertEquals("filter:occurrence(LOAD_DATE, '>', 1)", parseQuery("#OCCURRENCE(LOAD_DATE, >, 1)"));
        Assert.assertEquals("filter:occurrence(LOAD_DATE, '>', 1)", parseQuery("#OCCURRENCE(LOAD_DATE, '>', 1)"));
    }
    
    @Test
    public void testMatchesInGroupFunctionQuoting() throws ParseException {
        Assert.assertEquals("grouping:matchesInGroup(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP(FOO, 'foo', BAR, 'bar')"));
        Assert.assertEquals("grouping:matchesInGroup(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP(FOO, foo, BAR, bar)"));
        Assert.assertEquals("grouping:matchesInGroupLeft(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP_LEFT(FOO, 'foo', BAR, 'bar')"));
        Assert.assertEquals("grouping:matchesInGroupLeft(FOO, 'foo', BAR, 'bar')", parseQuery("#MATCHES_IN_GROUP_LEFT(FOO, foo, BAR, bar)"));
    }
    
    @Test
    public void testComposableFunctions() throws ParseException {
        Assert.assertEquals("filter:includeRegex(foo,bar).size() > 0", parseQuery("#JEXL(\"filter:includeRegex(foo,bar).size() > 0\")"));
    }
    
    @Test
    public void testOneCharacterFunctionArgument() throws ParseException {
        
        Assert.assertEquals("F == 'S' && (filter:includeRegex(F, 'test'))", parseQuery("F:S AND #INCLUDE(F, 'test')"));
    }
    
    @Test
    public void testFunctionArgumentEscapingMultipleBackslashes() throws ParseException {
        
        // inside single quotes, backslashes can be escaped so when two backslashes are adjacent, they equal one backslash... which then gets escaped inside the
        // JEXL string literal
        // inside single quotes, double quotes can not be escaped, so if you have a \" it shows up as a \"
        // inside single quotes, single quotes must be escaped, the escape gets removed, but then it gets escaped inside a JEXL string literal
        // F:S AND #INCLUDE(FIELD, '"\\\'test\'\"') --> F == 'S' && (filter:includeRegex(FIELD, '"\\\\\'test\'\\"'))
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, '\"\\\\\\\\\\'test\\'\\\\\"'))",
                        parseQuery("F:S AND #INCLUDE(FIELD, '\"\\\\\\'test\\'\\\"')"));
        
        // if you want a backslash right before a closing single (or double) quote, you need to escape the backslash
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'test\\\\\\\\'))", parseQuery("F:S AND #INCLUDE(FIELD, 'test\\\\')"));
        
        try {
            Assert.assertEquals("", parseQuery("F:S AND #INCLUDE(FIELD, 'test\\')"));
            Assert.fail("This test case should have failed with a ParseException");
        } catch (ParseException e) {
            // expected
        }
    }
    
    @Test
    public void testFunctionArgumentEscapingSingleQuotes() throws ParseException {
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 're\\'ge(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 're\\'ge(x)')"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 'rege(x)')"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'reg,e(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 'reg,e(x)')"));
        
        // escaped parentheses or commas remain in the result because they are quoted
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\\\\)ex'))", parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\)ex')"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\\\\(ex'))", parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\(ex')"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\\\\,ex'))", parseQuery("F:S AND #INCLUDE(FIELD, 'reg\\,ex')"));
        
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 're[a,b,c]g.*\\\\.\\\\,ex'))",
                        parseQuery("F:S AND #INCLUDE(FIELD, 're[a,b,c]g.*\\.\\,ex')"));
        
    }
    
    @Test
    public void testFunctionArgumentEscapingDoubleQuotes() throws ParseException {
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 're\"ge(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, \"re\\\"ge(x)\")"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, \"rege(x)\")"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'reg,e(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, \"reg,e(x)\")"));
    }
    
    @Test
    public void testFunctionArgumentEscapingNoQuotes() throws ParseException {
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'regex'))", parseQuery("F:S AND #INCLUDE(FIELD, regex)"));
        
        // escaped the parentheses and commas get by the lucene parser, but gets removed because they are not quoted
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg)ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\\)ex)"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg(ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\\(ex)"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg,ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\\,ex)"));
        
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\"ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\"ex)"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\\\\\"ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\\\"ex)"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\\\\\\'ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\\'ex)"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD, 'reg\\\\ex'))", parseQuery("F:S AND #INCLUDE(FIELD, reg\\\\ex)"));
        Assert.assertEquals("F == 'S' && (filter:getAllMatches(FIELD, 'reg\\\\ex'))", parseQuery("F:S AND #GET_ALL_MATCHES(FIELD, reg\\\\ex)"));
        
    }
    
    @Test
    public void testFunctionArgumentEscapingMixedQuotes() throws ParseException {
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, \"rege(x)\")"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'reg,e(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, 'reg,e(x)')"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)' , FIELD2 , \"rege(x)\" )"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, ' rege(x)'))",
                        parseQuery("F:S AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2 ,\" rege(x)\" )"));
        Assert.assertEquals("F == 'S' && (filter:includeRegex(FIELD1, 'rege(x)') && filter:includeRegex(FIELD2, 'r\\\\x\\\\d*.ege(x)'))",
                        parseQuery("F:S AND #INCLUDE('AND', 'FIELD1', 'rege(x)', FIELD2 , \"r\\x\\d*.ege(x)\" )"));
    }
    
    @Test
    public void testBoolean() throws ParseException {
        Assert.assertEquals("FIELD1 =~ '99.99' && FIELD2 =~ '1111.*?'", parseQuery("FIELD1:99?99 AND FIELD2:1111*"));
        Assert.assertEquals("FIELD1 == '99999' && FIELD2 == '11111'", parseQuery("FIELD1:99999 AND FIELD2:11111"));
        Assert.assertEquals("FIELD1 =~ '99.99' && FIELD2 =~ '1111.*?' || FIELD3 == 'AAAA'", parseQuery("FIELD1:99?99 AND FIELD2:1111* OR FIELD3:AAAA"));
        Assert.assertEquals("FIELD1 =~ '99.99' && (FIELD2 =~ '1111.*?' || FIELD3 == 'AAAA')", parseQuery("FIELD1:99?99 AND (FIELD2:1111* OR FIELD3:AAAA)"));
        Assert.assertEquals("FIELD1 =~ '99.99' && (FIELD2 =~ '1111.*?' || FIELD3 == 'AAAA') && !(FIELD4 == '1234')",
                        parseQuery("FIELD1:99?99 AND (FIELD2:1111* OR FIELD3:AAAA) NOT FIELD4:1234"));
        Assert.assertEquals("A == '1' && B == '2' && C == '3' && !(D == '4')", parseQuery("A:1 B:2 C:3 NOT D:4"));
        Assert.assertEquals("(A == '1' && B == '2' && C == '3') && !(D == '4')", parseQuery("(A:1 B:2 C:3) NOT D:4"));
        
        Assert.assertEquals("A =~ '11\\u005c.22.*?'", parseQuery("A:11.22*"));
        Assert.assertEquals("A =~ '11\\u005c\\u005c22.*?'", parseQuery("A:11\\\\22*"));
        Assert.assertEquals("A =~ 'TESTING.1\\u005c\\u005c22.*?'", parseQuery("A:TESTING?1\\\\22*"));
    }
    
    @Test
    public void testNot() throws ParseException {
        
        Assert.assertEquals("(F1 == 'A' && F2 == 'B') && !(F3 == 'C') && !(F4 == 'D')", parseQuery("(F1:A AND F2:B) NOT F3:C NOT F4:D"));
        Assert.assertEquals("(F1 == 'A' && F2 == 'B' || F3 == 'C') && !(F4 == 'D') && !(F5 == 'E') && !(F6 == 'F')",
                        parseQuery("(F1:A AND F2:B OR F3:C) NOT F4:D NOT F5:E NOT F6:F"));
    }
    
    @Test
    public void testTokenizedFieldNoAnalyzer() throws ParseException {
        parser.setAnalyzer(null);
        Assert.assertEquals("TOKFIELD == '1234 5678 to bird'", parseQuery("TOKFIELD:1234\\ 5678\\ to\\ bird"));
        Assert.assertEquals("TOKFIELD == '1234 5678 to bird'", parseQuery("TOKFIELD:\"1234\\ 5678\\ to\\ bird\""));
    }
    
    @Test
    public void testTokenizedFieldMixedCase() throws ParseException {
        Assert.assertEquals("TOKFIELD == 'BIRD'", parseQuery("TOKFIELD:BIRD"));
    }
    
    @Test
    public void testTokenizedFieldUnfieldedEnabled() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        Assert.assertEquals("(_ANYFIELD_ == '1234 5678 to bird' || content:within(4, termOffsetMap, '1234', '5678', 'bird'))",
                        parseQuery("1234\\ 5678\\ to\\ bird"));
        Assert.assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testTokenizedUnfieldedTerm() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        // verify that new slop is calculated properly.
        parser.setUseSlopForTokenizedTerms(false);
        Assert.assertEquals("(_ANYFIELD_ == 'wi-fi' || content:phrase(termOffsetMap, 'wi', 'fi'))", parseQuery("wi-fi"));
        parser.setUseSlopForTokenizedTerms(true);
        Assert.assertEquals("(_ANYFIELD_ == 'wi-fi' || content:within(2, termOffsetMap, 'wi', 'fi'))", parseQuery("wi-fi"));
    }
    
    @Test
    public void testTokenizedUnfieldedSlopPhrase() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        // verify that new slop is calculated properly.
        Assert.assertEquals("content:within(3, termOffsetMap, 'portable', 'document')", parseQuery("\"portable document\"~3"));
        Assert.assertEquals(
                        "(content:within(5, termOffsetMap, 'portable', 'wi-fi', 'access-point') || content:within(7, termOffsetMap, 'portable', 'wi', 'fi', 'access', 'point'))",
                        parseQuery("\"portable wi-fi access-point\"~5"));
    }
    
    @Test
    public void testTokenizedUnfieldedPhrase() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        // verifies that the slop setting has no impact on things that come in as phrases without slop.
        parser.setUseSlopForTokenizedTerms(false);
        Assert.assertEquals("content:phrase(termOffsetMap, 'portable', 'document')", parseQuery("\"portable document\""));
        Assert.assertEquals(
                        "(content:phrase(termOffsetMap, 'portable', 'wi-fi', 'access-point') || content:phrase(termOffsetMap, 'portable', 'wi', 'fi', 'access', 'point'))",
                        parseQuery("\"portable wi-fi access-point\""));
        parser.setUseSlopForTokenizedTerms(true);
        Assert.assertEquals("content:phrase(termOffsetMap, 'portable', 'document')", parseQuery("\"portable document\""));
        Assert.assertEquals(
                        "(content:phrase(termOffsetMap, 'portable', 'wi-fi', 'access-point') || content:phrase(termOffsetMap, 'portable', 'wi', 'fi', 'access', 'point'))",
                        parseQuery("\"portable wi-fi access-point\""));
    }
    
    @Test
    public void testTokenizedFieldUnfieldedEnabledNoAnalyzer() throws ParseException {
        parser.setTokenizeUnfieldedQueries(true);
        parser.setAnalyzer(null);
        Assert.assertEquals("_ANYFIELD_ == '1234 5678 to bird'", parseQuery("1234\\ 5678\\ to\\ bird"));
        Assert.assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testTokenizedFieldUnfieldedDisabled() throws ParseException {
        parser.setTokenizeUnfieldedQueries(false);
        Assert.assertEquals("_ANYFIELD_ == '1234 5678 to bird'", parseQuery("1234\\ 5678\\ to\\ bird"));
        Assert.assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testSkipTokenizedFields() throws ParseException {
        parser.setTokenizeUnfieldedQueries(false);
        Assert.assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
        // test for case independence of field name
        Assert.assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("notoken:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testSkipTokenizedFieldsNoAnalyzer() throws ParseException {
        parser.setAnalyzer(null);
        Assert.assertEquals(Constants.ANY_FIELD + " == '5678 1234 to bird'", parseQuery("NOTOKEN:5678\\ 1234\\ to\\ bird"));
    }
    
    @Test
    public void testTokenizedPhraseFail() throws ParseException {
        Assert.assertEquals("(TOKFIELD == 'joh.nny chicken' || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:\"joh.nny\\ chicken\""));
    }
    
    @Test
    public void testWildcards() throws ParseException {
        Assert.assertEquals("F1 == '*1234'", parseQuery("F1:\\*1234"));
        Assert.assertEquals("F1 =~ '\\u005c*12.34'", parseQuery("F1:\\*12?34"));
        
        Assert.assertEquals("F1 == '.1234'", parseQuery("F1:\\.1234"));
        Assert.assertEquals("F1 =~ '\\u005c.12.34'", parseQuery("F1:\\.12?34"));
    }
    
    @Test
    public void testRanges() throws ParseException {
        Assert.assertEquals("(fieldName >= 'aaa' && fieldName <= 'bbb')", parseQuery("fieldName:[aaa TO bbb]"));
        Assert.assertEquals("(fieldName > 'aaa' && fieldName < 'bbb')", parseQuery("fieldName:{aaa TO bbb}"));
        Assert.assertEquals("fieldName1 == 'value1' && (fieldName2 >= 'aaa' && fieldName2 <= 'bbb') && fieldName3 == 'value3'",
                        parseQuery("fieldName1:value1 AND fieldName2:[aaa TO bbb] AND fieldName3:value3"));
        Assert.assertEquals("(F >= 'A' && F <= 'B')", parseQuery("F:[A TO B]"));
        Assert.assertEquals("(F >= 'A\\u005c*' && F <= 'B')", parseQuery("F:[A\\\\\\* TO B]"));
        Assert.assertEquals("(F > 'lower' && F <= 'upper')", parseQuery("F:{lower TO upper]"));
        Assert.assertEquals("(F >= 'lower' && F < 'upper')", parseQuery("F:[lower TO upper}"));
    }
    
    @Test
    public void testPhrases() throws ParseException {
        Assert.assertEquals("content:phrase(termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("\"quick brown fox\""));
        Assert.assertEquals("content:phrase(TOKFIELD, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("TOKFIELD:\"quick brown fox\""));
        
        // testing case independence of "TOKFIELD" This would return content:phrase if it were not case independent
        Assert.assertEquals("content:phrase(tokfield, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("tokfield:\"quick brown fox\""));
        
        Assert.assertEquals("TOKFIELD == 'value' && content:phrase(termOffsetMap, 'quick', 'brown', 'fox')",
                        parseQuery("TOKFIELD:value AND \"quick brown fox\""));
        Assert.assertEquals(anyField + " == 'quick'", parseQuery("\"quick\""));
        Assert.assertEquals("content:phrase(termOffsetMap, 'qui.ck', 'brown', 'fox')", parseQuery("\"qui\\.ck brown fox\""));
        
        // Test smart quote conversions
        Assert.assertEquals("content:phrase(termOffsetMap, 'quick', 'brown', 'fox1')", parseQuery("\u0093quick brown fox1\u0094"));
        Assert.assertEquals("content:phrase(termOffsetMap, 'quick', 'brown', 'fox2')", parseQuery("\u201cquick brown fox2\u201d"));
    }
    
    @Test
    public void testMultiWordPhraseCompatability() throws ParseException {
        Assert.assertEquals(anyField + " == 'alpha beta'", parseQuery("alpha\\ beta"));
        Assert.assertEquals(anyField + " == 'alpha beta'", parseQuery("\"alpha\\ beta\""));
        Assert.assertEquals(anyField + " == 'alpha beta' && " + anyField + " == 'gamma'", parseQuery("\"alpha\\ beta\" gamma"));
        Assert.assertEquals("content:phrase(termOffsetMap, 'alpha beta', 'gamma')", parseQuery("\"alpha\\ beta gamma\""));
    }
    
    @Test
    public void testMiscEscape() throws ParseException {
        // apostrophe escapes
        Assert.assertEquals(anyField + " == 'johnny\\'s'", parseQuery("johnny's"));
        Assert.assertEquals(anyField + " == 'johnny\\'s'", parseQuery("johnny\\'s")); // lucene drops single slash
        Assert.assertEquals(anyField + " == 'johnny\\'s'", parseQuery("\"johnny's\""));
        Assert.assertEquals("content:phrase(termOffsetMap, 'johnny\\'s', 'chicken')", parseQuery("\"johnny's chicken\""));
        Assert.assertEquals("A =~ '11\\'22.*?'", parseQuery("A:11'22*"));
        //
        // // F:[A'\\\* TO B'], searching from A'\\* ("A" followed by a single quote, a backslash, and an asterisk to "B" followed by a single quote
        Assert.assertEquals("(F >= 'A\\'\\u005c*' && F <= 'B\\'')", parseQuery("F:[A'\\\\\\* TO B']"));
        //
        // // space escapes
        Assert.assertEquals(anyField + " == 'joh.nny chicken'", parseQuery("joh.nny\\ chicken"));
        Assert.assertEquals(anyField + " =~ 'joh\\u005c.nny chick.*?'", parseQuery("joh.nny\\ chick*"));
        Assert.assertEquals(anyField + " == 'joh.nny chicken'", parseQuery("\"joh.nny\\ chicken\""));
        Assert.assertEquals("content:phrase(termOffsetMap, 'joh.nny', 'chic ken')", parseQuery("\"joh.nny chic\\ ken\""));
        Assert.assertEquals("content:phrase(termOffsetMap, 'joh.nny', 'chic k*')", parseQuery("\"joh.nny chic\\ k*\""));
        
        // quote escapes
        Assert.assertEquals("content:phrase(termOffsetMap, 'joh.nny', '\"chicken\"')", parseQuery("\"joh.nny \\\"chicken\\\"\""));
        
        // unicode escapes.
        Assert.assertEquals("FIELD =~ 'joh\\u005c.nny.*?\\u005c\\u005c''", parseQuery("FIELD:joh.nny*\\\\'"));
        Assert.assertEquals("FIELD =~ 'joh\\u005c.nny\\u005c\\u005cu005cfive.*?\\''", parseQuery("FIELD:joh.nny\\\\u005cfive*'"));
        
        // literal blackslashes
        Assert.assertEquals(anyField + " == 'yep\\u005cyep'", parseQuery("yep\\\\yep"));
        Assert.assertEquals("content:phrase(termOffsetMap, 'yep\\u005cyep', 'otherterm')", parseQuery("\"yep\\\\yep otherterm\""));
    }
    
    @Test
    public void testMiscEscapeTokenization() throws ParseException {
        // apostrophe escapes
        Assert.assertEquals("(TOKFIELD == 'johnny\\'s' || TOKFIELD == 'johnny')", parseQuery("TOKFIELD:johnny's"));
        Assert.assertEquals("(TOKFIELD == 'johnny\\'s' || TOKFIELD == 'johnny')", parseQuery("TOKFIELD:johnny\\'s")); // lucene drops single slash
        Assert.assertEquals("(TOKFIELD == 'johnny\\'s' || TOKFIELD == 'johnny')", parseQuery("TOKFIELD:\"johnny's\""));
        Assert.assertEquals(
                        "(content:phrase(TOKFIELD, termOffsetMap, 'johnny\\'s', 'chicken') || content:phrase(TOKFIELD, termOffsetMap, 'johnny', 'chicken'))",
                        parseQuery("TOKFIELD:\"johnny's chicken\""));
        Assert.assertEquals("TOKFIELD =~ '11\\'22.*?'", parseQuery("TOKFIELD:11'22*"));
        Assert.assertEquals("(TOKFIELD >= 'A\\'\\u005c*' && TOKFIELD <= 'B\\'')", parseQuery("TOKFIELD:[A'\\\\\\* TO B']"));
        
        // space escapes
        Assert.assertEquals("(TOKFIELD == 'joh.nny chicken' || content:within(TOKFIELD, 2, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:joh.nny\\ chicken"));
        Assert.assertEquals("TOKFIELD =~ 'joh\\u005c.nny chick.*?'", parseQuery("TOKFIELD:joh.nny\\ chick*"));
        Assert.assertEquals("(TOKFIELD == 'joh.nny chicken' || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:\"joh.nny\\ chicken\""));
        
        Assert.assertEquals(
                        "(content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic ken') || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic', 'ken'))",
                        parseQuery("TOKFIELD:\"joh.nny chic\\ ken\""));
        Assert.assertEquals(
                        "(content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic k*') || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chic', 'k'))",
                        parseQuery("TOKFIELD:\"joh.nny chic\\ k*\""));
        
        // quote escapes
        Assert.assertEquals(
                        "(content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', '\"chicken\"') || content:phrase(TOKFIELD, termOffsetMap, 'joh.nny', 'chicken'))",
                        parseQuery("TOKFIELD:\"joh.nny \\\"chicken\\\"\""));
        
        // unicode escapes.
        Assert.assertEquals("TOKFIELD =~ 'joh\\u005c.nny.*?\\u005c\\u005c''", parseQuery("TOKFIELD:joh.nny*\\\\'"));
        Assert.assertEquals("TOKFIELD =~ 'joh\\u005c.nny\\u005c\\u005cu005cfive.*?\\''", parseQuery("TOKFIELD:joh.nny\\\\u005cfive*'"));
        
        // literal blackslashes
        Assert.assertEquals("(TOKFIELD == 'someone\\u005c234someplace.com' || content:within(TOKFIELD, 2, termOffsetMap, 'someone', '234someplace.com'))",
                        parseQuery("TOKFIELD:someone\\\\234someplace.com"));
        Assert.assertEquals(
                        "(content:phrase(TOKFIELD, termOffsetMap, 'someone\\u005c234someplace.com', 'otherterm') || content:phrase(TOKFIELD, termOffsetMap, 'someone', '234someplace.com', 'otherterm'))",
                        parseQuery("TOKFIELD:\"someone\\\\234someplace.com otherterm\""));
        // FIX THIS, trailing slashes in queries should be acceptable.
        // Assert.assertEquals("(TOKFIELD == 'someone\\u005c234someplace.com\\u005c' || content:within(TOKFIELD, 2, termOffsetMap, 'someone',
        // '234someplace.com'))",
        // parseQuery("TOKFIELD:someone\\\\234someplace.com\\\\"));
    }
    
    @Test
    public void testWithin() throws ParseException {
        Assert.assertEquals("content:within(10, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("\"quick brown fox\"~10"));
        Assert.assertEquals("content:within(FIELD, 20, termOffsetMap, 'quick', 'brown', 'fox')", parseQuery("FIELD:\"quick brown fox\"~20"));
        Assert.assertEquals("FIELD == 'value' && content:within(15, termOffsetMap, 'quick', 'brown', 'fox')",
                        parseQuery("FIELD:value AND \"quick brown fox\"~15"));
        Assert.assertEquals(anyField + " == 'quick'", parseQuery("\"quick\"~10"));
    }
    
    @Test
    public void testUnbalancedParens() throws ParseException {
        Assert.assertEquals("(FIELD == 'value') && (content:phrase(termOffsetMap, 'term1', 'term2'))", parseQuery("(FIELD:value) AND (\"term1 term2\")"));
        Assert.assertEquals("content:phrase(termOffsetMap, 'term1', 'term2', 'term3') && " + anyField + " == 'term4'",
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
        Assert.assertEquals("fielda == 'selectora' && " + jexl, parseQuery("fielda:selectora " + lucene));
        Assert.assertEquals("fielda == 'selectora' && fieldb == 'selectorb' && " + jexl, parseQuery("fielda:selectora fieldb:selectorb " + lucene));
        Assert.assertEquals("(fielda == 'selectora' || fieldb == 'selectorb' || fieldc == 'selectorc') && " + jexl,
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
        Assert.assertEquals("FOO.1 == 'ABC' && FOO.2 == 'DEF'", parseQuery(s));
    }
    
    @Test
    public void testSmartQuoteReplacement() throws ParseException {
        String s = "\u0093see jane run\u0094";
        Assert.assertEquals("content:phrase(termOffsetMap, 'see', 'jane', 'run')", parseQuery(s));
    }
    
    @Test
    public void testParseSpaces() throws ParseException {
        Assert.assertEquals("FIELD == '  '", parseQuery("FIELD:\\ \\ "));
        Assert.assertEquals("FIELD == '  TEST'", parseQuery("FIELD:\\ \\ TEST"));
        Assert.assertEquals("FIELD == 'TEST  '", parseQuery("FIELD:TEST\\ \\ "));
    }
    
    @Test
    public void testIsNotNull() throws ParseException {
        Assert.assertEquals("F == 'S' && not(filter:isNull(FOO))", parser.parse("F:S #ISNOTNULL(FOO)").getOriginalQuery());
    }
    
    @Test
    public void testRegex() throws ParseException {
        Assert.assertEquals("F =~ 'test.[abc]?ing'", parser.parse("F:/test.[abc]?ing/").getOriginalQuery());
        Assert.assertEquals("F =~ 'test./[abc]?ing'", parser.parse("F:/test.\\/[abc]?ing/").getOriginalQuery());
        Assert.assertEquals("F =~ 'test.*[^abc]?ing'", parser.parse("F:/test.*[^abc]?ing/").getOriginalQuery());
        Assert.assertEquals("F == '/test/file/path'", parser.parse("F:\"/test/file/path\"").getOriginalQuery());
    }
    
    @Test
    public void testOpenEndedRanges() throws ParseException {
        Assert.assertEquals("(F >= 'lower')", parser.parse("F:[lower TO *]").getOriginalQuery());
        Assert.assertEquals("(F <= 'upper')", parser.parse("F:[* TO upper]").getOriginalQuery());
        Assert.assertEquals("(F > 'lower')", parser.parse("F:{lower TO *}").getOriginalQuery());
        Assert.assertEquals("(F < 'upper')", parser.parse("F:{* TO upper}").getOriginalQuery());
        Assert.assertEquals("(LO <= '2') && (HI >= '3') && FIELD == 'foobar'", parser.parse("LO:[* TO 2] HI:[3 TO *] FIELD:foobar").getOriginalQuery());
    }
    
    @Test
    public void testNumericRange() throws ParseException {
        Assert.assertEquals("(FOO >= '1' && FOO <= '5000')", parser.parse("FOO:[1 TO 5000]").getOriginalQuery());
    }
    
    @Test
    public void testParseRegexsWithEscapes() throws ParseException {
        Assert.assertEquals("F == '6515' && FOOBAR =~ 'Foo/5\\u005c.0 \\u005c(ibar.*?'", parser.parse("F:6515 and FOOBAR:Foo\\/5.0\\ \\(ibar*")
                        .getOriginalQuery());
        Assert.assertEquals("FOO == 'bar' && BAZ =~ 'Foo Foo Foo.*?'", parser.parse("FOO:bar BAZ:Foo\\ Foo\\ Foo*").getOriginalQuery());
        Assert.assertEquals("FOO == 'bar' && BAZ =~ 'Foo Foo Foo.*'", parser.parse("FOO:bar BAZ:/Foo Foo Foo.*/").getOriginalQuery());
        Assert.assertEquals("FOO == 'bar' && BAZ =~ 'Foo Foo Foo.*?'", parser.parse("FOO:bar BAZ:/Foo Foo Foo.*?/").getOriginalQuery());
        
        Assert.assertEquals("FOO == 'bar' && BAZ =~ 'Foo/Foo Foo.*?'", parser.parse("FOO:bar BAZ:Foo\\/Foo\\ Foo*").getOriginalQuery());
        Assert.assertEquals("FOO == 'bar' && BAZ =~ 'Foo/Foo\\ Foo.*'", parser.parse("FOO:bar BAZ:/Foo\\/Foo\\ Foo.*/").getOriginalQuery());
        Assert.assertEquals("FOO == 'bar' && BAZ =~ 'Foo/Foo Foo.*?'", parser.parse("FOO:bar BAZ:/Foo\\/Foo Foo.*?/").getOriginalQuery());
    }
    
}
