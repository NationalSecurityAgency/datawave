package datawave.query.jexl.visitors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;

public class UnescapedSpecialCharactersVisitorTest {

    private String query;
    private final Set<Character> literalExceptions = new HashSet<>();
    private boolean escapedWhitespaceRequiredForLiterals;
    private final Set<Character> patternExceptions = new HashSet<>();
    private boolean escapedWhitespaceRequiredForPatterns;

    private final Multimap<String,Character> expectedLiterals = HashMultimap.create();
    private final Multimap<String,Character> expectedPatterns = HashMultimap.create();

    @After
    public void tearDown() throws Exception {
        query = null;
        clearExpected();
        escapedWhitespaceRequiredForLiterals = false;
        escapedWhitespaceRequiredForPatterns = false;
    }

    /**
     * Test a literal and pattern that contain alphanumeric characters only.
     */
    @Test
    public void testNoSpecialCharacters() throws ParseException {
        String alphanumericStr = ofChars('a', 'z') + ofChars('A', 'Z') + ofChars('0', '9');
        givenQuery("FOO == '" + alphanumericStr + "' || FOO =~ '" + alphanumericStr + "'");

        assertResults();
    }

    private String ofChars(char start, char end) {
        return IntStream.rangeClosed(start, end).mapToObj(c -> "" + ((char) c)).collect(Collectors.joining());
    }

    /**
     * Test a literal and pattern with whitespace in them and do not allow unescaped whitespace.
     */
    @Test
    public void testUnescapedWhitespace() throws ParseException {
        givenEscapedWhitespaceRequiredForLiterals(true);
        givenEscapedWhitespaceRequiredForPatterns(true);
        givenQuery("FOO == 'ab c' || FOO =~ 'ab cd'");

        expectCharsForLiterals("ab c", ' ');
        expectCharsForPatterns("ab cd", ' ');

        assertResults();
    }

    /**
     * Test a literal and pattern with whitespace in them and allow unescaped whitespace.
     */
    @Test
    public void testUnescapedWhitespaceAllowed() throws ParseException {
        givenEscapedWhitespaceRequiredForLiterals(false);
        givenEscapedWhitespaceRequiredForPatterns(false);
        givenQuery("FOO == 'ab c' || FOO =~ 'ab cd'");

        // Do not expect any unescaped chars.

        assertResults();
    }

    /**
     * Test literals and patterns with unescaped special chars at the start, in the middle, and at the end.
     */
    @Test
    public void testUnescapedSpecialCharAtDifferentIndexes() throws ParseException {
        // Test special chars at the start of the string.
        givenQuery("FOO == '&abc' || FOO =~ '&abc'");
        expectCharsForLiterals("&abc", '&');
        expectCharsForPatterns("&abc", '&');
        assertResults();

        // Test special chars in the middle of the string.
        givenQuery("FOO == 'a&bc' || FOO =~ 'a&bc'");
        clearExpected();
        expectCharsForLiterals("a&bc", '&');
        expectCharsForPatterns("a&bc", '&');
        assertResults();

        // Test special chars at the end of the string.
        givenQuery("FOO == 'abc&' || FOO =~ 'abc&'");
        clearExpected();
        expectCharsForLiterals("abc&", '&');
        expectCharsForPatterns("abc&", '&');
        assertResults();
    }

    /**
     * Test a literal and pattern with a special character that is allowed to be escaped.
     */
    @Test
    public void testSpecialCharThatIsException() throws ParseException {
        givenLiteralExceptions('&');
        givenPatternExceptions('&');
        givenQuery("FOO == 'ab&c' || FOO =~ 'ab&d'");

        // Do not expect any unescaped chars.

        assertResults();
    }

    /**
     * Test a literal and pattern with a special character that is not an exception and is escaped.
     */
    @Test
    public void testEscapedSpecialChar() throws ParseException {
        givenQuery("FOO == 'ab\\&c' || FOO =~ 'ab\\&d'");

        // Do not expect any unescaped chars.
        assertResults();
    }

    /**
     * Test that when we see a double backslash, it does not escape any special characters directly after it.
     */
    @Test
    public void testDoubleBackslashDoesNotEscapeCharacter() throws ParseException {
        // Backslashes must be doubly escaped in literals, but not patterns when parsed to JEXL.
        givenQuery("FOO == 'ab\\\\\\\\&c' || FOO =~ 'ab\\\\&d'");
        expectCharsForLiterals("ab\\\\&c", '&');
        expectCharsForPatterns("ab\\\\&d", '&');

        assertResults();
    }

    /**
     * Test that when we see a triple backslash, the last backslash escapes a special characters directly after it.
     */
    @Test
    public void testTripleBackslashEscapesCharacter() throws ParseException {
        // Backslashes must be doubly escaped in literals, but not patterns when parsed to JEXL.
        givenQuery("FOO == 'ab\\\\\\\\\\\\&c' || FOO =~ 'ab\\\\\\&d'");

        // Do not expect any unescaped chars.
        assertResults();
    }

    /**
     * Test that an unescaped backlash in a literal will be noted, but not an unescaped backslash in a pattern since it is a regex-reserved char.
     */
    @Test
    public void testUnescapedBackslashInLiteral() throws ParseException {
        // Backslashes must be doubly escaped in literals, but not patterns when parsed to JEXL.
        givenQuery("FOO == '\\\\' || FOO =~ '\\\\'");
        clearExpected();
        expectCharsForLiterals("\\", '\\');

        assertResults();
    }

    /**
     * Test that regex-reserved characters do not get flagged as unescaped special characters in patterns.
     */
    @Test
    public void testRegexReservedCharacters() throws ParseException {
        // This is not a valid pattern, but patterns are not compiled in the visitor, so an exception will not be thrown.
        givenQuery("FOO =~ '.+*?^$()[]{}|\\\\'");

        // Do not expect any unescaped characters.
        assertResults();
    }

    /**
     * Test empty strings will not result in flagged special characters.
     */
    @Test
    public void testEmptyStrings() throws ParseException {
        givenQuery("FOO == '' || FOO =~ ''");

        // Do not expect any unescaped chars.
        assertResults();
    }

    /**
     * Test that regex patterns inside of ER, NR, and function nodes are evaluated.
     */
    @Test
    public void testPossiblePatternLocations() throws ParseException {
        givenQuery("FOO =~ 'er&' && FOO !~ 'nr&' && filter:includeRegex(FOO, 'function&')");
        expectCharsForPatterns("er&", '&');
        expectCharsForPatterns("nr&", '&');
        expectCharsForPatterns("function&", '&');

        assertResults();
    }

    /**
     * Test that literal strings for EQ, NE, LT, GT, LE, and GE nodes are evaluated.
     */
    @Test
    public void testPossibleLiteralLocations() throws ParseException {
        givenQuery("FOO == 'eq&' || FOO != 'ne&' || FOO < 'lt&' || FOO > 'gt&' || FOO <= 'le&' || FOO >= 'ge&'");
        expectCharsForLiterals("eq&", '&');
        expectCharsForLiterals("ne&", '&');
        expectCharsForLiterals("lt&", '&');
        expectCharsForLiterals("gt&", '&');
        expectCharsForLiterals("le&", '&');
        expectCharsForLiterals("ge&", '&');

        assertResults();
    }

    @Test
    public void testMultipleSpecialCharactersFound() throws ParseException {
        givenQuery("FOO == 'ab^123%34#' || FOO =~ '343&kje:jd@'");
        expectCharsForLiterals("ab^123%34#", '^', '%', '#');
        expectCharsForPatterns("343&kje:jd@", '&', ':', '@');

        assertResults();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenLiteralExceptions(Character... chars) {
        this.literalExceptions.addAll(List.of(chars));
    }

    private void givenEscapedWhitespaceRequiredForLiterals(boolean bool) {
        this.escapedWhitespaceRequiredForLiterals = bool;
    }

    private void givenPatternExceptions(Character... chars) {
        this.patternExceptions.addAll(List.of(chars));
    }

    private void givenEscapedWhitespaceRequiredForPatterns(boolean bool) {
        this.escapedWhitespaceRequiredForPatterns = bool;
    }

    private void expectCharsForLiterals(String literal, Character... characters) {
        expectedLiterals.putAll(literal, List.of(characters));
    }

    private void expectCharsForPatterns(String pattern, Character... characters) {
        expectedPatterns.putAll(pattern, List.of(characters));
    }

    private void clearExpected() {
        this.expectedLiterals.clear();
        this.expectedPatterns.clear();
    }

    private void assertResults() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        UnescapedSpecialCharactersVisitor visitor = UnescapedSpecialCharactersVisitor.check(script, literalExceptions, escapedWhitespaceRequiredForLiterals,
                        patternExceptions, escapedWhitespaceRequiredForPatterns);
        Multimap<String,Character> actualLiterals = visitor.getUnescapedCharactersInLiterals();
        Multimap<String,Character> actualPatterns = visitor.getUnescapedCharactersInPatterns();
        Assert.assertEquals("Unescaped chars for literals did not match expected", expectedLiterals, actualLiterals);
        Assert.assertEquals("Unescaped chars for patterns did not match expected", expectedPatterns, actualPatterns);
    }
}
