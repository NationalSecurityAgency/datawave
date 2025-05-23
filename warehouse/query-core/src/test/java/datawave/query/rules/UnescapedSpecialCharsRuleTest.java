package datawave.query.rules;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UnescapedSpecialCharsRuleTest extends ShardQueryRuleTest {

    private final Set<Character> literalExceptions = new HashSet<>();
    private boolean escapedWhitespaceRequiredForLiterals;
    private final Set<Character> patternExceptions = new HashSet<>();
    private boolean escapedWhitespaceRequiredForPatterns;

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
        literalExceptions.clear();
        patternExceptions.clear();
        escapedWhitespaceRequiredForLiterals = false;
        escapedWhitespaceRequiredForPatterns = false;
    }

    /**
     * Test a literal and pattern that contain alphanumeric characters only.
     */
    @Test
    public void testNoSpecialCharacters() throws Exception {
        String alphanumericStr = ofChars('a', 'z') + ofChars('A', 'Z') + ofChars('0', '9');
        givenQuery("FOO == '" + alphanumericStr + "' || FOO =~ '" + alphanumericStr + "'");

        assertResult();
    }

    private String ofChars(char start, char end) {
        return IntStream.rangeClosed(start, end).mapToObj(c -> "" + ((char) c)).collect(Collectors.joining());
    }

    /**
     * Test a literal and pattern with whitespace in them and do not allow unescaped whitespace.
     */
    @Test
    public void testUnescapedWhitespace() throws Exception {
        givenEscapedWhitespaceRequiredForLiterals(true);
        givenEscapedWhitespaceRequiredForPatterns(true);
        givenQuery("FOO == 'ab c' || FOO =~ 'ab cd'");

        expectMessage("Literal string \"ab c\" has the following unescaped special character(s): ' '");
        expectMessage("Regex pattern \"ab cd\" has the following unescaped special character(s): ' '");
        assertResult();
    }

    /**
     * Test a literal and pattern with whitespace in them and allow unescaped whitespace.
     */
    @Test
    public void testUnescapedWhitespaceAllowed() throws Exception {
        givenEscapedWhitespaceRequiredForLiterals(false);
        givenEscapedWhitespaceRequiredForPatterns(false);
        givenQuery("FOO == 'ab c' || FOO =~ 'ab cd'");

        // Do not expect any unescaped chars.

        assertResult();
    }

    /**
     * Test literals and patterns with unescaped special chars at the start of the string.
     */
    @Test
    public void testUnescapedSpecialCharAtStart() throws Exception {
        givenQuery("FOO == '&abc' || FOO =~ '&abc'");
        expectMessage("Literal string \"&abc\" has the following unescaped special character(s): '&'");
        expectMessage("Regex pattern \"&abc\" has the following unescaped special character(s): '&'");
        assertResult();
    }

    /**
     * Test literals and patterns with unescaped special chars in the middle of the string.
     */
    @Test
    public void testUnescapedSpecialCharInMiddle() throws Exception {
        givenQuery("FOO == 'a&bc' || FOO =~ 'a&bc'");
        expectMessage("Literal string \"a&bc\" has the following unescaped special character(s): '&'");
        expectMessage("Regex pattern \"a&bc\" has the following unescaped special character(s): '&'");
        assertResult();
    }

    /**
     * Test literals and patterns with unescaped special chars at the end of the string.
     */
    @Test
    public void testUnescapedSpecialCharAtEnd() throws Exception {
        givenQuery("FOO == 'abc&' || FOO =~ 'abc&'");
        expectMessage("Literal string \"abc&\" has the following unescaped special character(s): '&'");
        expectMessage("Regex pattern \"abc&\" has the following unescaped special character(s): '&'");
        assertResult();
    }

    /**
     * Test a literal and pattern with a special character that is allowed to be escaped.
     */
    @Test
    public void testSpecialCharThatIsException() throws Exception {
        givenLiteralExceptions('&');
        givenPatternExceptions('&');
        givenQuery("FOO == 'ab&c' || FOO =~ 'ab&d'");

        // Do not expect any unescaped chars.

        assertResult();
    }

    /**
     * Test a literal and pattern with a special character that is not an exception and is escaped.
     */
    @Test
    public void testEscapedSpecialChar() throws Exception {
        givenQuery("FOO == 'ab\\&c' || FOO =~ 'ab\\&d'");

        // Do not expect any unescaped chars.
        assertResult();
    }

    /**
     * Test that when we see a double backslash, it does not escape any special characters directly after it.
     */
    @Test
    public void testDoubleBackslashDoesNotEscapeCharacter() throws Exception {
        // Backslashes must be doubly escaped in literals, but not patterns when parsed to JEXL.
        givenQuery("FOO == 'ab\\\\\\\\&c' || FOO =~ 'ab\\\\&d'");
        expectMessage("Literal string \"ab\\\\&c\" has the following unescaped special character(s): '&'");
        expectMessage("Regex pattern \"ab\\\\&d\" has the following unescaped special character(s): '&'");
        assertResult();
    }

    /**
     * Test that when we see a triple backslash, the last backslash escapes a special characters directly after it.
     */
    @Test
    public void testTripleBackslashEscapesCharacter() throws Exception {
        // Backslashes must be doubly escaped in literals, but not patterns when parsed to JEXL.
        givenQuery("FOO == 'ab\\\\\\\\\\\\&c' || FOO =~ 'ab\\\\\\&d'");

        // Do not expect any unescaped chars.
        assertResult();
    }

    /**
     * Test that an unescaped backlash in a literal will be noted, but not an unescaped backslash in a pattern since it is a regex-reserved char.
     */
    @Test
    public void testUnescapedBackslashInLiteral() throws Exception {
        // Backslashes must be doubly escaped in literals, but not patterns when parsed to JEXL.
        givenQuery("FOO == '\\\\' || FOO =~ '\\\\'");
        expectMessage("Literal string \"\\\" has the following unescaped special character(s): '\\'");
        assertResult();
    }

    /**
     * Test that regex-reserved characters do not get flagged as unescaped special characters in patterns.
     */
    @Test
    public void testRegexReservedCharacters() throws Exception {
        // This is not a valid pattern, but patterns are not compiled in the visitor, so an exception will not be thrown.
        givenQuery("FOO =~ '.+*?^$()[]{}|\\\\'");

        // Do not expect any unescaped characters.
        assertResult();
    }

    /**
     * Test empty strings will not result in flagged special characters.
     */
    @Test
    public void testEmptyStrings() throws Exception {
        givenQuery("FOO == '' || FOO =~ ''");

        // Do not expect any unescaped chars.
        assertResult();
    }

    /**
     * Test that regex patterns inside of ER, NR, and function nodes are evaluated.
     */
    @Test
    public void testPossiblePatternLocations() throws Exception {
        givenQuery("FOO =~ 'er&' && FOO !~ 'nr&' && filter:includeRegex(FOO, 'function&')");
        expectMessage("Regex pattern \"er&\" has the following unescaped special character(s): '&'");
        expectMessage("Regex pattern \"nr&\" has the following unescaped special character(s): '&'");
        expectMessage("Regex pattern \"function&\" has the following unescaped special character(s): '&'");
        assertResult();
    }

    /**
     * Test that literal strings for EQ, NE, LT, GT, LE, and GE nodes are evaluated.
     */
    @Test
    public void testPossibleLiteralLocations() throws Exception {
        givenQuery("FOO == 'eq&' || FOO != 'ne&' || FOO < 'lt&' || FOO > 'gt&' || FOO <= 'le&' || FOO >= 'ge&'");
        expectMessage("Literal string \"eq&\" has the following unescaped special character(s): '&'");
        expectMessage("Literal string \"ne&\" has the following unescaped special character(s): '&'");
        expectMessage("Literal string \"lt&\" has the following unescaped special character(s): '&'");
        expectMessage("Literal string \"gt&\" has the following unescaped special character(s): '&'");
        expectMessage("Literal string \"le&\" has the following unescaped special character(s): '&'");
        expectMessage("Literal string \"ge&\" has the following unescaped special character(s): '&'");
        assertResult();
    }

    @Test
    public void testMultipleSpecialCharactersFound() throws Exception {
        givenQuery("FOO == 'ab^123%34#' || FOO =~ '343&kje:jd@'");
        expectMessage("Literal string \"ab^123%34#\" has the following unescaped special character(s): '^', '%', '#'");
        expectMessage("Regex pattern \"343&kje:jd@\" has the following unescaped special character(s): '&', ':', '@'");
        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        UnescapedSpecialCharsRule rule = new UnescapedSpecialCharsRule(ruleName);
        rule.setLiteralExceptions(literalExceptions);
        rule.setEscapedWhitespaceRequiredForLiterals(escapedWhitespaceRequiredForLiterals);
        rule.setPatternExceptions(patternExceptions);
        rule.setEscapedWhitespaceRequiredForPatterns(escapedWhitespaceRequiredForPatterns);
        return rule;
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

}
