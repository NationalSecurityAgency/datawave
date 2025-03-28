package datawave.data.normalizer.regex;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import datawave.data.type.util.NumericalEncoder;

class NumericRegexEncoderTest {
    
    private static final List<String> letters = new ArrayList<>();
    
    @BeforeAll
    static void beforeAll() {
        letters.addAll(generateLetters('a', 'z'));
        letters.addAll(generateLetters('A', 'Z'));
    }
    
    /**
     * Return an unmodifiable list of letters in order from the given starting letter to the given ending letter.
     *
     * @param start
     *            the starting letter
     * @param end
     *            the ending letter
     * @return a list of letters
     */
    private static List<String> generateLetters(char start, char end) {
        // @formatter:off
        return IntStream.rangeClosed(start, end)
                                        .mapToObj(c -> "" + (char) c).collect(Collectors.toUnmodifiableList());
        // @formatter:on
    }
    
    /**
     * Verify that an exception is thrown for a blank regex pattern.
     */
    @Test
    void testEmptyRegex() {
        assertExceptionThrown("", "Regex pattern may not be blank.");
    }
    
    /**
     * Verify that an exception is thrown for any regex with whitespace.
     */
    @Test
    void testRegexWithWhitespace() {
        assertExceptionThrown(" 123 ", "Regex pattern may not contain any whitespace.");
        assertExceptionThrown("123| 234", "Regex pattern may not contain any whitespace.");
    }
    
    /**
     * Verify that an exception is thrown for any regex that cannot be compiled.
     */
    @Test
    void testNonCompilablePatterns() {
        // Empty character class.
        assertExceptionThrown("123[]", "Regex pattern will not compile.");
        // Empty negated character class.
        assertExceptionThrown("123[^]", "Regex pattern will not compile.");
        // Trailing backslash.
        assertExceptionThrown("123\\", "Regex pattern will not compile.");
        // Leading optional.
        assertExceptionThrown("?234", "Regex pattern will not compile.");
        // Repetition with undefined start.
        assertExceptionThrown("3{,3}", "Regex pattern will not compile.");
    }
    
    /**
     * Verify that exceptions are thrown for nonsensical patterns that, while compilable, are certainly not valid numeric regexes.
     */
    @Test
    void testNonsensePatterns() {
        assertExceptionThrown("\\.", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("\\-", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("-?", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("^$", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("^", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("$", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("(\\.)+", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("\\.*", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("-{3}", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("()|()", "A nonsense pattern has been given that cannot be normalized.");
        assertExceptionThrown("\\.|-", "A nonsense pattern has been given that cannot be normalized.");
    }
    
    /**
     * Verify that an exception is thrown for any regex that contains a letter other than the special case of \d.
     */
    @Test
    void testRegexWithRestrictedLetters() {
        // Verify an exception is thrown for any non-escaped letter.
        for (String letter : letters) {
            assertExceptionThrown(letter, "Regex pattern may not contain any letters other than \\d to indicate a member of the digit character class 0-9.");
        }
        
        // Verify an exception is thrown for '\D'.
        assertExceptionThrown("\\D", "Regex pattern may not contain any letters other than \\d to indicate a member of the digit character class 0-9.");
    }
    
    /**
     * Verify that an exception is thrown for any escaped character that is not \- \. or \d.
     */
    @Test
    void testRegexWithRestrictedEscapedCharacters() {
        // Verify no exception thrown for \d, \-, \., or an escaped number.
        NumericRegexEncoder.encode("\\d");
        NumericRegexEncoder.encode("\\.3");
        NumericRegexEncoder.encode("\\-4");
        
        // Verify exceptions are thrown for other characters.
        assertExceptionThrown("\\\\", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("\\(", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("\\)", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("1\\?", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("\\[", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("\\]", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("1\\|", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("1\\+", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("1\\*", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("1\\^", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        assertExceptionThrown("1\\$", "Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
    }
    
    /**
     * Verify that an exception is thrown for regexes with groups.
     */
    @Test
    void testRegexWithGroups() {
        assertExceptionThrown("(234)*", "Regex pattern may not contain any groups.");
    }
    
    /**
     * Verify that an exception is thrown for regexes with invalid character classes.
     */
    @Test
    void testRegexWithInvalidCharacterClasses() {
        assertExceptionThrown("[+!]", "Character classes may only contain numeric characters and numeric ranges.");
    }
    
    /**
     * Verify that invalid decimal points are not allowed.
     */
    @Test
    void testInvalidDecimalPoints() {
        // Verify quantifiers and optionals result in exceptions.
        assertExceptionThrown("234\\.?34", "Regex pattern may not contain any decimal points that are directly followed by * ? or {}.");
        assertExceptionThrown("234\\.*34", "Regex pattern may not contain any decimal points that are directly followed by * ? or {}.");
        assertExceptionThrown("234\\.+34", "Regex pattern may not contain any decimal points that are directly followed by * ? or {}.");
        assertExceptionThrown("234\\.{3}34", "Regex pattern may not contain any decimal points that are directly followed by * ? or {}.");
        
        // Verify multiple required decimal points result in exceptions.
        assertExceptionThrown("3\\.34\\.3", "Regex may not contain expressions with than one decimal point.");
        assertExceptionThrown("543.*|3\\.34\\.3", "Regex may not contain expressions with than one decimal point.");
    }
    
    /**
     * Verify that patterns that are ultimately empty after trimming all zero-length repetitions are not allowed.
     */
    @Test
    void testRegexConsistingOfZeroLengthRepetition() {
        assertExceptionThrown("3{0}?", "Regex pattern is empty after trimming all characters followed by {0} or {0,0}.");
        assertExceptionThrown("3{0,0}", "Regex pattern is empty after trimming all characters followed by {0} or {0,0}.");
        assertExceptionThrown("3{0,0}?|[4-6]{0}", "Regex pattern is empty after trimming all characters followed by {0} or {0,0}.");
    }
    
    /**
     * Test that regexes not requiring encoding are not modified.
     */
    @Test
    void testRegexesThatDoNotRequireEncoding() {
        assertRegex(".*").normalizesTo(".*");
        assertRegex("^.*$").normalizesTo("^.*$");
        assertRegex(".*?").normalizesTo(".*?");
        assertRegex("^.*?$").normalizesTo("^.*?$");
        assertRegex(".+").normalizesTo(".+");
        assertRegex("^.+$").normalizesTo("^.+$");
        assertRegex(".+?").normalizesTo(".+?");
        assertRegex("^.+?$").normalizesTo("^.+?$");
        assertRegex(".*.+.*?.+?").normalizesTo(".*.+.*?.+?");
        assertRegex("^.*.+.*?.+?$").normalizesTo("^.*.+.*?.+?$");
    }
    
    /**
     * Test parsing regexes that consists of simple numbers.
     */
    @Test
    void testSimpleNumbers() {
        // @formatter:off
        // Single simple numbers.
        assertRegex("123").normalizesTo("\\+cE1\\.23")
                        .matches("123");
        assertRegex("-32").normalizesTo("!YE6\\.8")
                        .matches("-32");
        assertRegex("983749587983487998734\\.34534").normalizesTo("\\+uE9\\.8374958798348799873434534")
                        .matches("983749587983487998734.34534");
        assertRegex("9983495030984594\\.54332").normalizesTo("\\+pE9\\.98349503098459454332")
                        .matches("9983495030984594.54332");
        assertRegex("-8889793487598488893485793").normalizesTo("!BE1\\.110206512401511106514207")
                        .matches("-8889793487598488893485793");
        
        // Verify escaped hyphens are supported.
        assertRegex("\\-32").normalizesTo("!YE6\\.8")
                        .matches("-32");
        
        // Verify anchors are trimmed.
        assertRegex("^123").normalizesTo("\\+cE1\\.23")
                        .matches("123");
        assertRegex("123$").normalizesTo("\\+cE1\\.23")
                        .matches("123");
        assertRegex("^123$").normalizesTo("\\+cE1\\.23")
                        .matches("123");
        
        // Verify no issues with combining anchors and escaped hyphens.
        assertRegex("^\\-123\\.234$").normalizesTo("!XE8\\.76766")
                        .matches("-123.234");
        
        // Verify no issues with alternated simple numbers.
        assertRegex("12|-45|23\\.45").normalizesTo("\\+bE1\\.2|!YE5\\.5|\\+bE2\\.345")
                        .matchesAllOf("12", "-45", "23.45");
        assertRegex("^12|-45|23\\.45$").normalizesTo("\\+bE1\\.2|!YE5\\.5|\\+bE2\\.345")
                        .matchesAllOf("12", "-45", "23.45");
        // @formatter:on
    }
    
    @Test
    void testDigitCharacterClass() {
        // @formatter:off
        assertRegex("\\d").normalizesTo("\\+aE\\d|\\+AE0")
                        .matchesAllOf("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
                        .matchesNoneOf("11", "34", "454");
        // @formatter:on
    }
    
    @Test
    void testCharacterClasses() {
        // @formatter:off
        // Test regexes made only of character classes.
        assertRegex("[12][45][78]").normalizesTo("\\+cE[12]\\.[45][78]")
                        .matchesAllOf("147", "148", "157", "158", "247", "248", "257", "258")
                        .matchesNoneOf("14.7", "14.8", "1.57", "1.58", "27.7", "25.7", "258.1");
    
        // Test character classes with a defined decimal point.
        assertRegex("[12][45]\\.[78]").normalizesTo("\\+bE[12]\\.[45][78]")
                        .matchesAllOf("14.7", "14.8", "15.7", "15.8", "24.7", "24.8", "25.7", "25.8")
                        .matchesNoneOf("147", "148", "157", "158", "247", "248", "257", "258");
    
        // Test character classes combined with numbers.
        assertRegex("12[6-8]").normalizesTo("\\+cE1\\.2[6-8]")
                        .matchesAllOf("126", "127", "128")
                        .matchesNoneOf("125", "129", "12.6", "12.7", "128.4");
        
        assertRegex("1\\.2[6-8]").normalizesTo("\\+aE1\\.2[6-8]")
                        .matchesAllOf("1.26", "1.27", "1.28")
                        .matchesNoneOf("1.25", "1.29");
        
        assertRegex("[6-8]12").normalizesTo("\\+cE[6-8]\\.12")
                        .matchesAllOf("612", "712", "812")
                        .matchesNoneOf("512", "912");
        
        assertRegex("[6-8]1\\.2").normalizesTo("\\+bE[6-8]\\.12")
                        .matchesAllOf("61.2", "71.2", "81.2")
                        .matchesNoneOf("51.2", "91.2");
        // @formatter:on
    }
    
    @Test
    void testCharacterClassesContainingPossibleZeroes() {
        // Special case where a regex contains character classes that can match numbers equal to or greater than one, or zero. In this case, the zero must be
        // put into an alternation and removed from the character class to ensure correct matching.
        // @formatter:off
        assertRegex("[0-9]").normalizesTo("\\+aE[0-9]|\\+AE0").matchesAllOf("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        
        assertRegex("[01234566789]").normalizesTo("\\+aE[01234566789]|\\+AE0")
                        .matchesAllOf("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        // @formatter:on
    }
    
    @Test
    void testTrailingWildcard() {
        // @formatter:off
        assertRegex("111.").normalizesTo("\\+[c-d]E1\\.11.?")
                        .matchesAllOf("1110", "1111", "1113", "1114", "1115", "1116", "1117", "1118", "1119")
                        .matchesNoneOf("11145", "111.45");
        // @formatter:on
    }
    
    @Test
    void testTrailingWildcardZeroOrMore() {
        // Test .* and .*? at end of regex for positive number.
        // @formatter:off
        assertRegex("111.*").normalizesTo("\\+[c-z]E1\\.11.*")
                        .matchesAllOf("111", "111445", "111.4325", "11153453.234")
                        .matchesNoneOf("1.11", "11.1", "0.43111");
        
        
        assertRegex("111.*?").normalizesTo("\\+[c-z]E1\\.11.*?")
                        .matchesAllOf("111", "111445", "111.4325", "11153453.234")
                        .matchesNoneOf("1.11", "11.1", "0.43111");
        // @formatter:on
        
        // Test .* and .*? at end of regex for negative number.
        // @formatter:off
        assertRegex("-111.*").normalizesTo("![A-X]E8\\.8(9|8.+)")
                        .matchesAllOf("-111", "-111.0", "-111.1", "-111.2", "-111.3", "-111.4", "-111.5", "-111.6", "-111.7", "-111.8", "-111.9")
                        .matchesAllOf("-1110", "-1111", "-1112", "-1113", "-1114", "-1115", "-1116", "-1117", "-1118", "-1119")
                        .matchesAllOf("-1114353454", "-1110.09203498", "-111090820394802933.234")
                        .matchesNoneOf("-110", "-1.11", "-11.1", "-121");
    
        assertRegex("-111.*?").normalizesTo("![A-X]E8\\.8(9|8.+?)")
                        .matchesAllOf("-1112", "-111.454", "-111111232", "-111")
                        .matchesNoneOf("-11", "-113544");
        // @formatter:on
    }
    
    @Test
    void testTrailingWildcardOneOrMore() {
        // Test .+ and .+? at end of regex for positive number.
        // @formatter:off
        assertRegex("111.+").normalizesTo("\\+[c-z]E1\\.11.*")
                        .matchesAllOf("1111", "111.0", "111445", "111.4325", "11153453.234")
                        .matchesNoneOf("1.11", "11.1", "0.43111");
        
        assertRegex("111.+?").normalizesTo("\\+[c-z]E1\\.11.*?")
                        .matchesAllOf("111.0", "111445", "111.4325", "11153453.234")
                        .matchesNoneOf("1.11", "11.1", "0.43111");
        
        // Test .+ and .+? at end of regex for negative number.
        assertRegex("-111.+").normalizesTo("![A-X]E8\\.8(9|8.+)")
                        .matchesAllOf("-111.0", "-111.1", "-111.2", "-111.3", "-111.4", "-111.5", "-111.6", "-111.7", "-111.8", "-111.9")
                        .matchesAllOf("-1110", "-1111", "-1112", "-1113", "-1114", "-1115", "-1116", "-1117", "-1118", "-1119")
                        .matchesAllOf("-1114353454", "-1110.09203498", "-111090820394802933.234")
                        .matchesNoneOf("-110", "-1.11", "-11.1", "-121");
    
        assertRegex("-111.+?").normalizesTo("![A-X]E8\\.8(9|8.+?)")
                        .matchesAllOf("-111.0", "-111.1", "-111.2", "-111.3", "-111.4", "-111.5", "-111.6", "-111.7", "-111.8", "-111.9")
                        .matchesAllOf("-1110", "-1111", "-1112", "-1113", "-1114", "-1115", "-1116", "-1117", "-1118", "-1119")
                        .matchesAllOf("-1114353454", "-1110.09203498", "-111090820394802933.234")
                        .matchesNoneOf("-110", "-1.11", "-11.1", "-121");
        // @formatter:on
    }
    
    @Test
    void testLeadingZeroOrMoreQuantifier() {
        // @formatter:off
        // Test .* at start of regex. The .* can remain a .* after the decimal point since it is a zero or more match.
        assertRegex(".*54").normalizesTo("\\+[b-zA-Z]E.*5\\.?4|![A-Ya-z]E.*4\\.?6")
                        .matchesAllOf("154", "6644444444444444.54", "54", "-154", "-54", "-3566666666654", "0.00054", "-0.42222254")
                        .matchesNoneOf("111143");
        
        // Test .*?.
        assertRegex(".*?54").normalizesTo("\\+[b-zA-Z]E.*?5\\.?4|![A-Ya-z]E.*?4\\.?6")
                        .matchesAllOf("154", "6644444444444444.54", "54", "-154", "-54", "-3566666666654", "0.00054", "-0.42222254")
                        .matchesNoneOf("111143");
    
        // Test .* at start of regex. The .* can remain a .* after the decimal point since it is a zero or more match.
        assertRegex(".*\\.54").normalizesTo("\\+[a-zZ]E.*5\\.?4|![A-Za]E.*4\\.?6")
                        .matchesAllOf("0.54", "6644444444444444.54", "-1.54", "-.54", "-35666666666.54")
                        .matchesNoneOf("111143", "0.00054");
    
        // Test .*?.
        assertRegex(".*?\\.54").normalizesTo("\\+[a-zZ]E.*?5\\.?4|![A-Za]E.*?4\\.?6")
                        .matchesAllOf("1.54", "6644444444444444.54", "-.54", "-1.54")
                        .matchesNoneOf("111143");
    
        assertRegex("\\..*54").normalizesTo("\\+[A-Z]E.*5\\.?4")
                        .matchesAllOf(".154", ".054", ".54", ".3566666666654", ".00054", ".42222254")
                        .matchesNoneOf("111143", "6644444444444444.54", "1.54", "154", "-.154");
    
        // Test .*?.
        assertRegex("\\..*?54").normalizesTo("\\+[A-Z]E.*?5\\.?4")
                        .matchesAllOf(".154", ".054", ".54", ".3566666666654", ".00054", ".42222254")
                        .matchesNoneOf("111143", "6644444444444444.54", "1.54", "154", "-.154");
        // @formatter:on
    }
    
    @Test
    void testLeadingOneOrMoreQuantifier() {
        // @formatter:off
        // Test .+ at start of regex. The .+ should become a .* after the decimal point since we have one wildcard guaranteed before the decimal point.
        assertRegex(".+54").normalizesTo("\\+[b-zA-Z]E.*5\\.?4|![A-Ya-z]E.*4\\.?6")
                        .matchesAllOf("154", "6644444444444444.54", "-154", "-444444444444454", "0.54", "054")
                        .matchesNoneOf("5.4", "542343");
        
        // Test .+?.
        assertRegex(".+?54").normalizesTo("\\+[b-zA-Z]E.*?5\\.?4|![A-Ya-z]E.*?4\\.?6")
                        .matchesAllOf("154", "6644444444444444.54", "-154", "-222222222222254", "-054")
                        .matchesNoneOf("5.4", "542343");
    
        assertRegex(".+\\.54").normalizesTo("\\+[a-zZ]E.*5\\.?4|![A-Za]E.*4\\.?6")
                        .matchesAllOf("1.54", "6644444444444444.54", "-1.54", "-4444444444444.54", "0.54")
                        .matchesNoneOf("542343", ".544453");
    
        // Test .+?.
        assertRegex(".+?\\.54").normalizesTo("\\+[a-zZ]E.*?5\\.?4|![A-Za]E.*?4\\.?6")
                        .matchesAllOf("1.54", "6644444444444444.54", "-1.54", "-2222222222222.54")
                        .matchesNoneOf("542343", ".0000054");
    
        assertRegex("\\..+54").normalizesTo("\\+[A-Z]E.*5\\.?4")
                        .matchesAllOf(".154", ".664444444444444454", ".0000000054", ".054")
                        .matchesNoneOf("54", "5.4", "542343", "-.154", "154", "-.000054");
    
        // Test .+?.
        assertRegex("\\..+?54").normalizesTo("\\+[A-Z]E.*?5\\.?4")
                        .matchesAllOf(".154", ".664444444444444454", ".0000000054", ".054")
                        .matchesNoneOf("54", "5.4", "542343", "-.154", "154", "-.000054");
        // @formatter:on
    }
    
    @Test
    void testLeadingOneOrMoreQuantifierForWildcard() {
        // @formatter:off
        // Test .+ at start of regex. The .+ should become a .* after the decimal point since we have one wildcard guaranteed before the decimal point.
        assertRegex(".{3}54").normalizesTo("\\+[b-eW-Z]E.?\\.?.{0,2}5\\.?4|![V-Ya-d]E.?\\.?.{0,2}4\\.?6")
                        .matchesAllOf("11154", "43.54", "-1154", "-4454", "00054", "-0054")
                        .matchesNoneOf("5.4", "542343");
        
        // Test .+?.
        assertRegex(".{3}?54").normalizesTo("\\+[b-eW-Z]E.?\\.?.{0,2}?5\\.?4|![V-Ya-d]E.?\\.?.{0,2}?4\\.?6")
                        .matchesAllOf("00154", "-1054", "00054", "99954", ".0054")
                        .matchesNoneOf("5.4", "542343", "6644444444444444.54", "-222222222222254");
        
        assertRegex(".{3}\\.54").normalizesTo("\\+[a-cZ]E.?\\.?.{0,2}5\\.?4|![X-Za]E.?\\.?.{0,2}4\\.?6")
                        .matchesAllOf("111.54", "444.54", "000.54", "-00.54", "-46.54")
                        .matchesNoneOf("542343", "343.4554", "0.000054");
        
        // Test .+?.
        assertRegex(".{3}?\\.54").normalizesTo("\\+[a-cZ]E.?\\.?.{0,2}?5\\.?4|![X-Za]E.?\\.?.{0,2}?4\\.?6")
                        .matchesAllOf("111.54", "444.54", "000.54", "-00.54", "-46.54")
                        .matchesNoneOf("542343", "343.4554", "0.000054");
        
        assertRegex("\\..{3}54").normalizesTo("\\+[W-Z]E.?\\.?.{0,2}5\\.?4")
                        .matchesAllOf(".00154", ".34354", ".99954")
                        .matchesNoneOf("54", "5.4", "542343", "-.00154");
        
        // Test .+?.
        assertRegex("\\..{3}?54").normalizesTo("\\+[W-Z]E.?\\.?.{0,2}?5\\.?4")
                        .matchesAllOf(".00154", ".34354", ".99954")
                        .matchesNoneOf("54", "5.4", "542343", "-.00154");
        // @formatter:on
    }
    
    @Test
    void testLeadingRepetitionQuantifier() {
        // @formatter:off
        // Test {3} at start of regex. The {3} should become a {2} after the decimal point since we will have an occurrence '1' exactly specified in the regex
        // before the decimal point.
        assertRegex("1{3}4").normalizesTo("\\+dE1\\.1{2}4")
                        .matches("1114")
                        .matchesNoneOf("1.4", "114", "11114", "111.4");
    
        // Test {2} at start of regex. The repetition quantifier should be removed entirely after the decimal point since we have the two occurrences of '1'
        // exactly specified in the regex.
        assertRegex("1{2}4").normalizesTo("\\+cE1\\.14")
                        .matches("114")
                        .matchesNoneOf("1.4", "14", "11.4", "1114");
    
        // Test {3,} at start of regex. The {3,} should become a {2,} after the decimal point since we will have an occurrence of '1' exactly specified in the
        // regex before the decimal point.
        assertRegex("1{3,}4").normalizesTo("\\+[d-z]E1\\.1{2,}4")
                        .matchesAllOf("1114", "1111111114", "11111111111111111114")
                        .matchesNoneOf("1.4", "14", "114", "124", "111.4");
    
        // Test {2,} at start of regex. The {2,} should become a + since we only need to require an occurrence of '1' one or more times after the decimal point.
        assertRegex("1{2,}4").normalizesTo("\\+[c-z]E1\\.1+4")
                        .matchesAllOf("114", "11111114", "1111111111111111114")
                        .matchesNoneOf("1.4", "4", "14", "24", "11.4");
    
        // Test {1} at start of regex. The {1} can be removed entirely.
        assertRegex("1{1}4").normalizesTo("\\+bE1\\.4")
                        .matches("14")
                        .matchesNoneOf("1", "1.4", "4", "114", "1111111111111114", "124");
    
        // Test {1,} at start of regex. The {1,} should become a * since an occurrence of '1' can happen zero or more times after the decimal point.
        assertRegex("1{1,}4").normalizesTo("\\+[b-z]E1\\.?1*4")
                        .matchesAllOf("14", "1111111114", "11111111111111111114")
                        .matchesNoneOf("4", "1.4", "104");
    
        // Test {1,2} at start of regex. The {1,2} should become an ? after the decimal point.
        assertRegex("1{1,2}4").normalizesTo("\\+[b-c]E1\\.?1{0,1}4")
                        .matchesAllOf("14", "114")
                        .matchesNoneOf("1", "4", "1.4", "104", "1114");
    
        // Test {1,2} at start of regex. The {1,2} should become a {1,2} after the decimal point.
        assertRegex("1{2,3}4").normalizesTo("\\+[c-d]E1\\.1{1,2}4")
                        .matchesAllOf("114", "1114")
                        .matchesNoneOf("1.4", "14", "11114", "104", "11.4");
    
        // Test {1,2} at start of regex. The {1,2} should become a {4,13} after the decimal point.
        assertRegex("1{5,14}4").normalizesTo("\\+[f-o]E1\\.1{4,13}4")
                        .matchesAllOf("111114", "11111111114", "111111111111114")
                        .matchesNoneOf("11111.4", "11114", "1111111111111114");
        
        // Test {0,} at the start of the regex. The {0,} is equivalent to * and can remain the same after the decimal point.
        assertRegex("1{0,}4").normalizesTo("\\+[a-z]E1?\\.?1*4")
                        .matchesAllOf("4", "14", "11111111111114", "111111111111111114")
                        .matchesNoneOf("1", "104");
        
        // Test {0,5} at the start of the regex. The {0,5} should become {0,4} after the decimal point.
        assertRegex("1{0,5}4").normalizesTo("\\+[a-f]E1?\\.?1{0,4}4")
                        .matchesAllOf("4", "14", "114", "1114", "11114", "111114")
                        .matchesNoneOf("1", "1111114");
        // @formatter:on
    }
    
    @Test
    void testSubOneRegexesWithDecimalPoints() {
        // @formatter:off
        assertRegex("0\\.5.*").normalizesTo("\\+ZE5\\.?.*")
                        .matchesAllOf("0.5", "0.545984")
                        .matchesNoneOf("0.6", "0.05");
        
        assertRegex("0\\.005.*").normalizesTo("\\+XE5\\.?.*")
                        .matchesAllOf("0.005", "0.0059834795")
                        .matchesNoneOf("0.05", "0.006");
        
        assertRegex("0\\.0000000000000[3-7]45\\d").normalizesTo("\\+ME[3-7]\\.45\\d?")
                        .matchesAllOf("0.00000000000003450", "0.00000000000005453", "0.00000000000007459")
                        .matchesNoneOf("0.000345", "0.00000000000001450");
        // @formatter:on
    }
    
    @Test
    void testSingleTrailingZeroOrMoreAfterDecimalPoint() {
        // @formatter:off
        assertRegex("0\\.5.*").normalizesTo("\\+ZE5\\.?.*")
                        .matchesAllOf("0.5", "0.545984")
                        .matchesNoneOf("0.6", "0.05");
        // @formatter:on
    }
    
    @Test
    void testComplexNegativePattern() {
        // @formatter:off
        assertRegex("-34\\d.{0,4}4*").normalizesTo("![A-X]E6\\.(6|5\\d|5\\d.{0,4}|5\\d.{0,4}5*6)")
                        .matchesAllOf("-340", "-341", "-342", "-343", "-344", "-345", "-346", "-347", "-348", "-349") // Test matching -34\d portion
                        .matchesNoneOf("340", "341", "342", "343", "344", "345", "346", "347", "348", "349") // Ensure does not match positive variant
                        .matchesAllOf("-3411", "-34112", "-341123", "-3411234", "-3419876") // Test matching -34\d.{0,4} portion
                        .matchesAllOf("-3419586444444", "-34195864444444444444") // Test matching -34\d.{0,4}4* portion
                        .matchesNoneOf("-3414321999999"); // Ensure does not match numbers not ending with 4*
        // @formatter:on
    }
    
    private void assertExceptionThrown(String pattern, String message) {
        assertThatThrownBy(() -> NumericRegexEncoder.encode(pattern)).hasMessage(message);
    }
    
    private RegexAssert assertRegex(String regex) {
        return new RegexAssert(regex);
    }
    
    private static class RegexAssert {
        private final Pattern original;
        private Pattern normalized;
        
        private RegexAssert(String pattern) {
            this.original = Pattern.compile(pattern);
        }
        
        public RegexAssert normalizesTo(String expected) {
            String actual = NumericRegexEncoder.encode(original.toString());
            assertThat(actual).as("Check normalizing %s", original).isEqualTo(expected);
            normalized = Pattern.compile(actual);
            return this;
        }
        
        public RegexAssert matches(String number) {
            assertMatchStatus(number, true);
            return this;
        }
        
        public RegexAssert matchesAllOf(String... numbers) {
            for (String number : numbers) {
                matches(number);
            }
            return this;
        }
        
        public RegexAssert doesNotMatch(String number) {
            assertMatchStatus(number, false);
            return this;
        }
        
        public RegexAssert matchesNoneOf(String... numbers) {
            for (String number : numbers) {
                doesNotMatch(number);
            }
            return this;
        }
        
        private void assertMatchStatus(String number, boolean match) {
            String matchStatus = match ? " matches " : " does not match ";
            assertThat(original.matcher(number).matches()).as("Assert " + original + matchStatus + number).isEqualTo(match);
            String encodedNumber = NumericalEncoder.encode(number);
            assertThat(normalized.matcher(encodedNumber).matches()).as("Assert " + normalized + matchStatus + encodedNumber + " (" + number + ")")
                            .isEqualTo(match);
        }
    }
}
