package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;
import datawave.data.type.util.NumericalEncoder;

class NegativeNumberPatternInverterTest {
    
    /**
     * Verify that patterns consisting of simple numbers are not modified by {@link NegativeNumberPatternInverter}.
     */
    @Test
    void testPatternsMadeOfSimpleNumbers() {
        // Test a single positive number.
        assertInverted("345", "\\+cE3\\.45");
        
        // Test a single negative number.
        assertInverted("-345", "!XE6\\.55");
        
        // Test alternated positive and negative number.
        assertInverted("345|-345", "\\+cE3\\.45|!XE6\\.55");
    }
    
    /**
     * Verify that patterns consisting of positive number patterns are not modified by {@link NegativeNumberPatternInverter}.
     */
    @Test
    void testPatternsMadeOfPositivePatterns() {
        // Single positive number pattern.
        assertInverted(".*345", "\\+[c-zA-Z]E.*345");
        
        // Alternated positive number patterns.
        assertInverted("45.*|0.045[3-5]", "\\+[b-z]E45.*|\\+[c-eY]E.?0?45[3-5]");
    }
    
    /**
     * Verify that patterns consisting of positive number patterns and simple numbers are not modified by {@link NegativeNumberPatternInverter}.
     */
    @Test
    void testPatternsMadeOfPositivePatternsAndSimpleNumbers() {
        // Alternated with positive simple number.
        assertInverted("345.*|456", "\\+[c-z]E345.*|\\+cE4\\.56");
        
        // Alternated with negative simple number.
        assertInverted(".*345|-456", "\\+[c-zA-Z]E.*345|!XE5\\.44");
        
        // Alternated with positive and negative simple number.
        assertInverted("45.*|0.045[3-5]|456|-456", "\\+[b-z]E45.*|\\+[c-eY]E.?0?45[3-5]|\\+cE4\\.56|!XE5\\.44");
    }
    
    @Test
    void testWildcard() {
        assertInverted("-.234", "![W-Xa]E.?766");
        assertInverted("-34.454", "![U-Y]E65.546");
        assertInverted("-34454.", "![U-V]E6554(6|5.)");
    }
    
    @Test
    void testMultiWildcards() {
        assertInverted("-.*234", "![A-Xa-z]E.*766");
        assertInverted("-.+234", "![A-Xa-z]E.*766");
        assertInverted("-0.00454.*", "![A-Xc]E.?(9{2})?54(6|5.+)");
    }
    
    @Test
    void testCharacterClasses() {
        assertInverted("-[2-4]", "!ZE[6-8]");
        assertInverted("-[1357]", "!ZE[9753]");
        assertInverted("-[46-8]", "!ZE[62-4]");
    }
    
    @Test
    void testDigitCharacterClass() {
        assertInverted("-\\d", "!ZE\\d");
        assertInverted("-\\d*", "![A-Z]E\\d+");
    }
    
    @Test
    void testConsolidatedLeadingZeros() {
        // The consolidated zeros in (0{3})? should not be modified for a positive expression.
        assertInverted(".*000.*3", "\\+[a-zA-Z]E.*(0{3})?.*3");
        
        // However, in a positive expression, they should get negated to a value of 9.
        assertInverted("-.*000.*3", "![A-Za-z]E.*(9{3})?.*7");
        
        // Even if there could possibly be no elements after the consolidated zeros, they should get negated to a value of 9.
        assertInverted("-3.*000.*", "![A-Z]E(7|6.+|6.*9{3}.+)");
    }
    
    @Test
    void testTrailingZeros() {
        assertInverted("-22[0-4]", "!XE7(8|7[6-9])");
        assertInverted("-22[0157]", "!XE7(8|7[953])");
        assertInverted("-22[0157]*", "![A-Y]E7(8|7[9842]*[953])");
        assertInverted("-22[0157]+", "![A-X]E7(8|7[9842]*[953])");
        assertInverted("-22[0157]{3}", "!VE7(8|7[9842]{0,2}[953])");
        assertInverted("-22[0157]{1,3}", "![V-X]E7(8|7[9842]{0,2}[953])");
        assertInverted("-[1369]*2[0157]{1,3}0{1,3}", "![A-X]E[8630]*(8|7[9842]{0,2}[953])");
        assertInverted("-22[^03]{1,3}[06]{3,4}", "![R-U]E77([^96]{0,2}[^7]|[^96]{1,3}[93]{2,3}4)");
    }
    
    @Test
    void testRepetitions() {
        assertInverted("-4{3}", "!XE5{2}6");
        assertInverted("-4{3,6}", "![U-X]E5{2,5}6");
        assertInverted("-4{0,6}", "![U-Z]E5{0,5}6");
        assertInverted("-4{1,4}", "![W-Z]E5{0,3}6");
        assertInverted("-4{1,}", "![A-Z]E5{0,}6");
        assertInverted("-4{2,}", "![A-Y]E5{1,}6");
    }
    
    public void assertInverted(String pattern, String expectedPattern) {
        Node actual = SimpleNumberEncoder.encode(parse(pattern));
        actual = ExponentialBinAdder.addBins(actual);
        actual = ZeroTrimmer.trim(actual);
        actual = NegativeNumberPatternInverter.invert(actual);
        
        assertThat(actual).asTreeString().isEqualTo(expectedPattern);
    }
    
}
