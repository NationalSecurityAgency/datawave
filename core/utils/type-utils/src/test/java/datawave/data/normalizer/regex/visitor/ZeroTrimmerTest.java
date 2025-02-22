package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

import datawave.data.normalizer.ZeroRegexStatus;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexParser;

class ZeroTrimmerTest {
    
    @Nested
    class LeadingZeros {
        
        @Test
        void testZerosWithoutQuantifiers() {
            // Test trimming explicit zeros in pattern without decimal point.
            assertTrimmedTo("00345.*", "\\+[c-z]E345.*");
            
            // Test trimming explicit zeros in pattern before decimal point.
            assertTrimmedTo("000\\.34.*", "\\+ZE34.*");
            
            // Test trimming explicit zeros in pattern before and after the decimal point.
            assertTrimmedTo("00\\.000034.*", "\\+VE34.*");
            
            // Test trimming explicit zeros in pattern after decimal point.
            assertTrimmedTo("\\.000034.*", "\\+VE34.*");
            
            // Test trimming explicit zeros (including [0]) in pattern after decimal point.
            assertTrimmedTo("\\.0[0][0]034.*", "\\+VE34.*");
        }
        
        @Test
        void testZerosWithQuantifiers() {
            // Test trimming leading zeros with * quantifier.
            assertTrimmedTo("0*0345.*", "\\+[c-z]E345.*");
            
            // Test trimming leading zeros with + quantifier.
            assertTrimmedTo("0+0\\.34.*", "\\+ZE34.*");
            
            // Test trimming leading zeros with {3} quantifier.
            assertTrimmedTo("00\\.0[0]{3}034.*", "\\+UE34.*");
            
            // Test trimming leading zeros with {3,5} quantifier.
            assertTrimmedTo("\\.0{3,5}00034.*", "\\+[R-T]E34.*");
            
            // Test that 0* with no other zeros.
            assertTrimmedTo("0*34", "\\+bE34");
            
            // Test that 0+ with no other zeros.
            assertTrimmedTo("0+34", "\\+bE34");
            
            // Test that 0* with other zeros.
            assertTrimmedTo("00*0034", "\\+bE34");
            
            // Test that 0+ with other zeros.
            assertTrimmedTo("00+0034", "\\+bE34");
            
            // Test that zeros with repetitions.
            assertTrimmedTo("00{3}0{2}34", "\\+bE34");
            
            // Test that a + after a possible zero is changed to a *.
            assertTrimmedTo("[06]+\\.[01789]*[124678]", "\\+[a-zA-Z]E[06]*[01789]*[124678]");
            
            // Test that a +? after a possible zero is changed to a *?.
            assertTrimmedTo("[06]+?\\.[01789]*[124678]", "\\+[a-zA-Z]E[06]*?[01789]*[124678]");
            
            // Test that {x} quantifiers after a possible zero will include a possible length of zero.
            assertTrimmedTo("[06]{3}\\.[01789]{5}[124678]", "\\+[a-cU-Z]E[06]{0,3}[01789]{0,5}[124678]");
            
            // Test that {x}? quantifiers after a possible zero will include a possible length of zero.
            assertTrimmedTo("[06]{3}?\\.[01789]{5}[124678]", "\\+[a-cU-Z]E[06]{0,3}?[01789]{0,5}[124678]");
            
            // Test that {x,y} quantifiers after a possible zero will be made optional.
            assertTrimmedTo("[06]{4,7}\\.[01789]{3,9}[124678]", "\\+[a-gQ-Z]E([06]{4,7})?([01789]{3,9})?[124678]");
            
            // Test that {x,y}? quantifiers after a possible zero will be made optional.
            assertTrimmedTo("[06]{4,7}?\\.[01789]{3,9}?[124678]", "\\+[a-gQ-Z]E([06]{4,7}?)?([01789]{3,9}?)?[124678]");
        }
        
        @Test
        void testZerosAfterWildcard() {
            // Test trimming explicit zeros after a single wildcard.
            assertTrimmedTo(".0000\\.34.*", "\\+[eZ]E.?(0{4})?34.*");
            
            // Test trimming explicit zeros after .*.
            assertTrimmedTo(".*0000\\.34.*", "\\+[e-zZ]E.*(0{4})?34.*");
            
            // Test trimming explicit zeros after .+.
            assertTrimmedTo(".+0000\\.34.*", "\\+[e-zZ]E.*(0{4})?34.*");
        }
        
        @Test
        void testZerosAfterMultipleWildcards() {
            // Test trimming explicit zeros after a single wildcard.
            assertTrimmedTo(".000.0\\.34.*", "\\+[b-fZ]E.?(0{3})?.?0?34.*");
            
            // Test trimming explicit zeros after .*.
            assertTrimmedTo(".*000.*0\\.34.*", "\\+[b-zZ]E.*(0{3})?.*0?34.*");
            
            // Test trimming explicit zeros after .+.
            assertTrimmedTo(".+000.+0\\.34.*", "\\+[b-zZ]E.*(0{3})?.*0?34.*");
        }
        
        @Test
        void testZerosAfterDecimalPointWithPossibleAllLeadingZeros() {
            assertTrimmedTo(".\\.000034.*", "\\+[aV]E.?(0{4})?34.*");
            assertTrimmedTo(".*\\.000034.*", "\\+[a-zV]E.*(0{4})?34.*");
            assertTrimmedTo(".+\\.000034.*", "\\+[a-zV]E.*(0{4})?34.*");
            assertTrimmedTo(".0{3}\\.000034.*", "\\+[dV]E.?(0{7})?34.*");
            assertTrimmedTo("[034]0\\.000034.*", "\\+[bV]E[034]?(0{5})?34.*");
        }
        
        @Test
        void testZerosAfterPossibleZeroCharacter() {
            assertTrimmedTo(".000000343", "\\+[c-jT]E.?(0{6})?343");
            assertTrimmedTo(".*000000343", "\\+[c-zA-Z]E.*(0{6})?343");
            assertTrimmedTo(".+000000343", "\\+[c-zA-Z]E.*(0{6})?343");
            assertTrimmedTo("[0-9]000000343", "\\+[c-j]E[0-9]?(0{6})?343");
        }
        
        @Test
        void testZerosWithRepetitionRange() {
            // Test 0{0,}, equivalent to 0*
            assertTrimmedTo(".*0{0,}3", "\\+[a-zA-Z]E.*(0*)?3");
            
            // Test 0{0,} with other zeros.
            assertTrimmedTo(".*00{0,}03", "\\+[a-zA-Z]E.*(0{2,})?3");
            
            // Test 0{1,}, equivalent to 0+.
            assertTrimmedTo(".*0{1,}3", "\\+[a-zA-Z]E.*(0+)?3");
            
            // Test 0{1,} with other zeros.
            assertTrimmedTo(".*00{1,}03", "\\+[a-zA-Z]E.*(0{3,})?3");
            
            // Test 0{1,5} with defined end bound.
            assertTrimmedTo(".*0{1,5}3", "\\+[a-zA-Z]E.*(0{1,5})?3");
            
            // Test 0{1,5} with other zeros.
            assertTrimmedTo(".*00{1,5}03", "\\+[a-zA-Z]E.*(0{3,7})?3");
            
            // Test 0{3,} with undefined end bound.
            assertTrimmedTo(".*0{3,}3", "\\+[a-zA-Z]E.*(0{3,})?3");
            
            // Test 0{3,} with other zeros.
            assertTrimmedTo(".*00{3,}03", "\\+[a-zA-Z]E.*(0{5,})?3");
        }
    }
    
    @Nested
    class TrailingZeros {
        
        @Test
        void testZerosWithoutQuantifiers() {
            // Test trimming explicit zeros in pattern without decimal point.
            assertTrimmedTo("345.*00", "\\+[c-z]E345.*");
            
            // Test trimming explicit zeros in pattern after decimal point.
            assertTrimmedTo("\\.34.*00", "\\+ZE34.*");
            
            // Test trimming explicit zeros (including [0]).
            assertTrimmedTo("34.*0[0][0]0", "\\+[b-z]E34.*");
        }
        
        @Test
        void testZerosWithQuantifiers() {
            // Test trimming zeros with * quantifier.
            assertTrimmedTo("345.*0*", "\\+[c-z]E345.*");
            
            // Test trimming zeros with + quantifier.
            assertTrimmedTo("\\.34.*0+", "\\+ZE34.*");
            
            // Test trimming zeros with {3} quantifier.
            assertTrimmedTo("34.*0{3}", "\\+[b-z]E34.*");
            
            // Test trimming zeros with {3,5} quantifier.
            assertTrimmedTo("34.0{3,5}", "\\+[b-h]E34.?");
            
            // Test that 0* with no other zeros.
            assertTrimmedTo("340*", "\\+[b-z]E34");
            
            // Test that 0+ with no other zeros.
            assertTrimmedTo("340+", "\\+[c-z]E34");
            
            // Test that 0* with other zeros.
            assertTrimmedTo("3400*00", "\\+[e-z]E34");
            
            // Test that 0+ with other zeros.
            assertTrimmedTo("3400+00", "\\+[f-z]E34");
            
            // Test that zeros with repetitions.
            assertTrimmedTo("3400{3}0{2}", "\\+hE34");
            
            // Test that {x} quantifiers after a possible trailing zero will include a possible length of zero.
            assertTrimmedTo("[123678]3\\.[01789]{5}", "\\+bE[123678]3[01789]{0,5}");
            
            // Test that {x}? quantifiers after a possible trailing zero will include a possible length of zero.
            assertTrimmedTo("[124678]3\\.[01789]{5}?", "\\+bE[124678]3[01789]{0,5}?");
            
            // Test that {x,y} quantifiers after a possible trailing zero will be made optional.
            assertTrimmedTo("[124678]3\\.[01789]{3,9}", "\\+bE[124678]3([01789]{3,9})?");
            
            // Test that {x,y}? quantifiers after a possible trailing zero will be made optional.
            assertTrimmedTo("[124678]3\\.[01789]{3,9}?", "\\+bE[124678]3([01789]{3,9}?)?");
        }
        
        @Test
        void testZerosBeforeWildcard() {
            // Test trimming explicit zeros before a single wildcard.
            assertTrimmedTo("23000.", "\\+[e-f]E23(0{3})?.?");
            
            // Test trimming explicit zeros before .*.
            assertTrimmedTo("23000.*", "\\+[e-z]E23(0{3})?.*");
            
            // Test trimming explicit zeros before .+. Because .+ could be a number of zeros, we need to change it to .* to allow for the fact that trailing
            // zeros would be trimmed in encoded numbers.
            assertTrimmedTo("23000.+00.*", "\\+[e-z]E23(0{3})?.*(0{2})?.*");
        }
        
        @Test
        void testZerosBeforeMultipleWildcards() {
            // Test trimming explicit zeros before a single wildcard.
            assertTrimmedTo("23.00.0.", "\\+[b-h]E23.?(0{2})?.?0?.?");
            
            // Test trimming explicit zeros before .*.
            assertTrimmedTo("23.*00.*0.*", "\\+[b-z]E23.*(0{2})?.*0?.*");
            
            // Test trimming explicit zeros after .+.
            assertTrimmedTo("23.+00.+0.+", "\\+[b-z]E23.*(0{2})?.*0?.*");
        }
        
        @Test
        void testZerosBeforeDecimalPointWithPossibleAllTrailingZeros() {
            assertTrimmedTo("3400\\.0000.", "\\+dE34(0{6})?.?");
            assertTrimmedTo("3400\\.0000.*", "\\+dE34(0{6})?.*");
            assertTrimmedTo("3400{3}\\.0000.*", "\\+fE34(0{8})?.*");
            assertTrimmedTo("34[012]0\\.0000.*", "\\+dE34[012]?(0{5})?.*");
            // The trailing .+ must become .* to allow for trimmed zeros.
            assertTrimmedTo("3400\\.0000.+", "\\+dE34(0{6})?.*");
        }
        
        @Test
        void testZerosBeforePossibleZeroCharacter() {
            // The trailing .+ must become .* to allow for trimmed zeros.
            assertTrimmedTo("2300000.+", "\\+[g-z]E23(0{5})?.*");
            
            assertTrimmedTo("2300000.", "\\+[g-h]E23(0{5})?.?");
            assertTrimmedTo("2300000.*", "\\+[g-z]E23(0{5})?.*");
            assertTrimmedTo("2300000[0-9]", "\\+hE23(0{5})?[0-9]?");
        }
        
        @Test
        void testZerosWithRepetitionRange() {
            // Test 0{0,}, equivalent to 0*
            assertTrimmedTo("3.*0{0,}[01]", "\\+[a-z]E3.*(0*)?[01]?");
            
            // Test 0{0,} with other zeros.
            assertTrimmedTo("3.*00{0,}0[01]", "\\+[a-z]E3.*(0{2,})?[01]?");
            
            // Test 0{1,}, equivalent to 0+.
            assertTrimmedTo("3.*0{1,}[01]", "\\+[a-z]E3.*(0+)?[01]?");
            
            // Test 0{1,} with other zeros.
            assertTrimmedTo("3.*00{1,}0[01]", "\\+[a-z]E3.*(0{3,})?[01]?");
            
            // Test 0{1,5} with defined end bound.
            assertTrimmedTo("3.*0{1,5}[01]", "\\+[a-z]E3.*(0{1,5})?[01]?");
            
            // Test 0{1,5} with other zeros.
            assertTrimmedTo("3.*00{1,5}0[01]", "\\+[a-z]E3.*(0{3,7})?[01]?");
            
            // Test 0{3,} with undefined end bound.
            assertTrimmedTo("3.*0{3,}[01]", "\\+[a-z]E3.*(0{3,})?[01]?");
            
            // Test 0{3,} with other zeros.
            assertTrimmedTo("3.*00{3,}0[01]0", "\\+[a-z]E3.*(0{5,})?[01]?");
        }
    }
    
    @Test
    void testNoLeadingOrTrailingZeros() {
        assertTrimmedTo(".*344", "\\+[c-zA-Z]E.*344");
        assertTrimmedTo("45.*", "\\+[b-z]E45.*");
        assertTrimmedTo("300454.*", "\\+[f-z]E300454.*");
        assertTrimmedTo("300.*0003", "\\+[c-z]E300.*0003");
        assertTrimmedTo("300.*000[1-9]", "\\+[c-z]E300.*000[1-9]");
        
    }
    
    @Test
    void testSingleElementPatterns() {
        assertTrimmedTo(".", "\\+aE.");
        assertTrimmedTo(".*", "\\+[a-zA-Z]E.*");
        assertTrimmedTo(".*?", "\\+[a-zA-Z]E.*?");
        assertTrimmedTo(".+", "\\+[a-zA-Z]E.+");
        assertTrimmedTo(".+?", "\\+[a-zA-Z]E.+?");
        assertTrimmedTo("[14]", "\\+aE[14]");
        assertTrimmedTo("[14]{3}", "\\+cE[14]{3}");
        assertTrimmedTo("\\d", "\\+aE\\d");
        assertTrimmedTo("\\d{3}", "\\+[a-c]E\\d{3}");
    }
    
    @Test
    void testStatus() {
        ZeroRegexStatus status = ZeroRegexStatus.NONE;
        assertStatus("300.*0003", status);
        assertStatus("300.*000[1-9]", status);
        assertStatus("45.*", status);
        assertStatus("-45.*", status);
        
        status = ZeroRegexStatus.LEADING;
        assertStatus(".*", status);
        assertStatus(".*?", status);
        assertStatus(".*?11", status);
        assertStatus("[04][05][06]", status);
        assertStatus("[04]{1,3}[05][06]", status);
        assertStatus("\\d{3}", status);
        assertStatus(".\\.000034.*", status);
        assertStatus("00345.*", status);
        assertStatus("\\.000034.*", status);
        assertStatus("-00345.*", status);
        
        status = ZeroRegexStatus.TRAILING;
        assertStatus("3.*0{0,}[01]", status);
        assertStatus("3.*?0{0,}[01]", status);
        assertStatus("3400\\.0000.", status);
        assertStatus("340.*", status);
        assertStatus("340.*?", status);
        assertStatus("3400{3}0{2}", status);
        
    }
    
    @Test
    void testTrailingZerosWithoutQuantifiers() {
        assertTrimmedTo(".*34300", "\\+[e-zA-Z]E.*343");
    }
    
    @Test
    void testNegativeNumber() {
        assertTrimmedTo("-0.00454.*", "![A-Xc]E.?(0{2})?454.*");
    }
    
    @Test
    void testMixedAlternation() {
        assertTrimmedTo("234\\.45|343.*|0\\.00[0]34.*", "\\+cE2\\.3445|\\+[c-z]E343.*|\\+WE34.*");
    }
    
    private void assertStatus(String pattern, ZeroRegexStatus status) {
        Assert.equals(ZeroTrimmer.getStatus(RegexParser.parse(pattern).getChildren()), status);
    }
    
    private void assertTrimmedTo(String pattern, String expectedPattern) {
        Node actual = SimpleNumberEncoder.encode(parse(pattern));
        actual = ExponentialBinAdder.addBins(actual);
        actual = ZeroTrimmer.trim(actual);
        assertThat(actual).asTreeString().isEqualTo(expectedPattern);
    }
}
