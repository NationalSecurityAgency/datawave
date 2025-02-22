package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;

class ExponentialBinAdderTest {
    
    @Nested
    class PositiveVariants {
        
        @Test
        void testLeadingZerosWithoutDecimalPoint() {
            // Test leading explicit zero.
            assertBins("00345.*", "\\+[c-z]E00345.*");
            
            // Test digit character class.
            assertBins("\\d0345.*", "\\+[c-z]E\\d0345.*");
            
            // Test leading character class that only matches zero.
            assertBins("[0]0345.*", "\\+[c-z]E[0]0345.*");
            
            // Test leading character class that can match zero and other numbers.
            assertBins("[05-7]0345.*", "\\+[c-z]E[05-7]0345.*");
            
            // Test leading character class that cannot match zero.
            assertBins("[5-7]0345.*", "\\+[e-z]E[5-7]0345.*");
            
            // Test leading potential zero without multi-wildcard to test upper bin correctness.
            assertBins("0[0-6]06", "\\+[a-c]E0[0-6]06");
            
            // Test leading potential zeros with non-multi wildcards.
            assertBins("0[0-6]0.06", "\\+[a-eY]E0[0-6]0.06");
            
            // Test leading zeros with repetition quantifiers.
            assertBins("[06]{3}", "\\+[a-c]E[06]{3}");
            assertBins("[06]{3}[01789]{5}", "\\+[a-h]E[06]{3}[01789]{5}");
            assertBins("[06]{3}[01789]{5}[124678]", "\\+[a-i]E[06]{3}[01789]{5}[124678]");
        }
        
        @Test
        void testLeadingZerosWithDecimalPoint() {
            // Test leading explicit zero.
            assertBins("00\\.00345.*", "\\+XE00\\.00345.*");
            
            // Test leading character class that only matches zero.
            assertBins("[0]0\\.0[0]345.*", "\\+XE[0]0\\.0[0]345.*");
            
            // Test leading character class that can match zero and other numbers.
            assertBins("[05-7]0\\.[045]0345.*", "\\+[bX-Z]E[05-7]0\\.[045]0345.*");
            
            // Test leading character class that cannot match zero.
            assertBins("0\\.0[5-7]0345.*", "\\+YE0\\.0[5-7]0345.*");
            
            // Test leading zeros with repetition quantifiers.
            assertBins("\\.[06]{3}", "\\+[W-Z]E\\.[06]{3}");
            assertBins("[06]{3}\\.[01789]{5}[124678]", "\\+[a-cU-Z]E[06]{3}\\.[01789]{5}[124678]");
            assertBins("[06]{3}\\.[01789]{5}[124678]", "\\+[a-cU-Z]E[06]{3}\\.[01789]{5}[124678]");
            assertBins("\\.00[06]{3}.{3}", "\\+[R-X]E\\.00[06]{3}.{3}");
            assertBins("\\.00[06]*", "\\+[A-X]E\\.00[06]*");
            assertBins("\\.00[06]+", "\\+[A-X]E\\.00[06]+");
        }
        
        @Test
        void testWildcards() {
            // Test a wildcard in the middle of a whole number.
            assertBins("234.65", "\\+[c-f]E234.65");
            
            // Test a wildcard before a decimal point. It should count towards the bin.
            assertBins("234.65\\.045", "\\+fE234.65\\.045");
            
            // Test a wildcard after a decimal point. It should not count towards the bin.
            assertBins("234\\.0.", "\\+cE234\\.0.");
            
            // Test multiple wildcards. The first location of a wildcard should mark the smallest bin number, since we could match against a number with a
            // decimal
            // point there.
            assertBins("87.43.33.33.", "\\+[b-l]E87.43.33.33.");
            
            // Test single wildcard.
            assertBins(".", "\\+aE.");
            
            // Test single wildcard at beginning up to max bin.
            assertBins(".3333333333333333333333333", "\\+[y-zZ]E.3333333333333333333333333");
            
            // Test leading zeros with wildcards.
            assertBins("0\\.000.000.34", "\\+[R-W]E0\\.000.000.34");
            
            assertBins(".*54", "\\+[b-zA-Z]E.*54");
            
            assertBins(".+54", "\\+[b-zA-Z]E.+54");
        }
        
        @Test
        void testCharacterClasses() {
            // Test a character class in the middle of a whole number.
            assertBins("23[3-6]65", "\\+eE23[3-6]65");
            
            // Test a character class before a decimal point. It should count towards the bin.
            assertBins("[3-6]342\\.34", "\\+dE[3-6]342\\.34");
            
            // Test a character class after a decimal point. It should not count towards the bin.
            assertBins("234\\.3[3-6]", "\\+cE234\\.3[3-6]");
            
            // Test multiple character classes.
            assertBins("[3][3-5]35[54]", "\\+eE[3][3-5]35[54]");
            
            // Test min GTEOne bin.
            assertBins("[3-5]", "\\+aE[3-5]");
            
            // Test max GTEOne bin.
            assertBins("[3-5]3333333333333333333333333", "\\+zE[3-5]3333333333333333333333333");
            
            // Test min LTOne bin.
            assertBins("\\.[3-5]", "\\+ZE\\.[3-5]");
            
            // Test max LTOne bin.
            assertBins("\\.0000000000000000000000000[3-5]", "\\+AE\\.0000000000000000000000000[3-5]");
            
            // Test character classes that can be leading zeroes for numbers less than one.
            assertBins("0\\.[0-4][03-5][045]3", "\\+[W-Z]E0\\.[0-4][03-5][045]3");
        }
        
        @Test
        void testRepetitions() {
            // Test a character class in the middle of a whole number.
            assertBins("23{2}65", "\\+eE23{2}65");
            
            // Test a character class range before a decimal point. It should count towards the bin.
            assertBins("3{4,6}42\\.34", "\\+[f-h]E3{4,6}42\\.34");
            
            // Test a character class after decimal point for a non-leading zero character. It should not count towards the bin if it's not after a leading
            // zero.
            assertBins("234\\.3{3}", "\\+cE234\\.3{3}");
            
            // Test repetitions after characters that can be a leading zero.
            assertBins("0\\.0{3}34", "\\+WE0\\.0{3}34");
            assertBins("0\\.0{0,5}34", "\\+[U-Z]E0\\.0{0,5}34");
            assertBins("0\\.0{25}34", "\\+AE0\\.0{25}34");
            assertBins("0\\..{3}34", "\\+[W-Z]E0\\..{3}34");
            assertBins("0\\.[0-4]{0,5}34", "\\+[U-Z]E0\\.[0-4]{0,5}34");
            
            // Test that range {0,} is treated like .*.
            assertBins("5{0,}4", "\\+[a-z]E5{0,}4");
            assertBins("\\.0{0,}4", "\\+[A-Z]E\\.0{0,}4");
        }
        
        @Test
        void testZeroOrMoreQuantifier() {
            // Test .* at the start of a number.
            assertBins(".*34", "\\+[b-zA-Z]E.*34");
            
            // Test .* at the end of a number.
            assertBins("34.*", "\\+[b-z]E34.*");
            
            // Test .* in the middle of a number.
            assertBins("343.*3", "\\+[c-z]E343.*3");
            
            // Test .* after a leading zero.
            assertBins("0.*343", "\\+[c-zA-Z]E0.*343");
            
            // Test .* after leading zero after decimal point.
            assertBins("0\\.0.*34", "\\+[A-Y]E0\\.0.*34");
        }
        
        @Test
        void testOneOrMoreQuantifier() {
            // Test .+ at the start of a number.
            assertBins(".+34", "\\+[b-zA-Z]E.+34");
            
            // Test .+ at the end of a number.
            assertBins("34.+", "\\+[b-z]E34.+");
            
            // Test .+ in the middle of a number.
            assertBins("343.+3", "\\+[c-z]E343.+3");
            
            // Test .+ after a leading zero.
            assertBins("0.+343", "\\+[c-zA-Z]E0.+343");
            
            // Test .+ after leading zero after decimal point.
            assertBins("0\\.0.+34", "\\+[A-Y]E0\\.0.+34");
        }
        
        @Test
        void testSingleElementPatterns() {
            assertBins(".", "\\+aE.");
            assertBins(".*", "\\+[a-zA-Z]E.*");
            assertBins(".*?", "\\+[a-zA-Z]E.*?");
            assertBins(".+", "\\+[a-zA-Z]E.+");
            assertBins(".+?", "\\+[a-zA-Z]E.+?");
            assertBins("[14]", "\\+aE[14]");
            assertBins("[14]{3}", "\\+cE[14]{3}");
            assertBins("\\d", "\\+aE\\d");
            assertBins("\\d{3}", "\\+[a-c]E\\d{3}");
        }
        
        @Test
        void testZerosAfterPossibleLeadingZero() {
            assertBins("[05-7]000\\.34.*", "\\+[dZ]E[05-7]000\\.34.*");
            
            assertBins("0.00\\.*", "\\+[cZ]E0.00\\.*");
            
            // Test zeros after a single wildcard.
            assertBins(".0000\\.34.*", "\\+[eZ]E.0000\\.34.*");
            
            // Test zeros after .*.
            assertBins(".*0000\\.34.*", "\\+[e-zZ]E.*0000\\.34.*");
            
            // Test zeros after .+.
            assertBins(".+0000\\.34.*", "\\+[e-zZ]E.+0000\\.34.*");
            
            // Test zeros after multiple wildcards.
            assertBins("...0000\\.34.*", "\\+[e-gZ]E...0000\\.34.*");
            
            // Test zeros after a digit character class.
            assertBins("\\d0000\\.34.*", "\\+[eZ]E\\d0000\\.34.*");
            
            // Test zeros after a character class containing zero.
            assertBins("[0-4]0000\\.34.*", "\\+[eZ]E[0-4]0000\\.34.*");
            
            // Test zeros that are single char classes.
            assertBins(".[0][0][0][0]\\.34.*", "\\+[eZ]E.[0][0][0][0]\\.34.*");
            
            // Test only zeros in single char classes.
            assertBins("[0][0][0]\\.34.*", "\\+ZE[0][0][0]\\.34.*");
        }
        
        @Test
        void testLeadingQuantifiers() {
            assertBins(".*54", "\\+[b-zA-Z]E.*54");
            assertBins(".+54", "\\+[b-zA-Z]E.+54");
            assertBins(".{3}54", "\\+[b-eW-Z]E.{3}54");
            assertBins(".{0,3}54", "\\+[b-eW-Z]E.{0,3}54");
            assertBins(".{2,5}54", "\\+[b-gU-Z]E.{2,5}54");
            assertBins(".{2,}54", "\\+[b-zA-Z]E.{2,}54");
            
            assertBins(".*0054", "\\+[b-zA-Z]E.*0054");
            assertBins(".+0054", "\\+[b-zA-Z]E.+0054");
            assertBins(".{3}0054", "\\+[b-gU-Z]E.{3}0054");
            assertBins(".{0,3}0054", "\\+[b-gU-Z]E.{0,3}0054");
            assertBins(".{2,5}0054", "\\+[b-iS-Z]E.{2,5}0054");
            assertBins(".{2,}0054", "\\+[b-zA-Z]E.{2,}0054");
        }
        
        @Test
        void testMixedWildcardLocations() {
            assertBins("54.00", "\\+[b-e]E54.00");
            assertBins("00.", "\\+aE00.");
            assertBins("00.033.324", "\\+[b-hY]E00.033.324");
            assertBins("00.034323.34", "\\+[e-jY]E00.034323.34");
            assertBins("00.0[06][09].33", "\\+[b-gV-Y]E00.0[06][09].33");
            assertBins(".000.0\\.34.*", "\\+[b-fZ]E.000.0\\.34.*");
        }
    }
    
    @Nested
    class NegativeVariants {
        
        @Test
        void testLeadingZerosWithoutDecimalPoint() {
            // Test leading explicit zero.
            assertBins("-00345.*", "![A-X]E00345.*");
            
            // Test digit character class.
            assertBins("-\\d0345.*", "![A-X]E\\d0345.*");
            
            // Test leading character class that only matches zero.
            assertBins("-[0]0345.*", "![A-X]E[0]0345.*");
            
            // Test leading character class that can match zero and other numbers.
            assertBins("-[05-7]0345.*", "![A-X]E[05-7]0345.*");
            
            // Test leading character class that cannot match zero.
            assertBins("-[5-7]0345.*", "![A-V]E[5-7]0345.*");
            
            // Test leading potential zero without multi-wildcard to test upper bin correctness.
            assertBins("-0[0-6]06", "![X-Z]E0[0-6]06");
            
            // Test leading potential zeros with non-multi wildcards.
            assertBins("-0[0-6]0.06", "![V-Zb]E0[0-6]0.06");
            
            // Test leading zeros with repetition quantifiers.
            assertBins("-[06]{3}", "![X-Z]E[06]{3}");
            assertBins("-[06]{3}[01789]{5}", "![S-Z]E[06]{3}[01789]{5}");
            assertBins("-[06]{3}[01789]{5}[124678]", "![R-Z]E[06]{3}[01789]{5}[124678]");
        }
        
        @Test
        void testLeadingZerosWithDecimalPoint() {
            // Test leading explicit zero.
            assertBins("-00\\.00345.*", "!cE00\\.00345.*");
            
            // Test leading character class that only matches zero.
            assertBins("-[0]0\\.0[0]345.*", "!cE[0]0\\.0[0]345.*");
            
            // Test leading character class that can match zero and other numbers.
            assertBins("-[05-7]0\\.[045]0345.*", "![Ya-c]E[05-7]0\\.[045]0345.*");
            
            // Test leading character class that cannot match zero.
            assertBins("-0\\.0[5-7]0345.*", "!bE0\\.0[5-7]0345.*");
            
            // Test leading zeros with repetition quantifiers.
            assertBins("-\\.[06]{3}", "![a-d]E\\.[06]{3}");
            assertBins("-[06]{3}\\.[01789]{5}[124678]", "![X-Za-f]E[06]{3}\\.[01789]{5}[124678]");
            assertBins("-[06]{3}\\.[01789]{5}[124678]", "![X-Za-f]E[06]{3}\\.[01789]{5}[124678]");
        }
        
        @Test
        void testWildcards() {
            // Test a wildcard in the middle of a whole number.
            assertBins("-234.65", "![U-X]E234.65");
            
            // Test a wildcard before a decimal point. It should count towards the bin.
            assertBins("-234.65\\.045", "!UE234.65\\.045");
            
            // Test a wildcard after a decimal point. It should not count towards the bin.
            assertBins("-234\\.0.", "!XE234\\.0.");
            
            // Test multiple wildcards. The first location of a wildcard should mark the smallest bin number, since we could match against a number with a
            // decimal
            // point there.
            assertBins("-87.43.33.33.", "![O-Y]E87.43.33.33.");
            
            // Test min bin.
            assertBins("-.", "!ZE.");
            
            // Test max bin.
            assertBins("-.3333333333333333333333333", "![A-Ba]E.3333333333333333333333333");
            
            assertBins("-0\\.000.000.34", "![d-i]E0\\.000.000.34");
        }
        
        @Test
        void testCharacterClasses() {
            // Test a character class in the middle of a whole number.
            assertBins("-23[3-6]65", "!VE23[3-6]65");
            
            // Test a character class before a decimal point. It should count towards the bin.
            assertBins("-[3-6]342\\.34", "!WE[3-6]342\\.34");
            
            // Test a character class after a decimal point. It should not count towards the bin.
            assertBins("-234\\.3[3-6]", "!XE234\\.3[3-6]");
            
            // Test multiple character classes.
            assertBins("-[3][3-5]35[54]", "!VE[3][3-5]35[54]");
            
            // Test min GTEOne bin.
            assertBins("-[3-5]", "!ZE[3-5]");
            
            // Test max GTEOne bin.
            assertBins("-[3-5]3333333333333333333333333", "!AE[3-5]3333333333333333333333333");
            
            // Test min LTOne bin.
            assertBins("-\\.[3-5]", "!aE\\.[3-5]");
            
            // Test max LTOne bin.
            assertBins("-\\.0000000000000000000000000[3-5]", "!zE\\.0000000000000000000000000[3-5]");
            
            // Test character classes that can be leading zeroes for numbers less than one.
            assertBins("-0\\.[0-4][03-5][045]3", "![a-d]E0\\.[0-4][03-5][045]3");
        }
        
        @Test
        void testRepetitions() {
            // Test a character class in the middle of a whole number.
            assertBins("-23{2}65", "!VE23{2}65");
            
            // Test a character class range before a decimal point. It should count towards the bin.
            assertBins("-3{4,6}42\\.34", "![S-U]E3{4,6}42\\.34");
            
            // Test a character class after decimal point for a non-leading zero character. It should not count towards the bin if it's not after a leading
            // zero.
            assertBins("-234\\.3{3}", "!XE234\\.3{3}");
            
            // Test repetitions after characters that can be a leading zero.
            assertBins("-0\\.0{3}34", "!dE0\\.0{3}34");
            assertBins("-0\\.0{0,5}34", "![a-f]E0\\.0{0,5}34");
            assertBins("-0\\.0{25}34", "!zE0\\.0{25}34");
            assertBins("-0\\..{3}34", "![a-d]E0\\..{3}34");
            assertBins("-0\\.[0-4]{0,5}34", "![a-f]E0\\.[0-4]{0,5}34");
            
            // Test that range {0,} is treated like .*.
            assertBins("-5{0,}4", "![A-Z]E5{0,}4");
            assertBins("-\\.0{0,}4", "![a-z]E\\.0{0,}4");
        }
        
        @Test
        void testZeroOrMoreQuantifier() {
            // Test .* at the start of a number.
            assertBins("-.*34", "![A-Ya-z]E.*34");
            
            // Test .* at the end of a number.
            assertBins("-34.*", "![A-Y]E34.*");
            
            // Test .* in the middle of a number.
            assertBins("-343.*3", "![A-X]E343.*3");
            
            // Test .* after a leading zero.
            assertBins("-0.*343", "![A-Xa-z]E0.*343");
            
            // Test .* after leading zero after decimal point.
            assertBins("-0\\.0.*34", "![b-z]E0\\.0.*34");
        }
        
        @Test
        void testOneOrMoreQuantifier() {
            // Test .+ at the start of a number.
            assertBins("-.+34", "![A-Ya-z]E.+34");
            
            // Test .+ at the end of a number.
            assertBins("-34.+", "![A-Y]E34.+");
            
            // Test .+ in the middle of a number.
            assertBins("-343.+3", "![A-X]E343.+3");
            
            // Test .+ after a leading zero.
            assertBins("-0.+343", "![A-Xa-z]E0.+343");
            
            // Test .+ after leading zero after decimal point.
            assertBins("-0\\.0.+34", "![b-z]E0\\.0.+34");
        }
    }
    
    @Test
    void testEncodedNumbersAreNotModified() {
        Node tree = SimpleNumberEncoder.encode(parse("234|54.*"));
        Node enriched = ExponentialBinAdder.addBins(tree);
        assertThat(enriched).asTreeString().isEqualTo("\\+cE2\\.34|\\+[b-z]E54.*");
        // Validate the tree structure.
        // @formatter:off
        assertThat(enriched).assertChild(0).isAlternationNode()
                        .assertChild(0).isEncodedNumberNode().assertParent()
                        .assertChild(1).isEncodedPatternNode();
        // @formatter:on
    }
    
    private void assertBins(String pattern, String expectedPattern) {
        Node actual = ExponentialBinAdder.addBins(parse(pattern));
        assertThat(actual).asTreeString().isEqualTo(expectedPattern);
    }
}
