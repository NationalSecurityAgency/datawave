package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;

class DecimalPointPlacerTest {
    
    @Nested
    class PositiveVariants {
        
        @Test
        void testSimpleNumbers() {
            assertAdded("234", "\\+cE2\\.34");
            assertAdded("234|454", "\\+cE2\\.34|\\+cE4\\.54");
        }
        
        @Test
        void testSingleLengthPatterns() {
            assertAdded("[3-9]", "\\+aE[3-9]");
            assertAdded("\\d", "\\+aE\\d");
            assertAdded(".", "\\+aE.");
        }
        
        @Test
        void testLeadingMultiWildcards() {
            assertAdded(".*", "\\+[a-zA-Z]E.*");
            assertAdded(".*?", "\\+[a-zA-Z]E.*?");
            assertAdded(".+", "\\+[a-zA-Z]E.+");
            assertAdded(".+?", "\\+[a-zA-Z]E.+?");
            
            // In the case of .*, allow for a possible decimal point occurring after it, and after the next character.
            assertAdded(".*454", "\\+[c-zA-Z]E.*4\\.?54");
            assertAdded(".*?45", "\\+[b-zA-Z]E.*?4\\.?5");
            
            // In the case of a leading .+, allow for a possible decimal point occurring after it, and after the next character. We must account for when .+
            // might
            // be a decimal point, such as for the number 0.343.
            assertAdded(".+343", "\\+[c-zA-Z]E.*3\\.?43");
            assertAdded(".+?343", "\\+[c-zA-Z]E.*?3\\.?43");
        }
        
        @Test
        void testConsolidatedZeros() {
            assertAdded(".*000004", "\\+[a-zA-Z]E.*(0{5})?4");
        }
        
        @Test
        void testLeadingQuantifiersForSingleChar() {
            assertAdded("4*11", "\\+[b-z]E4?\\.?4*1\\.?1");
            assertAdded("4+11", "\\+[c-z]E4\\.?4*11");
            assertAdded("4{3}11", "\\+eE4\\.4{2}11");
            assertAdded("4{3,5}11", "\\+[e-g]E4\\.4{2,4}11");
            assertAdded("4{1,5}11", "\\+[c-g]E4\\.4{0,4}11");
            assertAdded("4{1,}11", "\\+[c-z]E4\\.?4*11");
            assertAdded("4{2,}11", "\\+[d-z]E4\\.4+11");
            assertAdded("4{1,2}11", "\\+[c-d]E4\\.4{0,1}11");
            assertAdded("4{1}11", "\\+cE4\\.11");
            assertAdded("4{2}11", "\\+dE4\\.411");
            assertAdded("4{0,5}11", "\\+[b-g]E4?\\.4{0,4}11");
            
            assertAdded("4{1,5}.*", "\\+[a-z]E4\\.?4{0,4}.*");
            assertAdded("4{1,2}.*", "\\+[a-z]E4\\.?4{0,1}.*");
            assertAdded("4{0,5}.*", "\\+[a-z]E4?\\.?4{0,4}.*");
            
            assertAdded("4{1,5}", "\\+[a-e]E4\\.?4{0,4}");
            assertAdded("4{1,2}", "\\+[a-b]E4\\.?4{0,1}");
            assertAdded("4{0,5}", "\\+[a-e]E4?\\.?4{0,4}");
        }
        
        @Test
        void testLeadingQuantifiersForWildcard() {
            assertAdded(".*11", "\\+[b-zA-Z]E.*1\\.?1");
            assertAdded(".+11", "\\+[b-zA-Z]E.*1\\.?1");
            assertAdded(".{3}11", "\\+[b-eW-Z]E.?\\.?.{0,2}1\\.?1");
            assertAdded(".{3,5}11", "\\+[b-gU-Z]E(.\\.?.{2,4})?1\\.?1");
            assertAdded(".{1,5}11", "\\+[b-gU-Z]E(.\\.?.{0,4})?1\\.?1");
            assertAdded(".{1,}11", "\\+[b-zA-Z]E(.\\.?.*)?1\\.?1");
            assertAdded(".{2,}11", "\\+[b-zA-Z]E(.\\.?.+)?1\\.?1");
            assertAdded(".{1,2}11", "\\+[b-dX-Z]E(.\\.?.{0,1})?1\\.?1");
            assertAdded(".{1}11", "\\+[b-cY-Z]E.?\\.?1\\.?1");
            assertAdded(".{2}11", "\\+[b-dX-Z]E.?\\.?.{0,1}1\\.?1");
            assertAdded(".{0,5}11", "\\+[b-gU-Z]E.?\\.?.{0,4}1\\.?1");
            
            assertAdded(".{1,5}.*", "\\+[a-zA-Z]E(.\\.?.{0,4})?.*");
            assertAdded(".{1,2}.*", "\\+[a-zA-Z]E(.\\.?.{0,1})?.*");
            assertAdded(".{0,5}.*", "\\+[a-zA-Z]E.?\\.?.{0,4}.*");
            
            assertAdded(".{1,5}", "\\+[a-eU-Z]E.\\.?.{0,4}");
            assertAdded(".{1,2}", "\\+[a-bX-Z]E.\\.?.{0,1}");
            assertAdded(".{0,5}", "\\+[a-eU-Z]E.?\\.?.{0,4}");
        }
        
        @Test
        void testLeadingQuantifiersForDigitCharClass() {
            assertAdded("\\d*11", "\\+[b-z]E\\d?\\.?\\d*1\\.?1");
            assertAdded("\\d+11", "\\+[b-z]E\\d?\\.?\\d*1\\.?1");
            assertAdded("\\d{3}11", "\\+[b-e]E\\d?\\.?\\d{0,2}1\\.?1");
            assertAdded("\\d{3,5}11", "\\+[b-g]E(\\d\\.?\\d{2,4})?1\\.?1");
            assertAdded("\\d{1,5}11", "\\+[b-g]E(\\d\\.?\\d{0,4})?1\\.?1");
            assertAdded("\\d{1,}11", "\\+[b-z]E(\\d\\.?\\d*)?1\\.?1");
            assertAdded("\\d{2,}11", "\\+[b-z]E(\\d\\.?\\d+)?1\\.?1");
            assertAdded("\\d{1,2}11", "\\+[b-d]E(\\d\\.?\\d{0,1})?1\\.?1");
            assertAdded("\\d{1}11", "\\+[b-c]E\\d?\\.?1\\.?1");
            assertAdded("\\d{2}11", "\\+[b-d]E\\d?\\.?\\d{0,1}1\\.?1");
            assertAdded("\\d{0,5}11", "\\+[b-g]E\\d?\\.?\\d{0,4}1\\.?1");
            
            assertAdded("\\d{1,5}.*", "\\+[a-zA-Z]E(\\d\\.?\\d{0,4})?.*");
            assertAdded("\\d{1,2}.*", "\\+[a-zA-Z]E(\\d\\.?\\d{0,1})?.*");
            assertAdded("\\d{0,5}.*", "\\+[a-zA-Z]E\\d?\\.?\\d{0,4}.*");
            
            assertAdded("\\d{1,5}", "\\+[a-e]E\\d\\.?\\d{0,4}");
            assertAdded("\\d{1,2}", "\\+[a-b]E\\d\\.?\\d{0,1}");
            assertAdded("\\d{0,5}", "\\+[a-e]E\\d?\\.?\\d{0,4}");
        }
        
        @Test
        void testLeadingQuantifiersForCharClassContainingZero() {
            assertAdded("[012]*11", "\\+[b-z]E[012]?\\.?[012]*1\\.?1");
            assertAdded("[012]+11", "\\+[b-z]E[012]?\\.?[012]*1\\.?1");
            assertAdded("[012]{3}11", "\\+[b-e]E[012]?\\.?[012]{0,2}1\\.?1");
            assertAdded("[012]{3,5}11", "\\+[b-g]E([012]\\.?[012]{2,4})?1\\.?1");
            assertAdded("[012]{1,5}11", "\\+[b-g]E([012]\\.?[012]{0,4})?1\\.?1");
            assertAdded("[012]{1,}11", "\\+[b-z]E([012]\\.?[012]*)?1\\.?1");
            assertAdded("[012]{2,}11", "\\+[b-z]E([012]\\.?[012]+)?1\\.?1");
            assertAdded("[012]{1,2}11", "\\+[b-d]E([012]\\.?[012]{0,1})?1\\.?1");
            assertAdded("[012]{1}11", "\\+[b-c]E[012]?\\.?1\\.?1");
            assertAdded("[012]{2}11", "\\+[b-d]E[012]?\\.?[012]{0,1}1\\.?1");
            assertAdded("[012]{0,5}11", "\\+[b-g]E[012]?\\.?[012]{0,4}1\\.?1");
            
            assertAdded("[012]{1,5}.*", "\\+[a-zA-Z]E([012]\\.?[012]{0,4})?.*");
            assertAdded("[012]{1,2}.*", "\\+[a-zA-Z]E([012]\\.?[012]{0,1})?.*");
            assertAdded("[012]{0,5}.*", "\\+[a-zA-Z]E[012]?\\.?[012]{0,4}.*");
            
            assertAdded("[012]{1,5}", "\\+[a-e]E[012]\\.?[012]{0,4}");
            assertAdded("[012]{1,2}", "\\+[a-b]E[012]\\.?[012]{0,1}");
            assertAdded("[012]{0,5}", "\\+[a-e]E[012]?\\.?[012]{0,4}");
        }
        
        @Test
        void testLeadingQuantifiersForCharClassNotContainingZero() {
            assertAdded("[24]*11", "\\+[b-z]E[24]?\\.?[24]*1\\.?1");
            assertAdded("[24]+11", "\\+[c-z]E[24]\\.?[24]*11");
            assertAdded("[24]{3}11", "\\+eE[24]\\.[24]{2}11");
            assertAdded("[24]{3,5}11", "\\+[e-g]E[24]\\.[24]{2,4}11");
            assertAdded("[24]{1,5}11", "\\+[c-g]E[24]\\.[24]{0,4}11");
            assertAdded("[24]{1,}11", "\\+[c-z]E[24]\\.?[24]*11");
            assertAdded("[24]{2,}11", "\\+[d-z]E[24]\\.[24]+11");
            assertAdded("[24]{1,2}11", "\\+[c-d]E[24]\\.[24]{0,1}11");
            assertAdded("[24]{1}11", "\\+cE[24]\\.11");
            assertAdded("[24]{2}11", "\\+dE[24]\\.[24]11");
            assertAdded("[24]{0,5}11", "\\+[b-g]E[24]?\\.[24]{0,4}11");
            
            assertAdded("[24]{1,5}.*", "\\+[a-z]E[24]\\.?[24]{0,4}.*");
            assertAdded("[24]{1,2}.*", "\\+[a-z]E[24]\\.?[24]{0,1}.*");
            assertAdded("[24]{0,5}.*", "\\+[a-z]E[24]?\\.?[24]{0,4}.*");
            
            assertAdded("[24]{1,5}", "\\+[a-e]E[24]\\.?[24]{0,4}");
            assertAdded("[24]{1,2}", "\\+[a-b]E[24]\\.?[24]{0,1}");
            assertAdded("[24]{0,5}", "\\+[a-e]E[24]?\\.?[24]{0,4}");
        }
        
        /**
         * Test patterns that have multiple possible leading zero elements that must all be made optional.
         */
        @Test
        void testMultiplePossibleLeadingZeros() {
            assertAdded("[30].\\d", "\\+[a-cY-Z]E[30]?\\.?.?\\.?\\d?");
            assertAdded("[30]\\d..*", "\\+[a-zA-Z]E[30]?\\.?\\d?\\.?.?\\.?.*");
            assertAdded(".*54", "\\+[b-zA-Z]E.*5\\.?4");
            assertAdded(".+54", "\\+[b-zA-Z]E.*5\\.?4");
            assertAdded("[04]{3}[05]{2}[06]", "\\+[a-f]E[04]?\\.?[04]{0,2}[05]?\\.?[05]{0,1}[06]?");
            assertAdded("[04]{3}[05]{2}[06].*.+43", "\\+[b-zA-Z]E[04]?\\.?[04]{0,2}[05]?\\.?[05]{0,1}[06]?\\.?.*.*4\\.?3");
            assertAdded("[04]{3}0000[05]{2}[06].*.+43", "\\+[b-zA-Z]E[04]?\\.?[04]{0,2}(0{4})?[05]?\\.?[05]{0,1}[06]?\\.?.*.*4\\.?3");
            assertAdded("[04]{3}0000[05]{2}[06][08]{3,5}.*.+43",
                            "\\+[b-zA-Z]E[04]?\\.?[04]{0,2}(0{4})?[05]?\\.?[05]{0,1}[06]?\\.?([08]\\.?[08]{2,4})?.*.*4\\.?3");
        }
        
        /**
         * Test patterns similar to those in {@link #testMultiplePossibleLeadingZeros()}, but with a non-leading zero at the beginning, and verify that none of
         * the elements after the non-leading zero are made optional.
         */
        @Test
        void testMultipleNonLeadingZeros() {
            assertAdded("3[30]\\d..*", "\\+[c-z]E3\\.?[30]?\\d?.?.*");
            assertAdded("3.*54", "\\+[a-z]E3\\..*54");
            assertAdded("3.+54", "\\+[a-z]E3\\..+54");
            assertAdded("3[04]{3}[05]{2}[06]", "\\+gE3\\.?[04]{0,3}[05]{0,2}[06]?");
            assertAdded("3[04]{3}[05]{2}[06].*.+43", "\\+[g-z]E3\\.[04]{3}[05]{2}[06].*.+43");
            assertAdded("3[04]{3}0000[05]{2}[06].*.+43", "\\+[k-z]E3\\.[04]{3}0000[05]{2}[06].*.+43");
            assertAdded("3[04]{3}0000[05]{2}[06][08]{3,5}.*.+43", "\\+[n-z]E3\\.[04]{3}0000[05]{2}[06][08]{3,5}.*.+43");
        }
        
        /**
         * Test patterns similar to those in {@link #testMultiplePossibleLeadingZeros()}, but with a non-leading zero somewhere in the middle. Verify that any
         * possible zeros before the first non-leading zeros are made optional, but any succeeding possible zeros are not made optional.
         */
        @Test
        void testMixedLeadingAndNonLeadingZeros() {
            assertAdded("[30]\\d..*34[05]{2}[04]", "\\+[e-zA-Z]E[30]?\\.?\\d?\\.?.?\\.?.*3\\.?4[05]{0,2}[04]?");
            assertAdded(".*[05]{2}5[05]{2}4", "\\+[d-zA-Z]E.*[05]?\\.?[05]{0,1}5\\.?[05]{2}4");
            assertAdded(".+[05]{2}54[05]{2}", "\\+[d-zA-Z]E.*[05]?\\.?[05]{0,1}5\\.?4[05]{0,2}");
            assertAdded("[04]{3}[05]{2}33[06][05]{2}", "\\+[e-j]E[04]?\\.?[04]{0,2}[05]?\\.?[05]{0,1}3\\.?3[06]?[05]{0,2}");
            assertAdded("[04]{3}[05]{2}33[06][05]{2}.*.+", "\\+[e-z]E[04]?\\.?[04]{0,2}[05]?\\.?[05]{0,1}3\\.?3[06]?[05]{0,2}.*.*");
            assertAdded("[04]{3}0000[05]{2}33[06].*.+", "\\+[c-z]E[04]?\\.?[04]{0,2}(0{4})?[05]?\\.?[05]{0,1}3\\.?3[06]?.*.*");
            assertAdded("[04]{3}0000[05]{2}33[06][08]{3,5}.*.+", "\\+[f-z]E[04]?\\.?[04]{0,2}(0{4})?[05]?\\.?[05]{0,1}3\\.?3[06]?([08]{3,5})?.*.*");
        }
    }
    
    @Nested
    class NegativeVariants {
        
        @Test
        void testSimpleNumbers() {
            assertAdded("-234", "!XE7\\.66");
            assertAdded("-234|-454", "!XE7\\.66|!XE5\\.46");
        }
        
        @Test
        void testSingleLengthPatterns() {
            assertAdded("-[3-9]", "!ZE[1-7]");
            assertAdded("-\\d", "!ZE\\d");
            assertAdded("-.", "!ZE.");
        }
        
        @Test
        void testLeadingMultiWildcards() {
            assertAdded("-.*", "![A-Za-z]E.+");
            assertAdded("-.*?", "![A-Za-z]E.+?");
            assertAdded("-.+", "![A-Za-z]E.+");
            assertAdded("-.+?", "![A-Za-z]E.+?");
            
            // In the case of .*, allow for a possible decimal point occurring after it, and after the next character.
            assertAdded("-.*454", "![A-Xa-z]E.*5\\.?46");
            assertAdded("-.*?45", "![A-Ya-z]E.*?5\\.?5");
            
            // In the case of a leading .+, allow for a possible decimal point occurring after it, and after the next character. We must account for when .+
            // might
            // be a decimal point, such as for the number 0.343.
            assertAdded("-.+343", "![A-Xa-z]E.*6\\.?57");
            assertAdded("-.+?343", "![A-Xa-z]E.*?6\\.?57");
        }
        
        @Test
        void testConsolidatedZeros() {
            assertAdded("-.*000004", "![A-Za-z]E.*(9{5})?6");
        }
        
        @Test
        void testLeadingQuantifiersForSingleChar() {
            assertAdded("-4*11", "![A-Y]E5?\\.?5*8\\.?9");
            assertAdded("-4+11", "![A-X]E5\\.?5*89");
            assertAdded("-4{3}11", "!VE5\\.5{2}89");
            assertAdded("-4{3,5}11", "![T-V]E5\\.5{2,4}89");
            assertAdded("-4{1,5}11", "![T-X]E5\\.5{0,4}89");
            assertAdded("-4{1,}11", "![A-X]E5\\.?5*89");
            assertAdded("-4{2,}11", "![A-W]E5\\.5+89");
            assertAdded("-4{1,2}11", "![W-X]E5\\.5{0,1}89");
            assertAdded("-4{1}11", "!XE5\\.89");
            assertAdded("-4{2}11", "!WE5\\.589");
            assertAdded("-4{0,5}11", "![T-Y]E5?\\.5{0,4}89");
            
            assertAdded("-4{1,5}.*", "![A-Z]E(5?\\.?5{0,3}6|5\\.?5{0,4}.+)");
            assertAdded("-4{3,5}.*", "![A-X]E(5\\.5{1,3}6|5\\.5{2,4}.+)");
            assertAdded("-4{1,2}.*", "![A-Z]E(5?\\.?6|5\\.?5{0,1}.+)");
            assertAdded("-4{0,5}.*", "![A-Z]E(5?\\.?5{0,3}6|5?\\.?5{0,4}.+)");
            
            assertAdded("-4{1,5}", "![V-Z]E5?\\.?5{0,3}6");
            assertAdded("-4{1,2}", "![Y-Z]E5?\\.?6");
            assertAdded("-4{0,5}", "![V-Z]E5?\\.?5{0,3}6");
        }
        
        @Test
        void testLeadingQuantifiersForWildcard() {
            assertAdded("-.*11", "![A-Ya-z]E.*8\\.?9");
            assertAdded("-.+11", "![A-Ya-z]E.*8\\.?9");
            assertAdded("-.{3}11", "![V-Ya-d]E.?\\.?.{0,2}8\\.?9");
            assertAdded("-.{3,5}11", "![T-Ya-f]E(.\\.?.{2,4})?8\\.?9");
            assertAdded("-.{1,5}11", "![T-Ya-f]E(.\\.?.{0,4})?8\\.?9");
            assertAdded("-.{1,}11", "![A-Ya-z]E(.\\.?.*)?8\\.?9");
            assertAdded("-.{2,}11", "![A-Ya-z]E(.\\.?.+)?8\\.?9");
            assertAdded("-.{1,2}11", "![W-Ya-c]E(.\\.?.{0,1})?8\\.?9");
            assertAdded("-.{1}11", "![X-Ya-b]E.?\\.?8\\.?9");
            assertAdded("-.{2}11", "![W-Ya-c]E.?\\.?.{0,1}8\\.?9");
            assertAdded("-.{0,5}11", "![T-Ya-f]E.?\\.?.{0,4}8\\.?9");
            
            assertAdded("-.{1,5}.*", "![A-Za-z]E(.\\.?.{0,4}|.\\.?.{0,4}.\\.?.*)");
            assertAdded("-.{1,2}.*", "![A-Za-z]E(.\\.?.{0,1}|.\\.?.{0,1}.\\.?.*)");
            assertAdded("-.{0,5}.*", "![A-Za-z]E(.?\\.?.{0,4}|.?\\.?.{0,4}.\\.?.*)");
            
            assertAdded("-.{1,5}", "![V-Za-f]E.\\.?.{0,4}");
            assertAdded("-.{1,2}", "![Y-Za-c]E.\\.?.{0,1}");
            assertAdded("-.{0,5}", "![V-Za-f]E.?\\.?.{0,4}");
        }
        
        @Test
        void testLeadingQuantifiersForDigitCharClass() {
            assertAdded("-\\d*11", "![A-Y]E\\d?\\.?\\d*8\\.?9");
            assertAdded("-\\d+11", "![A-Y]E\\d?\\.?\\d*8\\.?9");
            assertAdded("-\\d{3}11", "![V-Y]E\\d?\\.?\\d{0,2}8\\.?9");
            assertAdded("-\\d{3,5}11", "![T-Y]E(\\d\\.?\\d{2,4})?8\\.?9");
            assertAdded("-\\d{1,5}11", "![T-Y]E(\\d\\.?\\d{0,4})?8\\.?9");
            assertAdded("-\\d{1,}11", "![A-Y]E(\\d\\.?\\d*)?8\\.?9");
            assertAdded("-\\d{2,}11", "![A-Y]E(\\d\\.?\\d+)?8\\.?9");
            assertAdded("-\\d{1,2}11", "![W-Y]E(\\d\\.?\\d{0,1})?8\\.?9");
            assertAdded("-\\d{1}11", "![X-Y]E\\d?\\.?8\\.?9");
            assertAdded("-\\d{2}11", "![W-Y]E\\d?\\.?\\d{0,1}8\\.?9");
            assertAdded("-\\d{0,5}11", "![T-Y]E\\d?\\.?\\d{0,4}8\\.?9");
            
            assertAdded("-\\d{1,5}.*", "![A-Za-z]E(\\d\\.?\\d{0,4}|\\d\\.?\\d{0,4}.\\.?.*)");
            assertAdded("-\\d{1,2}.*", "![A-Za-z]E(\\d\\.?\\d{0,1}|\\d\\.?\\d{0,1}.\\.?.*)");
            assertAdded("-\\d{0,5}.*", "![A-Za-z]E(\\d?\\.?\\d{0,4}|\\d?\\.?\\d{0,4}.\\.?.*)");
            
            assertAdded("-\\d{1,5}", "![V-Z]E\\d\\.?\\d{0,4}");
            assertAdded("-\\d{1,2}", "![Y-Z]E\\d\\.?\\d{0,1}");
            assertAdded("-\\d{0,5}", "![V-Z]E\\d?\\.?\\d{0,4}");
        }
        
        @Test
        void testLeadingQuantifiersForCharClassContainingZero() {
            assertAdded("-[012]*11", "![A-Y]E[987]?\\.?[987]*8\\.?9");
            assertAdded("-[012]+11", "![A-Y]E[987]?\\.?[987]*8\\.?9");
            assertAdded("-[012]{3}11", "![V-Y]E[987]?\\.?[987]{0,2}8\\.?9");
            assertAdded("-[012]{3,5}11", "![T-Y]E([987]\\.?[987]{2,4})?8\\.?9");
            assertAdded("-[012]{1,5}11", "![T-Y]E([987]\\.?[987]{0,4})?8\\.?9");
            assertAdded("-[012]{1,}11", "![A-Y]E([987]\\.?[987]*)?8\\.?9");
            assertAdded("-[012]{2,}11", "![A-Y]E([987]\\.?[987]+)?8\\.?9");
            assertAdded("-[012]{1,2}11", "![W-Y]E([987]\\.?[987]{0,1})?8\\.?9");
            assertAdded("-[012]{1}11", "![X-Y]E[987]?\\.?8\\.?9");
            assertAdded("-[012]{2}11", "![W-Y]E[987]?\\.?[987]{0,1}8\\.?9");
            assertAdded("-[012]{0,5}11", "![T-Y]E[987]?\\.?[987]{0,4}8\\.?9");
            
            assertAdded("-[012]{1,5}.*", "![A-Za-z]E([987]?\\.?[987]{0,3}[98]|[987]\\.?[987]{0,4}.\\.?.*)");
            assertAdded("-[012]{1,2}.*", "![A-Za-z]E([987]?\\.?[98]|[987]\\.?[987]{0,1}.\\.?.*)");
            assertAdded("-[012]{0,5}.*", "![A-Za-z]E([987]?\\.?[987]{0,3}[98]|[987]?\\.?[987]{0,4}.\\.?.*)");
            
            assertAdded("-[012]{1,5}", "![V-Z]E[987]?\\.?[987]{0,3}[98]");
            assertAdded("-[012]{1,2}", "![Y-Z]E[987]?\\.?[98]");
            assertAdded("-[012]{0,5}", "![V-Z]E[987]?\\.?[987]{0,3}[98]");
        }
        
        @Test
        void testLeadingQuantifiersForCharClassNotContainingZero() {
            assertAdded("-[24]*11", "![A-Y]E[75]?\\.?[75]*8\\.?9");
            assertAdded("-[24]+11", "![A-X]E[75]\\.?[75]*89");
            assertAdded("-[24]{3}11", "!VE[75]\\.[75]{2}89");
            assertAdded("-[24]{3,5}11", "![T-V]E[75]\\.[75]{2,4}89");
            assertAdded("-[24]{1,5}11", "![T-X]E[75]\\.[75]{0,4}89");
            assertAdded("-[24]{1,}11", "![A-X]E[75]\\.?[75]*89");
            assertAdded("-[24]{2,}11", "![A-W]E[75]\\.[75]+89");
            assertAdded("-[24]{1,2}11", "![W-X]E[75]\\.[75]{0,1}89");
            assertAdded("-[24]{1}11", "!XE[75]\\.89");
            assertAdded("-[24]{2}11", "!WE[75]\\.[75]89");
            assertAdded("-[24]{0,5}11", "![T-Y]E[75]?\\.[75]{0,4}89");
            
            assertAdded("-[24]{1,5}.*", "![A-Z]E([75]?\\.?[75]{0,3}[86]|[75]\\.?[75]{0,4}.+)");
            assertAdded("-[24]{1,2}.*", "![A-Z]E([75]?\\.?[86]|[75]\\.?[75]{0,1}.+)");
            assertAdded("-[24]{0,5}.*", "![A-Z]E([75]?\\.?[75]{0,3}[86]|[75]?\\.?[75]{0,4}.+)");
            
            assertAdded("-[24]{1,5}", "![V-Z]E[75]?\\.?[75]{0,3}[86]");
            assertAdded("-[24]{1,2}", "![Y-Z]E[75]?\\.?[86]");
            assertAdded("-[24]{0,5}", "![V-Z]E[75]?\\.?[75]{0,3}[86]");
        }
        
        /**
         * Test patterns that have multiple possible leading zero elements that must all be made optional.
         */
        @Test
        void testMultiplePossibleLeadingZeros() {
            assertAdded("-[30].\\d", "![X-Za-b]E(7|[69]\\.?.|[69]\\.?.\\d)");
            assertAdded("-[30]\\d..*", "![A-Za-z]E(7|[69]\\.?\\d|[69]\\.?\\d.|[69]\\.?\\d..+)");
            assertAdded("-.*54", "![A-Ya-z]E.*4\\.?6");
            assertAdded("-.+54", "![A-Ya-z]E.*4\\.?6");
            assertAdded("-[04]{3}[05]{2}[06]", "![U-Z]E([95]?\\.?[95]{0,1}6|[95]?\\.?[95]{0,2}[94]?\\.?5|[95]?\\.?[95]{0,2}[94]?\\.?[94]{0,1}4)");
            assertAdded("-[04]{3}[05]{2}[06].*.+43", "![A-Ya-z]E[95]?\\.?[95]{0,2}[94]?\\.?[94]{0,1}[93]?\\.?.*.*5\\.?7");
            assertAdded("-[04]{3}0000[05]{2}[06].*.+43", "![A-Ya-z]E[95]?\\.?[95]{0,2}(9{4})?[94]?\\.?[94]{0,1}[93]?\\.?.*.*5\\.?7");
            assertAdded("-[04]{3}0000[05]{2}[06][08]{3,5}.*.+43",
                            "![A-Ya-z]E[95]?\\.?[95]{0,2}(9{4})?[94]?\\.?[94]{0,1}[93]?\\.?([91]\\.?[91]{2,4})?.*.*5\\.?7");
        }
        
        /**
         * Test patterns similar to those in {@link #testMultiplePossibleLeadingZeros()}, but with a non-leading zero at the beginning, and verify that none of
         * the elements after the non-leading zero are made optional.
         */
        @Test
        void testMultipleNonLeadingZeros() {
            assertAdded("-3[30]\\d..*", "![A-X]E(7|6\\.7|6\\.?[69]\\d|6\\.?[69]\\d.|6\\.?[69]\\d..+)");
            assertAdded("-3.*54", "![A-Z]E6\\..*46");
            assertAdded("-3.+54", "![A-Z]E6\\..+46");
            assertAdded("-3[04]{3}[05]{2}[06]", "!TE(7|6\\.[95]{0,2}6|6\\.[95]{0,3}[94]{0,1}5|6\\.[95]{0,3}[94]{0,2}4)");
            assertAdded("-3[04]{3}[05]{2}[06].*.+43", "![A-T]E6\\.[95]{3}[94]{2}[93].*.+57");
            assertAdded("-3[04]{3}0000[05]{2}[06].*.+43", "![A-P]E6\\.[95]{3}9999[94]{2}[93].*.+57");
            assertAdded("-3[04]{3}0000[05]{2}[06][08]{3,5}.*.+43", "![A-M]E6\\.[95]{3}9999[94]{2}[93][91]{3,5}.*.+57");
        }
        
        /**
         * Test patterns similar to those in {@link #testMultiplePossibleLeadingZeros()}, but with a non-leading zero somewhere in the middle. Verify that any
         * possible zeros before the first non-leading zeros are made optional, but any succeeding possible zeros are not made optional.
         */
        @Test
        void testMixedLeadingAndNonLeadingZeros() {
            assertAdded("-[30]\\d..*34[05]{2}[04]", "![A-Va-z]E[69]?\\.?\\d?\\.?.?\\.?.*6\\.?(6|5[94]{0,1}5|5[94]{0,2}6)");
            assertAdded("-.*[05]{2}5[05]{2}4", "![A-Wa-z]E.*[94]?\\.?[94]{0,1}4\\.?[94]{2}6");
            assertAdded("-.+[05]{2}54[05]{2}", "![A-Wa-z]E.*[94]?\\.?[94]{0,1}4\\.?(6|5[94]{0,1}5)");
            assertAdded("-[04]{3}[05]{2}33[06][05]{2}", "![Q-V]E[95]?\\.?[95]{0,2}[94]?\\.?[94]{0,1}6\\.?(7|64|6[93][94]{0,1}5)");
            assertAdded("-[04]{3}[05]{2}33[06][05]{2}.*.+",
                            "![A-V]E[95]?\\.?[95]{0,2}[94]?\\.?[94]{0,1}6\\.?(7|64|6[93][94]{0,1}5|6[93][94]{0,2}.+|6[93][94]{0,2}.*.+)");
            assertAdded("-[04]{3}0000[05]{2}33[06].*.+", "![A-X]E[95]?\\.?[95]{0,2}(9{4})?[94]?\\.?[94]{0,1}6\\.?(7|64|6[93].+|6[93].*.+)");
            assertAdded("-[04]{3}0000[05]{2}33[06][08]{3,5}.*.+",
                            "![A-U]E[95]?\\.?[95]{0,2}(9{4})?[94]?\\.?[94]{0,1}6\\.?(7|64|6[93][91]{2,4}2|6[93][91]{3,5}.+|6[93][91]{3,5}.*.+)");
        }
    }
    
    public void assertAdded(String pattern, String expectedPattern) {
        Node actual = SimpleNumberEncoder.encode(parse(pattern));
        actual = ExponentialBinAdder.addBins(actual);
        actual = ZeroTrimmer.trim(actual);
        actual = NegativeNumberPatternInverter.invert(actual);
        actual = DecimalPointPlacer.addDecimalPoints(actual);
        assertThat(actual).asTreeString().isEqualTo(expectedPattern);
    }
}
