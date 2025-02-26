package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;

class OptionalVariantExpanderTest {
    
    @Test
    void testExpandingEmptyPattern() {
        assertNotExpanded("");
    }
    
    @Test
    void testExpandingPatternsWithoutOptionals() {
        assertNotExpanded("123.*");
        assertNotExpanded("^(123.)$");
        assertNotExpanded(".*1234");
        assertNotExpanded(".{3}54[3-6]");
        assertNotExpanded("343|34.*|6534.*|23\\.[34]{4}");
    }
    
    /**
     * Verify that any ? directly following a *, +, or a repetition quantifier are not expanded, and are kept to enforce lazy matching.
     */
    @Test
    void testExpandingPatternsWithLazyModifiers() {
        assertNotExpanded("234.*?");
        assertNotExpanded("234.+?");
        assertNotExpanded(".*?234");
        assertNotExpanded(".+?234");
        assertNotExpanded("2.*?4");
        assertNotExpanded("2.+?4");
        assertNotExpanded("34{4}?");
    }
    
    @Test
    void testExpandingPatternsWithOptionals() {
        // An optional element located after a decimal point should not be expanded.
        assertNotExpanded("232\\.4[3-6]?");
        
        // An optional decimal point should be expanded.
        assertExpandedTo("3\\.?6", "36|3\\.6");
        
        // Only the [4-6]? needs to be expanded to variants.
        assertExpandedTo(".*?35[4-6]?\\.34?", ".*?35\\.34?|.*?35[4-6]\\.34?");
        
        // Optionals following other characters should be expanded.
        assertExpandedTo("23?4", "24|234");
        assertExpandedTo("3[3-9]?6", "36|3[3-9]6");
        assertExpandedTo("3.?6", "36|3.6");
        assertExpandedTo("3(4.3)?5", "35|3(4.3)5");
        assertExpandedTo("-?34", "34|-34");
        
        // Multiple optionals should result in multiple expansion variants.
        assertExpandedTo("3.?4[36]?8?", "34|348|34[36]|34[36]8|3.4|3.48|3.4[36]|3.4[36]8");
        
        // Test pattern with optional at very end.
        assertExpandedTo("23?", "2|23");
        
        // Test pattern of single optional character.
        assertExpandedTo("2?", "2");
        
        // Optionals within alternations should be expanded.
        assertExpandedTo("23?4|3.?6", "24|234|36|3.6");
    }
    
    private void assertNotExpanded(String pattern) {
        assertExpandedTo(pattern, pattern);
    }
    
    private void assertExpandedTo(String pattern, String expectedPattern) {
        Node actual = OptionalVariantExpander.expand(parse(pattern));
        if (expectedPattern == null) {
            assertThat(actual).isNull();
        } else {
            assertThat(actual).asTreeString().isEqualTo(expectedPattern);
        }
    }
}
