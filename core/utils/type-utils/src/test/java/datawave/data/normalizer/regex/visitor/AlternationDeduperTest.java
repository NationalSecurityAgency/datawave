package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;

class AlternationDeduperTest {
    
    @Test
    void testPatternsWithoutAlternations() {
        assertNotDeduped("0");
        assertNotDeduped("345.*");
        assertNotDeduped("(345.*)");
        assertNotDeduped("-653[3-5]");
    }
    
    @Test
    void testAlternationsWithoutDuplications() {
        assertNotDeduped("0|1|5|56");
        assertNotDeduped("45.*|76[3-6]|.*?343");
    }
    
    @Test
    void testAlternationsWithDuplicates() {
        assertDeduped("54|54", "54");
        assertDeduped("54|34.*|54", "54|34.*");
        assertDeduped("54|34.*|76.*34|34.*", "54|34.*|76.*34");
    }
    
    private void assertNotDeduped(String pattern) {
        assertDeduped(pattern, pattern);
    }
    
    private void assertDeduped(String pattern, String expectedPattern) {
        Node actual = AlternationDeduper.dedupe(parse(pattern));
        Node expected = parse(expectedPattern);
        assertThat(actual).isEqualTreeTo(expected);
    }
}
