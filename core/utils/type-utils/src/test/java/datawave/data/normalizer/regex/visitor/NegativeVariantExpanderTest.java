package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.visitor.NegativeVariantExpander;

class NegativeVariantExpanderTest {
    
    @Test
    void testNullNode() {
        assertNotExpanded(null);
    }
    
    @Test
    void testEmptyNode() {
        assertNotExpanded("");
    }
    
    @Test
    void testRegexesWithoutLeadingWildcards() {
        assertNotExpanded("234.*");
        assertNotExpanded("\\..*");
        assertNotExpanded("-\\.34.*");
        assertNotExpanded("-\\.34.+");
        assertNotExpanded("[34]90.+");
        assertNotExpanded("[34]90.+");
        
        // Leading wildcards with a negative sign in front of them do not need to be expanded.
        assertNotExpanded("-.78");
        assertNotExpanded("-.*78");
        assertNotExpanded("-.*?78");
        assertNotExpanded("-.+78");
        assertNotExpanded("-.+?78");
    }
    
    @Test
    void testRegexesWithLeadingWildcards() {
        // Leading wildcards with no negative sign in front need to be expanded to include a negative variant.
        assertExpandedTo(".454", ".454|-.454");
        assertExpandedTo(".*455", ".*455|-.*455");
        assertExpandedTo(".*?455", ".*?455|-.*?455");
        assertExpandedTo(".+455", ".+455|-.+455");
        assertExpandedTo(".+?455", ".+?455|-.+?455");
        
        // Test alternations.
        assertExpandedTo(".455|343|[9]34.*", ".455|-.455|343|[9]34.*");
    }
    
    private void assertNotExpanded(String pattern) {
        assertExpandedTo(pattern, pattern);
    }
    
    private void assertExpandedTo(String pattern, String expectedPattern) {
        Node actual = NegativeVariantExpander.expand(parse(pattern));
        if (expectedPattern == null) {
            assertThat(actual).isNull();
        } else {
            Node expected = parse(expectedPattern);
            assertThat(actual).isEqualTreeTo(expected);
        }
    }
}
