package datawave.data.normalizer.regex;

import static datawave.data.normalizer.regex.RegexParser.parse;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class RegexUtilsTest {
    
    @Test
    void testSplitOnAlternations() {
        // Test empty string.
        assertThat(RegexUtils.splitOnAlternations("")).containsExactly("");
        
        // Test no alternations.
        assertThat(RegexUtils.splitOnAlternations("123")).containsExactly("123");
        
        // Test top-level alternations.
        assertThat(RegexUtils.splitOnAlternations("12|(34[45])|56.*")).containsExactly("12", "(34[45])", "56.*");
        
        // Test pipes within groups.
        assertThat(RegexUtils.splitOnAlternations("(234|345)")).containsExactly("(234|345)");
        
        // Test leading empty alternation.
        assertThat(RegexUtils.splitOnAlternations("|34")).containsExactly("", "34");
        
        // Test trailing empty alternation.
        assertThat(RegexUtils.splitOnAlternations("34|")).containsExactly("34", "");
        
        // Test inner alternations with no content.
        assertThat(RegexUtils.splitOnAlternations("|34||35|")).containsExactly("", "34", "", "35", "");
        
        // Test only empty alternations.
        assertThat(RegexUtils.splitOnAlternations("||")).containsExactly("", "", "");
        
    }
    
    @Test
    void testIsNumber() {
        assertThat(RegexUtils.isNumber("123")).isTrue();
        assertThat(RegexUtils.isNumber("-123")).isTrue();
        assertThat(RegexUtils.isNumber("12\\.3")).isTrue();
        assertThat(RegexUtils.isNumber("-12\\.3")).isTrue();
        
        assertThat(RegexUtils.isNumber("-12.3")).isFalse();
        assertThat(RegexUtils.isNumber("345|54")).isFalse();
        assertThat(RegexUtils.isNumber("(123)")).isFalse();
        assertThat(RegexUtils.isNumber("[34]")).isFalse();
        assertThat(RegexUtils.isNumber("34*")).isFalse();
        assertThat(RegexUtils.isNumber("34+")).isFalse();
        assertThat(RegexUtils.isNumber("34?")).isFalse();
    }
    
    @Test
    void testEncodeNumber() {
        assertThat(RegexUtils.encodeNumber("123")).isEqualTo("\\+cE1\\.23");
        assertThat(RegexUtils.encodeNumber("123\\.4")).isEqualTo("\\+cE1\\.234");
        assertThat(RegexUtils.encodeNumber("-14")).isEqualTo("!YE8\\.6");
        assertThat(RegexUtils.encodeNumber("-1\\.4")).isEqualTo("!ZE8\\.6");
        assertThat(RegexUtils.encodeNumber("-1111111\\.3454")).isEqualTo("!TE8\\.8888886546");
    }
    
    @Test
    void testIsChar() {
        assertThat(RegexUtils.isChar(parse("0").getFirstChild(), '0')).isTrue();
        assertThat(RegexUtils.isChar(parse("0").getFirstChild(), '1')).isFalse();
        assertThat(RegexUtils.isChar(parse("\\.").getFirstChild(), '.')).isTrue();
    }
    
    @Test
    void testContainsChar() {
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '0')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '1')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '2')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '8')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '9')).isFalse();
        
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '3')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '4')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '5')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '6')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[3-7]").getFirstChild(), '7')).isTrue();
        
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '1')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '2')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '3')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '4')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '6')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '7')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '8')).isFalse();
        
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '0')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '5')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[059]").getFirstChild(), '9')).isTrue();
    }
    
    @Test
    void testContainsCharNegated() {
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '0')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '1')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '2')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '8')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '9')).isTrue();
        
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '3')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '4')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '5')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '6')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[^3-7]").getFirstChild(), '7')).isFalse();
        
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '1')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '2')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '3')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '4')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '6')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '7')).isTrue();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '8')).isTrue();
        
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '0')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '5')).isFalse();
        assertThat(RegexUtils.charClassMatches(parse("[^059]").getFirstChild(), '9')).isFalse();
    }
    
    @Test
    void testMatchesZero() {
        // Test single char nodes.
        assertThat(RegexUtils.matchesZero(new SingleCharNode('0'))).isTrue();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('1'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('2'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('3'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('4'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('5'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('6'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('7'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('8'))).isFalse();
        assertThat(RegexUtils.matchesZero(new SingleCharNode('9'))).isFalse();
        
        // Test wildcard.
        assertThat(RegexUtils.matchesZero(new AnyCharNode())).isTrue();
        
        // Test digit character class.
        assertThat(RegexUtils.matchesZero(parse("\\d").getFirstChild())).isTrue();
        
        // Test character classes.
        assertThat(RegexUtils.matchesZero(parse("[0]").getFirstChild())).isTrue();
        assertThat(RegexUtils.matchesZero(parse("[0126-9]").getFirstChild())).isTrue();
        assertThat(RegexUtils.matchesZero(parse("[0-4]").getFirstChild())).isTrue();
        assertThat(RegexUtils.matchesZero(parse("[123456789]").getFirstChild())).isFalse();
        assertThat(RegexUtils.matchesZero(parse("[1-9]").getFirstChild())).isFalse();
    }
    
    @Test
    void testMatchesZeroOnly() {
        // Test single char nodes.
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('0'))).isTrue();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('1'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('2'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('3'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('4'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('5'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('6'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('7'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('8'))).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(new SingleCharNode('9'))).isFalse();
        
        // Test wildcard.
        assertThat(RegexUtils.matchesZeroOnly(new AnyCharNode())).isFalse();
        
        // Test digit character class.
        assertThat(RegexUtils.matchesZeroOnly(parse("\\d").getFirstChild())).isFalse();
        
        // Test character classes.
        assertThat(RegexUtils.matchesZeroOnly(parse("[0]").getFirstChild())).isTrue();
        assertThat(RegexUtils.matchesZeroOnly(parse("[0-0]").getFirstChild())).isTrue();
        assertThat(RegexUtils.matchesZeroOnly(parse("[0126-9]").getFirstChild())).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(parse("[0-4]").getFirstChild())).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(parse("[123456789]").getFirstChild())).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(parse("[1-9]").getFirstChild())).isFalse();
        assertThat(RegexUtils.matchesZeroOnly(parse("[0-9]").getFirstChild())).isFalse();
    }
    
    @Test
    void testGetQuantifierRange() {
        assertThat(RegexUtils.getQuantifierRange(new ZeroOrMoreNode())).isEqualTo(Pair.of(0, null));
        assertThat(RegexUtils.getQuantifierRange(new OneOrMoreNode())).isEqualTo(Pair.of(1, null));
        assertThat(RegexUtils.getQuantifierRange(parse("{2}").getFirstChild())).isEqualTo(Pair.of(2, 2));
        assertThat(RegexUtils.getQuantifierRange(parse("{2,5}").getFirstChild())).isEqualTo(Pair.of(2, 5));
        assertThat(RegexUtils.getQuantifierRange(parse("{2,}").getFirstChild())).isEqualTo(Pair.of(2, null));
    }
}
