package datawave.data.normalizer.regex;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

class RegexParserTest {
    
    @Test
    void testParsingEmptyString() {
        // @formatter:off
        assertThat(parse("")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isEmptyNode().hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingNumbers() {
        // Test parsing a whole number.
        // @formatter:off
        assertThat(parse("345")).isExpressionNode().hasChildCount(3)
                        .assertChild(0).isSingleCharNode().hasCharacter('3').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('4').hasNoChildren().assertParent()
                        .assertChild(2).isSingleCharNode().hasCharacter('5').hasNoChildren();
        // @formatter:on
        
        // Test parsing a floating point number.
        // @formatter:off
        assertThat(parse("23\\.5")).isExpressionNode().hasChildCount(4)
                        .assertChild(0).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('3').hasNoChildren().assertParent()
                        .assertChild(2).isEscapedSingleCharNode().hasCharacter('.').hasNoChildren().assertParent()
                        .assertChild(3).isSingleCharNode().hasCharacter('5').hasNoChildren();
        // @formatter:on
        
        // Test parsing a negative floating point number.
        // @formatter:off
        assertThat(parse("-12\\.5")).isExpressionNode().hasChildCount(5)
                        .assertChild(0).isSingleCharNode().hasCharacter('-').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('1').hasNoChildren().assertParent()
                        .assertChild(2).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(3).isEscapedSingleCharNode().hasCharacter('.').hasNoChildren().assertParent()
                        .assertChild(4).isSingleCharNode().hasCharacter('5').hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingAlternations() {
        // Test parsing simple top-level alternations.
        // @formatter:off
        assertThat(parse("24|4|5")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isAlternationNode().hasChildCount(3)
                            .assertChild(0).isExpressionNode().hasChildCount(2)
                                .assertChild(0).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                                .assertChild(1).isSingleCharNode().hasCharacter('4').hasNoChildren().assertGrandparent()
                            .assertChild(1).isExpressionNode().hasChildCount(1)
                                .assertChild(0).isSingleCharNode().hasCharacter('4').hasNoChildren().assertGrandparent()
                            .assertChild(2).isExpressionNode().hasChildCount(1)
                                .assertChild(0).isSingleCharNode().hasCharacter('5').hasNoChildren();
        // @formatter:on
        
        // Test parsing only empty alternations.
        // @formatter:off
        assertThat(parse("||")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isAlternationNode().hasChildCount(3)
                            .assertChild(0).isEmptyNode().hasNoChildren().assertParent()
                            .assertChild(1).isEmptyNode().hasNoChildren().assertParent()
                            .assertChild(2).isEmptyNode().hasNoChildren();
        // @formatter:on
        
        // Test parsing trailing empty alternation.
        // @formatter:off
        assertThat(parse("3|")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isAlternationNode().hasChildCount(2)
                            .assertChild(0).isExpressionNode().hasChildCount(1)
                                .assertChild(0).isSingleCharNode().hasCharacter('3').assertGrandparent()
                            .assertChild(1).isEmptyNode().hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingCharacterClasses() {
        // Test parsing a digit character class.
        // @formatter:off
        assertThat(parse("\\d")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isDigitCharClassNode().hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with digits.
        // @formatter:off
        assertThat(parse("[458]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(3)
                            .assertChild(0).isSingleCharNode().hasCharacter('4').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('5').hasNoChildren().assertParent()
                            .assertChild(2).isSingleCharNode().hasCharacter('8').hasNoChildren();
        // @formatter:on
        
        // Test parsing a negated character class.
        // @formatter:off
        assertThat(parse("[^458]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNegated().hasChildCount(3)
                            .assertChild(0).isSingleCharNode().hasCharacter('4').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('5').hasNoChildren().assertParent()
                            .assertChild(2).isSingleCharNode().hasCharacter('8').hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with a negative sign at the beginning.
        // @formatter:off
        assertThat(parse("[\\-58]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(3)
                            .assertChild(0).isEscapedSingleCharNode().hasCharacter('-').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('5').hasNoChildren().assertParent()
                            .assertChild(2).isSingleCharNode().hasCharacter('8').hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with a negative sign at the end.
        // @formatter:off
        assertThat(parse("[58\\-]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(3)
                            .assertChild(0).isSingleCharNode().hasCharacter('5').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('8').hasNoChildren().assertParent()
                            .assertChild(2).isEscapedSingleCharNode().hasCharacter('-').hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with a range.
        // @formatter:off
        assertThat(parse("[5-8]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(1)
                            .assertChild(0).isCharRangeNode().hasStart('5').hasEnd('8').hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with a range and a subsequent digit.
        // @formatter:off
        assertThat(parse("[2-46]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(2)
                            .assertChild(0).isCharRangeNode().hasStart('2').hasEnd('4').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('6').hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with multiple ranges.
        // @formatter:off
        assertThat(parse("[2-46-8]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(2)
                            .assertChild(0).isCharRangeNode().hasStart('2').hasEnd('4').hasNoChildren().assertParent()
                            .assertChild(1).isCharRangeNode().hasStart('6').hasEnd('8').hasNoChildren();
        // @formatter:on
        
        // Test parsing a character class with a decimal point.
        // @formatter:off
        assertThat(parse("[.58]")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isCharClassNode().isNotNegated().hasChildCount(3)
                            .assertChild(0).isSingleCharNode().hasCharacter('.').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('5').hasNoChildren().assertParent()
                            .assertChild(2).isSingleCharNode().hasCharacter('8').hasNoChildren();
        // @formatter:on
        
        // Test parsing character classes with escaped characters.
        assertThat(parse("[\\^\\\\]")).isExpressionNode().hasChildCount(1).assertChild(0).isCharClassNode().isNotNegated().hasChildCount(2).assertChild(0)
                        .isEscapedSingleCharNode().hasCharacter('^').hasNoChildren().assertParent().assertChild(1).isEscapedSingleCharNode().hasCharacter('\\')
                        .hasNoChildren();
        
        // Test parsing character classes with letters.
        assertThat(parse("[a-zA-Z]")).isExpressionNode().hasChildCount(1).assertChild(0).isCharClassNode().isNotNegated().hasChildCount(2).assertChild(0)
                        .isCharRangeNode().hasStart('a').hasEnd('z').hasNoChildren().assertParent().assertChild(1).isCharRangeNode().hasStart('A').hasEnd('Z')
                        .hasNoChildren();
        
        // Test parsing character classes with non-alphanumeric characters.
        assertThat(parse("[!+]")).isExpressionNode().hasChildCount(1).assertChild(0).isCharClassNode().isNotNegated().hasChildCount(2).assertChild(0)
                        .isSingleCharNode().hasCharacter('!').hasNoChildren().assertParent().assertChild(1).isSingleCharNode().hasCharacter('+')
                        .hasNoChildren();
    }
    
    @Test
    void testParsingRepetition() {
        // Test parsing a non-ranged repetition.
        // @formatter:off
        assertThat(parse("{3}")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isRepetitionNode().hasChildCount(1)
                            .assertChild(0).isIntegerNode().hasValue(3).hasNoChildren();
        // @formatter:on
        
        // Test parsing a ranged repetition with a bounded start and end.
        // @formatter:off
        assertThat(parse("{3,6}")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isRepetitionNode().hasChildCount(1)
                            .assertChild(0).isIntegerRangeNode().hasStart(3).hasEnd(6).hasNoChildren();
        // @formatter:on
        
        // Test parsing a ranged repetition with an unbounded end.
        // @formatter:off
        assertThat(parse("{3,}")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isRepetitionNode().hasChildCount(1)
                            .assertChild(0).isIntegerRangeNode().hasStart(3).hasUnboundedEnd().hasNoChildren();
        // @formatter:on
        
        // Test parsing multi-digit repetitions.
        // @formatter:off
        assertThat(parse("{344}")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isRepetitionNode().hasChildCount(1)
                            .assertChild(0).isIntegerNode().hasValue(344);
    
        assertThat(parse("{344,665}")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isRepetitionNode().hasChildCount(1)
                        .assertChild(0).isIntegerRangeNode().hasStart(344).hasEnd(665);
        // @formatter:on
    }
    
    @Test
    void testParsingGroups() {
        // Test parsing a simple group.
        // @formatter:off
        assertThat(parse("(123)")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isGroupNode().hasChildCount(1)
                            .assertChild(0).isExpressionNode().hasChildCount(3)
                                .assertChild(0).isSingleCharNode().hasCharacter('1').hasNoChildren().assertParent()
                            .assertChild(1).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                            .assertChild(2).isSingleCharNode().hasCharacter('3').hasNoChildren();
        // @formatter:on
        
        // Test parsing a group with alternations.
        // @formatter:off
        assertThat(parse("(12|3)")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isGroupNode().hasChildCount(1)
                            .assertChild(0).isAlternationNode().hasChildCount(2)
                                .assertChild(0).isExpressionNode().hasChildCount(2)
                                    .assertChild(0).isSingleCharNode().hasCharacter('1').hasNoChildren().assertParent()
                                    .assertChild(1).isSingleCharNode().hasCharacter('2').hasNoChildren().assertGrandparent()
                                .assertChild(1).isExpressionNode().hasChildCount(1)
                                    .assertChild(0).isSingleCharNode().hasCharacter('3').hasNoChildren();
        // @formatter:on
        
        // Test parsing empty group.
        // @formatter:off
        assertThat(parse("()")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isGroupNode().hasChildCount(1)
                            .assertChild(0).isEmptyNode();
        // @formatter:on
        
        // Test nested groups.
        // @formatter:off
        assertThat(parse("(1|(5|4))")).isExpressionNode().hasChildCount(1)
                        .assertChild(0).isGroupNode().hasChildCount(1)
                            .assertChild(0).isAlternationNode().hasChildCount(2)
                                .assertChild(0).isExpressionNode().hasChildCount(1)
                                    .assertChild(0).isSingleCharNode().hasCharacter('1').hasNoChildren().assertGrandparent()
                            .assertChild(1).isGroupNode().hasChildCount(1)
                                .assertChild(0).isAlternationNode().hasChildCount(2)
                                    .assertChild(0).isExpressionNode().hasChildCount(1)
                                        .assertChild(0).isSingleCharNode().hasCharacter('5').hasNoChildren().assertGrandparent()
                                    .assertChild(1).isExpressionNode().hasChildCount(1)
                                        .assertChild(0).isSingleCharNode().hasCharacter('4').hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingDot() {
        // @formatter:off
        assertThat(parse("23.")).isExpressionNode().hasChildCount(3)
                        .assertChild(0).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('3').hasNoChildren().assertParent()
                        .assertChild(2).isAnyCharNode().hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingStar() {
        // @formatter:off
        assertThat(parse("23*")).isExpressionNode().hasChildCount(3)
                        .assertChild(0).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('3').hasNoChildren().assertParent()
                        .assertChild(2).isZeroToManyNode().hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingPlus() {
        // @formatter:off
        assertThat(parse("23+")).isExpressionNode().hasChildCount(3)
                        .assertChild(0).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('3').hasNoChildren().assertParent()
                        .assertChild(2).isOneToManyNode().hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingQuestionMark() {
        // @formatter:off
        assertThat(parse("23?")).isExpressionNode().hasChildCount(3)
                        .assertChild(0).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('3').hasNoChildren().assertParent()
                        .assertChild(2).isOptionalNode().hasNoChildren();
        // @formatter:on
    }
    
    @Test
    void testParsingAnchors() {
        // @formatter:off
        assertThat(parse("^12$")).isExpressionNode().hasChildCount(4)
                        .assertChild(0).isStartAnchorNode().hasNoChildren().assertParent()
                        .assertChild(1).isSingleCharNode().hasCharacter('1').hasNoChildren().assertParent()
                        .assertChild(2).isSingleCharNode().hasCharacter('2').hasNoChildren().assertParent()
                        .assertChild(3).isEndAnchorNode().hasNoChildren();
        // @formatter:on
    }
}
