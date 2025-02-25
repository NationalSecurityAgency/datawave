package datawave.data.normalizer.regex;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RegexConstants {
    
    public static final char ZERO = '0';
    public static final char ONE = '1';
    public static final char TWO = '2';
    public static final char THREE = '3';
    public static final char FOUR = '4';
    public static final char FIVE = '5';
    public static final char SIX = '6';
    public static final char SEVEN = '7';
    public static final char EIGHT = '8';
    public static final char NINE = '9';
    public static final char LOWERCASE_D = 'd';
    public static final char BACKSLASH = '\\';
    public static final char PERIOD = '.';
    public static final char HYPHEN = '-';
    public static final char STAR = '*';
    public static final char PLUS = '+';
    public static final char PIPE = '|';
    public static final char LEFT_PAREN = '(';
    public static final char RIGHT_PAREN = ')';
    public static final char LEFT_BRACKET = '[';
    public static final char RIGHT_BRACKET = ']';
    public static final char EXCLAMATION_POINT = '!';
    public static final char LEFT_BRACE = '{';
    public static final char RIGHT_BRACE = '}';
    public static final char QUESTION_MARK = '?';
    public static final char COMMA = ',';
    public static final char CARET = '^';
    public static final char DOLLAR_SIGN = '$';
    public static final char CAPITAL_E = 'E';
    
    public static final String ESCAPED_BACKSLASH = "\\\\";
    
    /**
     * Use base 10 when parsing characters to ints.
     */
    public static final int DECIMAL_RADIX = 10;
    
    /**
     * The set of all digits. This reflects all possible permutations for any \d found in the regex.
     */
    public static final List<Character> ALL_DIGITS = ImmutableList.of(ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE);
    
    public static final Set<Class<? extends Node>> QUANTIFIER_TYPES = ImmutableSet.of(ZeroOrMoreNode.class, OneOrMoreNode.class, RepetitionNode.class);
    
    public static final Set<Class<? extends Node>> SIMPLE_NUMBER_TYPES = ImmutableSet.of(SingleCharNode.class, EscapedSingleCharNode.class,
                    StartAnchorNode.class, EndAnchorNode.class);
    
    public static final Pattern SIMPLE_NUMBER_REGEX_PATTERN = Pattern.compile("^\\^?(\\\\?-)?\\d*(\\\\\\.)?\\d+\\$?$");
    
    private RegexConstants() {
        throw new UnsupportedOperationException();
    }
}
