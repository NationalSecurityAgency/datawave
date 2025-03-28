package datawave.data.normalizer.regex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import datawave.data.normalizer.regex.visitor.StringVisitor;
import datawave.data.type.util.NumericalEncoder;

public class RegexUtils {
    
    /**
     * Split the given string by all top-level alternations into individual regex segments to be further evaluated. Any pipes encapsulated within groups, e.g.
     * (1|2|3) will not count as alternations to split. See the following input examples:
     * <ul>
     * <li>Input {@code ""} will return the list {@code {""}}</li>
     * <li>Input {@code "234.*"} will return the list {@code {"234.*"}}</li>
     * <li>Input {@code "234.*|45|653.*"} will return the list {@code {"234.*", "45", "653.*"}}</li>
     * <li>Input {@code "234.*|45|(3[34].*|4[54]3)} will return the list {@code {"234.*", "45", "(2[34].*|4[54]3)"}}</li>
     * <li>Input {@code "|34"} will return the list {@code {"", "34"}}}</li>
     * <li>Input {@code "34|"} will return the list {@code {"34", ""}}}</li>
     * <li>Input {@code "||"} will return the list {@code {"", "", ""}}}</li>
     * <li>Input {@code "|12||4|34|} will return the list {@code {"", "12", "", "4", "34"}}</li>
     * </ul>
     * 
     * @param str
     *            the string to split
     * @return the split segments
     */
    public static List<String> splitOnAlternations(String str) {
        List<String> segments = new ArrayList<>();
        // If the string is empty, return a list containing an empty string.
        if (str.isEmpty()) {
            segments.add("");
            return segments;
        }
        
        char[] chars = str.toCharArray();
        int strLength = chars.length;
        int lastPos = strLength - 1;
        int groupsToTraverse = 0;
        int startOfSegment = 0;
        // Stream over the string one character at a time.
        for (int pos = 0; pos < strLength; pos++) {
            char current = chars[pos];
            if (pos != lastPos) {
                switch (current) {
                    case RegexConstants.PIPE:
                        // If we found an alternation, it is top-level only if there are no groups we currently need to traverse.
                        if (groupsToTraverse == 0) {
                            // If the start of the segment is not the current position, we have a non-zero length segment.
                            if (startOfSegment != pos) {
                                segments.add(str.substring(startOfSegment, (pos)));
                            } else {
                                // Otherwise we've encountered an empty alternation somewhere before the end of the string.
                                segments.add("");
                            }
                            // Mark the start of the next segment as the next character.
                            startOfSegment = pos + 1;
                        }
                        break;
                    case RegexConstants.LEFT_PAREN:
                        // We found the start of a group. Increment the number of groups we need to traverse.
                        groupsToTraverse++;
                        break;
                    case RegexConstants.RIGHT_PAREN:
                        // We found the end of a group. Decrement the number of groups we need to traverse.
                        groupsToTraverse--;
                        break;
                    default:
                }
            } else {
                // If the last character is not a pipe, it is part of the last segment.
                if (current != RegexConstants.PIPE) {
                    segments.add(str.substring(startOfSegment));
                } else {
                    // If we have a zero-length segment, add an empty alternation.
                    if (startOfSegment == pos) {
                        segments.add("");
                    } else {
                        // Otherwise the segment ends at the character before last.
                        segments.add(str.substring(startOfSegment, lastPos));
                    }
                    // Add a trailing empty segment.
                    segments.add("");
                }
            }
        }
        return segments;
    }
    
    /**
     * Return whether the regex consists of a single simple number without any special operations, e.g. '1', '1\\.0', '-1', '-1\\.0'.
     */
    public static boolean isNumber(String str) {
        char[] chars = str.toCharArray();
        int lastPos = chars.length - 1;
        for (int currentPos = 0; currentPos <= lastPos; currentPos++) {
            char current = chars[currentPos];
            switch (current) {
                case RegexConstants.BACKSLASH:
                case RegexConstants.HYPHEN:
                case RegexConstants.ONE:
                case RegexConstants.TWO:
                case RegexConstants.THREE:
                case RegexConstants.FOUR:
                case RegexConstants.FIVE:
                case RegexConstants.SIX:
                case RegexConstants.SEVEN:
                case RegexConstants.EIGHT:
                case RegexConstants.NINE:
                    continue;
                case RegexConstants.PERIOD:
                    // If we encounter a period at the beginning of the regex, we know it is a dot wildcard and not an escaped decimal point.
                    if (currentPos == 0) {
                        return false;
                    } else {
                        // If we encounter a period anywhere else in the regex, if it is not preceded by a backslash to indicate that it's an escaped decimal
                        // point, then it is a dot wildcard.
                        char prev = chars[(currentPos - 1)];
                        if (prev != RegexConstants.BACKSLASH) {
                            return false;
                        }
                    }
                    break;
                default:
                    // Any characters other than 0-9, -, or \. indicate a non-simple number regex.
                    return false;
            }
        }
        return true;
    }
    
    /**
     * Returns the escaped, encoded form of a string containing a number from part of a regex. The string must be a number, and may be escaped. See the
     * following input examples:
     * <ul>
     * <li>Input {@code "1.2"} will return {@code "\+aE1\.2"}</li>
     * <li>Input {@code "1\.2"} will return {@code "\+aE1\.2"}</li>
     * <li>Input {@code "12"} will return {@code "\+bE1\.2"}</li>
     * <li>Input {@code "-1\.2"} will return {@code "\!ZE1\.2"}</li>
     * <li>Input {@code "-12"} will return {@code "\!YE1\.2"}</li>
     * </ul>
     * 
     * @param str
     *            the string to encode
     * @return the escaped, encoded number
     */
    public static String encodeNumber(String str) {
        return escapeEncodedNumber(NumericalEncoder.encode(removeBackslashes(str)));
    }
    
    /**
     * Return the given string with all backslashes removed from it.
     * 
     * @param str
     *            the string
     * @return the string without any backslashes
     */
    public static String removeBackslashes(String str) {
        return str.replaceAll(RegexConstants.ESCAPED_BACKSLASH, "");
    }
    
    /**
     * Return an encoded whole number with the characters {@code . ! +} escaped by a backslash.
     */
    public static String escapeEncodedNumber(String str) {
        StringBuilder sb = new StringBuilder();
        for (char current : str.toCharArray()) {
            if (current == RegexConstants.PERIOD || current == RegexConstants.PLUS) {
                sb.append(RegexConstants.BACKSLASH);
            }
            sb.append(current);
        }
        return sb.toString();
    }
    
    /**
     * Return the index of the first escaped period present in the children of the given node tree, or -1 if no such child is found.
     * 
     * @param node
     *            the node
     * @return the index of the first escaped period, or -1 if not found
     */
    public static int getDecimalPointIndex(Node node) {
        int index = node.indexOf(EscapedSingleCharNode.class);
        while (index != -1) {
            EscapedSingleCharNode escapedNode = (EscapedSingleCharNode) node.getChildAt(index);
            if (escapedNode.getCharacter() == RegexConstants.PERIOD) {
                return index;
            }
            index = node.indexOf(EscapedSingleCharNode.class, (index + 1));
        }
        return -1;
    }
    
    /**
     * Returns whether the first child in the given node tree is a minus sign.
     * 
     * @param node
     *            the node
     * @return true if the first child is a minus sign, or false otherwise
     */
    public static boolean isNegativeRegex(Node node) {
        return isChar(node.getFirstChild(), RegexConstants.HYPHEN);
    }
    
    /**
     * Return whether the given node is an escaped period.
     * 
     * @param node
     *            the node
     * @return true if the given node is an escaped period, or false otherwise.
     */
    public static boolean isDecimalPoint(Node node) {
        return node instanceof EscapedSingleCharNode && ((EscapedSingleCharNode) node).getCharacter() == RegexConstants.PERIOD;
    }
    
    /**
     * Return whether the given node is the given character, escaped or otherwise.
     * 
     * @param node
     *            the node
     * @param character
     *            the character
     * @return true if the given node is the given character, or false otherwise
     */
    public static boolean isChar(Node node, char character) {
        if (node instanceof SingleCharNode) {
            return ((SingleCharNode) node).getCharacter() == character;
        } else if (node instanceof EscapedSingleCharNode) {
            return ((EscapedSingleCharNode) node).getCharacter() == character;
        }
        return false;
    }
    
    /**
     * Return whether the given node is a character class that would match against the given character.
     * 
     * @param node
     *            the node
     * @param character
     *            the character
     * @return true if the given character class would match against the given character, or false otherwise
     * @throws IllegalArgumentException
     *             if the given node is not a {@link CharClassNode}
     */
    public static boolean charClassMatches(Node node, char character) {
        if (node instanceof CharClassNode) {
            CharClassNode charClass = (CharClassNode) node;
            boolean matchFound = false;
            for (Node child : charClass.getChildren()) {
                // If the current child is a single character, see if it is a match for the character.
                if (child instanceof SingleCharNode) {
                    if (isChar(child, character)) {
                        matchFound = true;
                        break;
                    }
                } else {
                    // If the current child is a character range, see if it is within the range.
                    CharRangeNode charRange = (CharRangeNode) child;
                    int charDigit = Character.digit(character, RegexConstants.DECIMAL_RADIX);
                    int startDigit = Character.digit(charRange.getStart(), RegexConstants.DECIMAL_RADIX);
                    int endDigit = Character.digit(charRange.getEnd(), RegexConstants.DECIMAL_RADIX);
                    if (startDigit <= charDigit && charDigit <= endDigit) {
                        matchFound = true;
                        break;
                    }
                }
            }
            // If the character class was negated, e.g. [^1-5], it matches against the character if no direct match was found.
            return charClass.isNegated() != matchFound;
        } else {
            throw new IllegalArgumentException("Node must be a " + CharClassNode.class.getSimpleName());
        }
    }
    
    /**
     * Return whether the given node is a character class that would only match against the given character.
     *
     * @param node
     *            the node
     * @param character
     *            the character
     * @return true if the given character class would only match against the given character, or false otherwise
     * @throws IllegalArgumentException
     *             if the given node is not a {@link CharClassNode}
     */
    public static boolean charClassMatchesOnly(Node node, char character) {
        if (node instanceof CharClassNode) {
            CharClassNode charClass = (CharClassNode) node;
            boolean matchFound = false;
            for (Node child : charClass.getChildren()) {
                // If the current child is a single character, see if it is a match for the character.
                if (child instanceof SingleCharNode) {
                    if (isChar(child, character)) {
                        matchFound = true;
                    } else {
                        // A character other than the target was found.
                        return false;
                    }
                } else {
                    // If the current child is a character range, see the range only encompasses the target character, e.g. [1-1].
                    CharRangeNode charRange = (CharRangeNode) child;
                    int charDigit = Character.digit(character, RegexConstants.DECIMAL_RADIX);
                    int startDigit = Character.digit(charRange.getStart(), RegexConstants.DECIMAL_RADIX);
                    int endDigit = Character.digit(charRange.getEnd(), RegexConstants.DECIMAL_RADIX);
                    if (startDigit == charDigit && charDigit == endDigit) {
                        matchFound = true;
                    } else {
                        // A range encompassing characters other than the target was found.
                        return false;
                    }
                }
            }
            // If the character class was negated, e.g. [^1], it matches against the character if no direct match was found.
            return charClass.isNegated() != matchFound;
        } else {
            throw new IllegalArgumentException("Node must be a " + CharClassNode.class.getSimpleName());
        }
    }
    
    /**
     * Return whether the given node is a regex element that would match against the given character.
     * 
     * @param node
     *            the regex element
     * @param character
     *            the character
     * @return true if the given node would match against the given character, or false otherwise
     */
    public static boolean matchesChar(Node node, char character) {
        switch (node.getType()) {
            case DIGIT_CHAR_CLASS:
            case ANY_CHAR:
                return true;
            case SINGLE_CHAR:
                return isChar(node, character);
            case CHAR_CLASS:
                return charClassMatches(node, character);
            default:
                return false;
        }
    }
    
    public static boolean groupNodeMatches(Node node, char character) {
        GroupNode group = (GroupNode) node;
        boolean matchFound = false;
        
        for (Node child : group.getChildren()) {
            // If the current child is a single character, see if it is a match for the character.
            if (child instanceof SingleCharNode) {
                if (isChar(child, character)) {
                    matchFound = true;
                } else {
                    // A character other than the target was found, but there may be more in the group
                    continue;
                }
            }
        }
        return matchFound;
    }
    
    /**
     * Return whether the given node is a regex element that can only match against the given character.
     *
     * @param node
     *            the node
     * @return true if the node can match only against the given character or false otherwise.
     */
    public static boolean matchesCharOnly(Node node, char character) {
        switch (node.getType()) {
            case SINGLE_CHAR:
                return isChar(node, character);
            case CHAR_CLASS:
                return charClassMatchesOnly(node, character);
            default:
                return false;
        }
    }
    
    /**
     * Return whether the given node is a regex element that can match against the character '0'.
     *
     * @param node
     *            the node
     * @return true if the node can match against '0' or false otherwise.
     */
    public static boolean matchesZero(Node node) {
        return matchesChar(node, RegexConstants.ZERO);
    }
    
    public static boolean matchesCharExplicitly(Node node, char character) {
        switch (node.getType()) {
            case SINGLE_CHAR:
                return isChar(node, character);
            case CHAR_CLASS:
                return charClassMatches(node, character);
            case GROUP:
                return groupNodeMatches(node, character);
            default:
                return false;
        }
    }
    
    public static boolean matchesZeroExplicitly(Node node) {
        return matchesCharExplicitly(node, RegexConstants.ZERO);
    }
    
    /**
     * Return whether the given node is a regex element that can only match against the character '0'.
     * 
     * @param node
     *            the node
     * @return true if the node can match only against '0' or false otherwise.
     */
    public static boolean matchesZeroOnly(Node node) {
        return matchesCharOnly(node, RegexConstants.ZERO);
    }
    
    /**
     * Return whether the given node is a quantifier type.
     * 
     * @param node
     *            the node
     * @return true if the node is a quantifier type, or false otherwise
     */
    public static boolean isQuantifier(Node node) {
        return RegexConstants.QUANTIFIER_TYPES.contains(node.getClass());
    }
    
    /**
     * Return a range representing the number of occurrences the given node can match against. The left side will be at a minimum, 0, and the right side may be
     * a number, or null (infinity).
     * 
     * @param node
     *            the node
     * @return the occurrence range
     * @throws IllegalArgumentException
     *             if the given node is not a quantifier type
     */
    public static Pair<Integer,Integer> getQuantifierRange(Node node) {
        if (!isQuantifier(node)) {
            throw new IllegalArgumentException("Node must be one of the following quantifier types: " + RegexConstants.QUANTIFIER_TYPES);
        }
        int min;
        Integer max = null;
        switch (node.getType()) {
            case ZERO_OR_MORE:
                // Minimum occurrence of 0.
                min = 0;
                break;
            case ONE_OR_MORE:
                // Minimum occurrence of 1.
                min = 1;
                break;
            case REPETITION:
                Node child = node.getFirstChild();
                if (child instanceof IntegerNode) {
                    // Minimum and maximum occurrences will be the same.
                    min = ((IntegerNode) child).getValue();
                    max = min;
                } else {
                    IntegerRangeNode rangeNode = (IntegerRangeNode) child;
                    // Minimum is defined in range. Maximum may be infinity if not defined.
                    min = rangeNode.getStart();
                    if (rangeNode.isEndBounded()) {
                        max = rangeNode.getEnd();
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unhandled quantifier type: " + RegexConstants.QUANTIFIER_TYPES);
        }
        return Pair.of(min, max);
    }
    
    /**
     * Return whether the given node represents a simple number regex.
     *
     * @param node
     *            the node
     * @return true if the node is a simple number regex, or false otherwise
     */
    public static boolean isSimpleNumber(Node node) {
        if (node.isAnyChildNotOf(RegexConstants.SIMPLE_NUMBER_TYPES)) {
            return false;
        }
        String expression = StringVisitor.toString(node);
        return RegexConstants.SIMPLE_NUMBER_REGEX_PATTERN.matcher(expression).matches();
    }
    
    /**
     * Return the given digit character as an integer.
     * 
     * @param digit
     *            the digit character
     * @return the integer form
     */
    public static int toInt(char digit) {
        return Character.digit(digit, RegexConstants.DECIMAL_RADIX);
    }
    
    /**
     * Return the given int as a digit character.
     * 
     * @param digit
     *            the int
     * @return the digit character
     */
    public static char toChar(int digit) {
        return Character.forDigit(digit, RegexConstants.DECIMAL_RADIX);
    }
    
    /**
     * Return whether the given quantifier node allows for zero occurrences.
     * 
     * @param node
     *            the node
     * @return true if the quantifier allows for zero occurrences, or false otherwise
     */
    public static boolean canOccurZeroTimes(Node node) {
        if (!isQuantifier(node)) {
            throw new IllegalArgumentException("Node must be one of the following quantifier types: " + RegexConstants.QUANTIFIER_TYPES);
        }
        switch (node.getType()) {
            case ZERO_OR_MORE:
                return true;
            case ONE_OR_MORE:
                return false;
            case REPETITION:
                return repetitionCanOccurZeroTimes((RepetitionNode) node);
            default:
                throw new IllegalArgumentException("Unhandled quantifier type: " + RegexConstants.QUANTIFIER_TYPES);
        }
    }
    
    /**
     * Return whether the given repetition quantifier node allows for zero occurrences.
     * 
     * @param node
     *            the node
     * @return true if the quantifier allows for zero occurrences, or false otherwise
     */
    public static boolean repetitionCanOccurZeroTimes(RepetitionNode node) {
        Node child = node.getFirstChild();
        if (child instanceof IntegerNode) {
            return ((IntegerNode) child).getValue() == 0;
        } else {
            return ((IntegerRangeNode) child).getStart() == 0;
        }
    }
    
    /**
     * Return the given repetition as an occurrence range.
     * 
     * @param node
     *            the node
     * @return the range
     */
    public static Pair<Integer,Integer> getRepetitionAsRange(RepetitionNode node) {
        Node child = node.getFirstChild();
        if (child instanceof IntegerNode) {
            int value = ((IntegerNode) child).getValue();
            return Pair.of(value, value);
        } else {
            IntegerRangeNode integerRange = (IntegerRangeNode) child;
            if (integerRange.isEndBounded()) {
                return Pair.of(integerRange.getStart(), integerRange.getEnd());
            } else {
                return Pair.of(integerRange.getStart(), null);
            }
        }
    }
    
    /**
     * Subtract one from the given range endpoints and return it.
     * 
     * @param range
     *            the range
     * @return the updated range
     */
    public static Pair<Integer,Integer> subtractOneFrom(Pair<Integer,Integer> range) {
        int left = range.getLeft() > 0 ? (range.getLeft() - 1) : 0;
        Integer right = range.getRight() == null ? null : (range.getRight() - 1);
        return Pair.of(left, right);
    }
    
    /**
     * Return a new repetition node created from the given range.
     * 
     * @param range
     *            the range
     * @return the new repetition node
     */
    public static RepetitionNode createRepetition(Pair<Integer,Integer> range) {
        if (Objects.equals(range.getLeft(), range.getRight())) {
            return new RepetitionNode(new IntegerNode(range.getLeft()));
        } else {
            return new RepetitionNode(new IntegerRangeNode(range.getLeft(), range.getRight()));
        }
    }
    
    /**
     * Return whether the given repetition quantifier is not a defined range, e.g. {x} rather than {x,y} or {x,}.
     * 
     * @param node
     *            the node
     * @return true if the repetition is not a range, or false otherwise
     */
    public static boolean isNotRange(RepetitionNode node) {
        return node.getFirstChild() instanceof IntegerNode;
    }
    
    /**
     * Return a copy of the given repetition as a range starting from zero.
     * 
     * @param node
     *            the node
     * @return the new repetition quantifier
     */
    public static RepetitionNode createRangeStartingFromZero(RepetitionNode node) {
        IntegerRangeNode range = new IntegerRangeNode();
        range.setStart(0);
        Node child = node.getFirstChild();
        if (child instanceof IntegerNode) {
            range.setEnd(((IntegerNode) child).getValue());
        } else {
            range.setEnd(((IntegerRangeNode) child).getEnd());
        }
        return new RepetitionNode(range);
    }
    
    private RegexUtils() {
        throw new UnsupportedOperationException();
    }
}
