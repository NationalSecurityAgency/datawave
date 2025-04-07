package datawave.data.normalizer.regex;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * A reader that traverses over a regex pattern and both identifies and steps through individual regex elements.
 */
class RegexReader {
    
    public enum ExpressionType {
        GROUP, ALTERNATION, REPETITION, CHAR_CLASS, SINGLE_CHAR, ESCAPED_CHAR, ANY_CHAR, ZERO_OR_MORE, ONE_OR_MORE, QUESTION_MARK, ANCHOR_START, ANCHOR_END
    }
    
    /**
     * The original char array of the pattern.
     */
    private final char[] pattern;
    
    /**
     * Index into the pattern array that keeps track of how much has been read.
     */
    private int cursor = 0;
    
    /**
     * The type of the most recently read regex expression.
     */
    private ExpressionType capturedType;
    
    /**
     * The content of the most recently read regex expression.
     */
    private String capturedContent;
    
    /**
     * Create a new {@link RegexReader} that will read over the given regex pattern.
     * 
     * @param pattern
     *            the regex pattern to read over
     */
    public RegexReader(String pattern) {
        Preconditions.checkNotNull(pattern, "regex must not be null");
        this.pattern = pattern.toCharArray();
    }
    
    /**
     * Return whether there is another expression to capture in this reader.
     *
     * @return true if there is another expression, or false otherwise
     */
    public boolean hasNext() {
        return cursor < pattern.length;
    }
    
    /**
     * Return the {@link ExpressionType} identified for the next expression during the last call to {@link #captureNext()}, or null if {@link #captureNext()}
     * has never been called.
     *
     * @return the captured type
     */
    public ExpressionType capturedType() {
        return capturedType;
    }
    
    /**
     * Return the string content identified for the next expression during the last call to {@link #captureNext()}, or null if {@link #captureNext()} has never
     * been called.
     *
     * @return the captured string expression
     */
    public String capturedExpression() {
        return capturedContent;
    }
    
    /**
     * Identify and capture the regex node type and content of the next expression in this reader.
     *
     * @throws IllegalStateException
     *             if {@link #hasNext()} returns false
     */
    public void captureNext() {
        if (hasNext()) {
            identifyCurrentType();
            int startOfCapture = cursor;
            skipPastCurrentExpression();
            this.capturedContent = new String(Arrays.copyOfRange(pattern, startOfCapture, cursor));
        } else {
            throw new IllegalStateException("Reader does not have next to capture");
        }
    }
    
    /**
     * Identify the type of the current expression starting at the current cursor point.
     */
    private void identifyCurrentType() {
        char current = current();
        switch (current) {
            case RegexConstants.PIPE:
                this.capturedType = ExpressionType.ALTERNATION;
                break;
            case RegexConstants.LEFT_PAREN:
                this.capturedType = ExpressionType.GROUP;
                break;
            case RegexConstants.LEFT_BRACE:
                this.capturedType = ExpressionType.REPETITION;
                break;
            case RegexConstants.LEFT_BRACKET:
                this.capturedType = ExpressionType.CHAR_CLASS;
                break;
            case RegexConstants.CARET:
                this.capturedType = ExpressionType.ANCHOR_START;
                break;
            case RegexConstants.DOLLAR_SIGN:
                this.capturedType = ExpressionType.ANCHOR_END;
                break;
            case RegexConstants.PERIOD:
                this.capturedType = ExpressionType.ANY_CHAR;
                break;
            case RegexConstants.STAR:
                this.capturedType = ExpressionType.ZERO_OR_MORE;
                break;
            case RegexConstants.PLUS:
                this.capturedType = ExpressionType.ONE_OR_MORE;
                break;
            case RegexConstants.QUESTION_MARK:
                this.capturedType = ExpressionType.QUESTION_MARK;
                break;
            case RegexConstants.BACKSLASH:
                this.capturedType = ExpressionType.ESCAPED_CHAR;
                break;
            default:
                this.capturedType = ExpressionType.SINGLE_CHAR;
        }
    }
    
    /**
     * Return the character in the chars array at the current cursor index.
     *
     * @return the current character
     */
    private char current() {
        return pattern[cursor];
    }
    
    /**
     * Increments the cursor by one and returns the next character in the char array.
     *
     * @return the next character
     */
    private char next() {
        return pattern[++cursor];
    }
    
    /**
     * Increment the cursor by one.
     */
    private void skip() {
        cursor++;
    }
    
    /**
     * Increment the cursor by the given number of skips.
     *
     * @param skips
     *            the skips to increment by
     */
    private void skip(int skips) {
        cursor = cursor + skips;
    }
    
    /**
     * Increment the cursor to point to the position after the current expression based on the current captured type.
     */
    private void skipPastCurrentExpression() {
        switch (capturedType) {
            case SINGLE_CHAR:
            case ALTERNATION:
            case ANY_CHAR:
            case ZERO_OR_MORE:
            case ONE_OR_MORE:
            case QUESTION_MARK:
            case ANCHOR_START:
            case ANCHOR_END:
                skip(1);
                break;
            case ESCAPED_CHAR:
                skip(2);
                break;
            case CHAR_CLASS:
                skipPastChar(RegexConstants.RIGHT_BRACKET);
                break;
            case REPETITION:
                skipPastChar(RegexConstants.RIGHT_BRACE);
                break;
            case GROUP:
                skipPastGroup();
                break;
            default:
                throw new IllegalArgumentException("Unable to seek past type " + capturedType);
        }
    }
    
    /**
     * Increment the cursor to point to the position after the first occurrence of the given character.
     *
     * @param character
     *            the character to skip past
     */
    private void skipPastChar(char character) {
        while (hasNext()) {
            char next = next();
            if (next == character) {
                skip();
                return;
            }
        }
    }
    
    /**
     * Increment the cursor to point to the position after the current group expression. This method will handle nested groups.
     */
    private void skipPastGroup() {
        int nestedGroups = 0;
        while (hasNext()) {
            char next = next();
            switch (next) {
                case RegexConstants.RIGHT_PAREN:
                    // If there are no nested groups, we've found the end of the target group. Skip ahead to the next character after it.
                    if (nestedGroups == 0) {
                        skip();
                        return;
                    } else {
                        // We've traversed to the end of a nested group.
                        nestedGroups--;
                    }
                    break;
                case RegexConstants.LEFT_PAREN:
                    // If we encounter a ( before the first ) we see, we've found a nested group and must traverse to the end it.
                    nestedGroups++;
                    break;
                default:
            }
        }
    }
}
