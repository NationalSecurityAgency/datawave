package datawave.query.jexl.visitors;

import static datawave.query.Constants.BACKSLASH_CHAR;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;

/**
 * This visitor provides the ability to detect and return unescaped special characters found in string literals and regex patterns in the query.
 */
public class UnescapedSpecialCharactersVisitor extends ShortCircuitBaseVisitor {

    public static final Set<Character> patternReservedCharacters = Set.of('.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\');

    private final Set<Character> literalExceptions;
    private final boolean escapedWhitespaceRequiredForLiterals;
    private final Set<Character> patternExceptions;
    private final boolean escapedWhitespaceRequiredForPatterns;
    private final Map<String,Set<Character>> literalMap = new HashMap<>();
    private final Map<String,Set<Character>> patternMap = new HashMap<>();

    /**
     * Returns a {@link UnescapedSpecialCharactersVisitor} that has traversed over the given queryTree and has searched for unescaped special characters in
     * string literals and regex patterns, taking into account the given exceptions. Whitespace characters will not be flagged if they are not escaped.
     *
     * @param queryTree
     *            the query
     * @param literalExceptions
     *            the characters that may be unescaped in string literals
     * @param patternExceptions
     *            the characters that may be unescaped in regex patterns. This will always include the set of regex-reserved characters.
     * @return the visitor
     */
    public static UnescapedSpecialCharactersVisitor check(JexlNode queryTree, Set<Character> literalExceptions, Set<Character> patternExceptions) {
        return check(queryTree, literalExceptions, false, patternExceptions, false);
    }

    /**
     * Returns a {@link UnescapedSpecialCharactersVisitor} that has traversed over the given queryTree and has searched for unescaped special characters in
     * string literals and regex patterns, taking into account the given exceptions and whitespace criteria.
     *
     * @param queryTree
     *            the query
     * @param literalExceptions
     *            the characters that may be unescaped in string literals
     * @param escapedWhitespaceRequiredForLiterals
     *            if true, whitespace characters must be escaped in string literals or they will be flagged
     * @param patternExceptions
     *            the characters that may be unescaped in regex patterns. This will always include the set of regex-reserved characters.
     * @param escapedWhitespaceRequiredForPatterns
     *            if true, whitespace characters must be escaped in regex patterns or they will be flagged
     * @return the visitor
     */
    public static UnescapedSpecialCharactersVisitor check(JexlNode queryTree, Set<Character> literalExceptions, boolean escapedWhitespaceRequiredForLiterals,
                    Set<Character> patternExceptions, boolean escapedWhitespaceRequiredForPatterns) {
        UnescapedSpecialCharactersVisitor visitor = new UnescapedSpecialCharactersVisitor(literalExceptions, escapedWhitespaceRequiredForLiterals,
                        patternExceptions, escapedWhitespaceRequiredForPatterns);
        queryTree.jjtAccept(visitor, null);
        return visitor;
    }

    private UnescapedSpecialCharactersVisitor(Set<Character> literalExceptions, boolean escapedWhitespaceRequiredForLiterals, Set<Character> patternExceptions,
                    boolean escapedWhitespaceRequiredForPatterns) {
        this.literalExceptions = new HashSet<>(literalExceptions);
        // Ensure the allowed pattern special characters always include the seto of pattern reserved characters.
        this.patternExceptions = new HashSet<>(patternExceptions);
        this.patternExceptions.addAll(patternReservedCharacters);
        this.escapedWhitespaceRequiredForLiterals = escapedWhitespaceRequiredForLiterals;
        this.escapedWhitespaceRequiredForPatterns = escapedWhitespaceRequiredForPatterns;
    }

    /**
     * Returns a map of string literals where unescaped characters were found to their unescaped characters.
     *
     * @return the multimap
     */
    public SetMultimap<String,Character> getUnescapedCharactersInLiterals() {
        return getMultimap(literalMap);
    }

    /**
     * Returns a map of pattern patterns unescaped characters were found to their unescaped characters.
     *
     * @return the multimap
     */
    public SetMultimap<String,Character> getUnescapedCharactersInPatterns() {
        return getMultimap(patternMap);
    }

    // Returns a {@link SetMultimap} of all entries in the given map where the value set is not empty.

    /**
     * Returns a {@link SetMultimap} of all entries in the given map where the value set is not empty.
     *
     * @param map
     *            the map
     * @return the multimap
     */
    private SetMultimap<String,Character> getMultimap(Map<String,Set<Character>> map) {
        // @formatter:off
        Map<String, Set<Character>> relevantEntries = map.entrySet().stream()
                        .filter(entry -> !entry.getValue().isEmpty())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // @formatter:on
        // Maintain insertion order.
        SetMultimap<String,Character> multimap = LinkedHashMultimap.create();
        relevantEntries.forEach(multimap::putAll);
        return multimap;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        checkLiteral(node);
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        checkLiteral(node);
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        checkLiteral(node);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        checkLiteral(node);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        checkLiteral(node);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        checkLiteral(node);
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        checkPattern(node);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        checkPattern(node);
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        if (descriptor == null) {
            throw new IllegalStateException("Could not get descriptor for ASTFunctionNode");
        }

        // Determine which evaluation function to use based of whether the function accepts pattern or not.
        Consumer<JexlNode> evaluationFunction = descriptor.regexArguments() ? this::checkPattern : this::checkLiteral;

        FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
        functionVisitor.visit(node, null);
        List<JexlNode> args = functionVisitor.args();
        args.stream().filter(arg -> arg instanceof ASTStringLiteral).forEach(evaluationFunction);

        return data;
    }

    // Check the given node for a literal string with unescaped characters.
    private void checkLiteral(JexlNode node) {
        checkValue(node, this.literalMap, this.literalExceptions, this.escapedWhitespaceRequiredForLiterals);
    }

    // Check the given node for a pattern value with unescaped characters.
    private void checkPattern(JexlNode node) {
        checkValue(node, this.patternMap, this.patternExceptions, this.escapedWhitespaceRequiredForPatterns);
    }

    // Check the given node for unescaped characters, using the given list of exceptions, and whether whitespace characters must be escaped, and add them to the
    // specified map.
    private void checkValue(JexlNode node, Map<String,Set<Character>> map, Set<Character> exceptions, boolean escapedWhitespaceRequired) {
        Object literalValue;
        // Catch the situation where no literal was given, e.g. FIELD1 !~ FIELD2.
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (Exception e) {
            return;
        }

        if (literalValue != null && String.class.equals(literalValue.getClass())) {
            String literalString = (String) literalValue;
            // Check if we have already examined this string before.
            if (map.containsKey(literalString)) {
                return;
            }

            Set<Character> characters = getUnescapedSpecialChars(literalString, exceptions, escapedWhitespaceRequired);
            map.put(literalString, characters);
        }
    }

    private Set<Character> getUnescapedSpecialChars(String str, Set<Character> allowedSpecialCharacters, boolean escapedWhitespaceRequired) {
        if (str.isEmpty()) {
            return Collections.emptySet();
        }

        // Maintain insertion order.
        Set<Character> unescapedChars = new LinkedHashSet<>();
        char[] chars = str.toCharArray();
        int totalChars = chars.length;
        int lastIndex = totalChars - 1;
        boolean isPrevBackslash = false;

        // Examine each character in the string.
        for (int currIndex = 0; currIndex < totalChars; currIndex++) {
            char currChar = chars[currIndex];
            if (currChar == BACKSLASH_CHAR) {
                // If the previous character was a backslash, then this is an escaped backslash. Reset the isPrevBackslash flag and proceed to the next
                // character.
                if (isPrevBackslash) {
                    isPrevBackslash = false;
                } else {
                    // If we have characters remaining, this backlash escapes the next character.
                    if (currIndex != lastIndex) {
                        isPrevBackslash = true;
                    } else {
                        // If this is the last character, it is an unescaped backslash. Treat it as invalid if it is not part of the allowed special characters.
                        if (!allowedSpecialCharacters.contains(currChar)) {
                            unescapedChars.add(currChar);
                        }
                    }
                }
            } else if (Character.isLetterOrDigit(currChar) || allowedSpecialCharacters.contains(currChar)) {
                // The current character is a letter, digit, or one of the specified special char exceptions.
                isPrevBackslash = false;
            } else if (Character.isWhitespace(currChar)) {
                // The current character is a whitespace. If escaped whitespace characters are required, and the previous character was not a backslash, track
                // the
                if (escapedWhitespaceRequired && !isPrevBackslash) {
                    unescapedChars.add(currChar);
                }
                isPrevBackslash = false;
            } else {
                // The current character is a special character that is not allowed to be unescaped.
                if (!isPrevBackslash) {
                    // The character was not escaped by a backlash. Retain the character..
                    unescapedChars.add(currChar);
                }
                isPrevBackslash = false;
            }
        }
        return unescapedChars;
    }

}
