package datawave.query.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.collect.SetMultimap;

import datawave.query.jexl.visitors.UnescapedSpecialCharactersVisitor;

/**
 * A {@link QueryRule} implementation that will check a query for any unescaped characters in literals or patterns in a JEXL query.
 */
public class UnescapedSpecialCharsRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(UnescapedSpecialCharsRule.class);

    private static final String REGEX_VALUE_DESCRIPTOR = "Regex pattern";
    private static final String LITERAL_VALUE_DESCRIPTOR = "Literal string";

    private Set<Character> literalExceptions = new HashSet<>();
    private Set<Character> patternExceptions = new HashSet<>();
    private boolean escapedWhitespaceRequiredForLiterals = false;
    private boolean escapedWhitespaceRequiredForPatterns = false;

    public UnescapedSpecialCharsRule() {}

    public UnescapedSpecialCharsRule(String name) {
        super(name);
    }

    public UnescapedSpecialCharsRule(String name, Set<Character> literalExceptions, Set<Character> patternExceptions,
                    boolean escapedWhitespaceRequiredForLiterals, boolean escapedWhitespaceRequiredForPatterns) {
        super(name);
        this.literalExceptions = literalExceptions == null ? Set.of() : Set.copyOf(literalExceptions);
        this.patternExceptions = patternExceptions == null ? Set.of() : Set.copyOf(patternExceptions);
        this.escapedWhitespaceRequiredForLiterals = escapedWhitespaceRequiredForLiterals;
        this.escapedWhitespaceRequiredForPatterns = escapedWhitespaceRequiredForPatterns;
    }

    /**
     * Returns the set the characters that should not trigger a notice if they are present and unescaped in a literal.
     *
     * @return a set of characters
     */
    public Set<Character> getLiteralExceptions() {
        return literalExceptions;
    }

    /**
     * Sets the characters that should not trigger a notice if they are present and unescaped in a literal.
     *
     * @param literalExceptions
     *            the characters
     */
    public void setLiteralExceptions(Set<Character> literalExceptions) {
        if (literalExceptions == null) {
            this.literalExceptions = Collections.emptySet();
        } else {
            this.literalExceptions = Collections.unmodifiableSet(literalExceptions);
        }
    }

    /**
     * Returns the set the characters that should not trigger a notice if they are present and unescaped in a pattern.
     *
     * @return a set of characters
     */
    public Set<Character> getPatternExceptions() {
        return patternExceptions;
    }

    /**
     * Sets the characters that should not trigger a notice if they are present and unescaped in a pattern. Note that the following reserved regex characters
     * will always be considered exceptions: {@code '.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\'}.
     *
     * @param patternExceptions
     *            the characters
     */
    public void setPatternExceptions(Set<Character> patternExceptions) {
        if (patternExceptions == null) {
            this.patternExceptions = Collections.emptySet();
        } else {
            this.patternExceptions = Collections.unmodifiableSet(patternExceptions);
        }
    }

    /**
     * Return whether whitespace must be escaped in literals.
     *
     * @return true if whitespace characters must be escaped in literals, or false otherwise.
     */
    public boolean isEscapedWhitespaceRequiredForLiterals() {
        return escapedWhitespaceRequiredForLiterals;
    }

    /**
     * Sets whether whitespace must be escaped in literals.
     *
     * @param escapedWhitespaceRequiredForLiterals
     *            true if whitespace characters must be escaped in literals, or false otherwise.
     */
    public void setEscapedWhitespaceRequiredForLiterals(boolean escapedWhitespaceRequiredForLiterals) {
        this.escapedWhitespaceRequiredForLiterals = escapedWhitespaceRequiredForLiterals;
    }

    /**
     * Return whether whitespace must be escaped in regex patterns.
     *
     * @return true if whitespace characters must be escaped in regex patterns, or false otherwise.
     */
    public boolean isEscapedWhitespaceRequiredForPatterns() {
        return escapedWhitespaceRequiredForPatterns;
    }

    /**
     * Sets whether whitespace must be escaped in regex patterns.
     *
     * @param escapedWhitespaceRequiredForPatterns
     *            true if whitespace characters must be escaped in regex patterns, or false otherwise.
     */
    public void setEscapedWhitespaceRequiredForPatterns(boolean escapedWhitespaceRequiredForPatterns) {
        this.escapedWhitespaceRequiredForPatterns = escapedWhitespaceRequiredForPatterns;
    }

    @Override
    protected Syntax getSupportedSyntax() {
        return Syntax.JEXL;
    }

    @Override
    public QueryRuleResult validate(QueryValidationConfiguration configuration) throws Exception {
        ShardQueryValidationConfiguration ruleConfig = (ShardQueryValidationConfiguration) configuration;
        if (log.isDebugEnabled()) {
            log.debug("Validating config against instance '" + getName() + "' of " + getClass() + ": " + ruleConfig);
        }

        QueryRuleResult result = new QueryRuleResult(getName());
        try {
            ASTJexlScript jexlScript = (ASTJexlScript) ruleConfig.getParsedQuery();
            // Check the query for unescaped characters.
            UnescapedSpecialCharactersVisitor visitor = UnescapedSpecialCharactersVisitor.check(jexlScript, literalExceptions,
                            escapedWhitespaceRequiredForLiterals, patternExceptions, escapedWhitespaceRequiredForPatterns);
            // Add messages for any unescaped special characters seen in literals or regex patterns.
            result.addMessages(getMessages(visitor.getUnescapedCharactersInLiterals(), LITERAL_VALUE_DESCRIPTOR));
            result.addMessages(getMessages(visitor.getUnescapedCharactersInPatterns(), REGEX_VALUE_DESCRIPTOR));
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }

        return result;
    }

    @Override
    public QueryRule copy() {
        return new UnescapedSpecialCharsRule(name, literalExceptions, patternExceptions, escapedWhitespaceRequiredForLiterals,
                        escapedWhitespaceRequiredForPatterns);
    }

    /**
     * Returns a list of formatted messages regarding the given unescaped characters.
     *
     * @param unescapedCharacters
     *            the unescaped characters.
     * @param valueDescriptor
     *            the value descriptor, i.e. whether we're looking at string literals or regex patterns.
     * @return
     */
    private List<String> getMessages(SetMultimap<String,Character> unescapedCharacters, String valueDescriptor) {
        List<String> messages = new ArrayList<>();
        for (String key : unescapedCharacters.keySet()) {
            Set<Character> characters = unescapedCharacters.get(key);
            StringBuilder sb = new StringBuilder();
            sb.append(valueDescriptor).append(" \"").append(key).append("\" has the following unescaped special character(s): ");
            String characterList = characters.stream().map(ch -> "'" + ch + "'").collect(Collectors.joining(", "));
            sb.append(characterList);
            messages.add(sb.toString());
        }
        return messages;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        UnescapedSpecialCharsRule rule = (UnescapedSpecialCharsRule) object;
        return escapedWhitespaceRequiredForLiterals == rule.escapedWhitespaceRequiredForLiterals
                        && escapedWhitespaceRequiredForPatterns == rule.escapedWhitespaceRequiredForPatterns
                        && Objects.equals(literalExceptions, rule.literalExceptions) && Objects.equals(patternExceptions, rule.patternExceptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Set.copyOf(literalExceptions), Set.copyOf(patternExceptions), escapedWhitespaceRequiredForLiterals,
                        escapedWhitespaceRequiredForPatterns);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UnescapedSpecialCharsRule.class.getSimpleName() + "[", "]").add("name='" + name + "'")
                        .add("literalExceptions=" + literalExceptions).add("patternExceptions=" + patternExceptions)
                        .add("escapedWhitespaceRequiredForLiterals=" + escapedWhitespaceRequiredForLiterals)
                        .add("escapedWhitespaceRequiredForPatterns=" + escapedWhitespaceRequiredForPatterns).toString();
    }
}
