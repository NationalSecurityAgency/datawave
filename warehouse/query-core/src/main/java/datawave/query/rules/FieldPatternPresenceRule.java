package datawave.query.rules;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.jexl.visitors.QueryPatternsVisitor;

/**
 * A {@link QueryRule} implementation that will check return configured messages if any of the configured fields or regex patterns are seen.
 */
public class FieldPatternPresenceRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(FieldPatternPresenceRule.class);

    private Map<String,String> fieldMessages = Collections.emptyMap();
    private Map<String,String> patternMessages = Collections.emptyMap();

    public FieldPatternPresenceRule() {}

    public FieldPatternPresenceRule(String name) {
        super(name);
    }

    public FieldPatternPresenceRule(String name, Map<String,String> fieldMessages, Map<String,String> patternMessages) {
        super(name);
        this.fieldMessages = fieldMessages == null ? Map.of() : Map.copyOf(fieldMessages);
        this.patternMessages = patternMessages == null ? Map.of() : Map.copyOf(patternMessages);
    }

    /**
     * Return the map of fields to messages
     *
     * @return the field message map
     */
    public Map<String,String> getFieldMessages() {
        return fieldMessages;
    }

    /**
     * Sets the map of fields to messages that should be returned if the field keys are seen in a query. If the map is null, an empty map will be set, otherwise
     * the map will be copied and each field key will be trimmed and capitalized.
     *
     * @param fieldMessages
     *            the field messages
     */
    public void setFieldMessages(Map<String,String> fieldMessages) {
        if (fieldMessages == null) {
            this.fieldMessages = Collections.emptyMap();
        } else {
            // @formatter:off
            // Ensured any configured field keys are trimmed and uppercased.
            this.fieldMessages = fieldMessages.entrySet().stream()
                            .collect(Collectors.toUnmodifiableMap(
                                            entry -> entry.getKey().trim().toUpperCase(),
                                            Map.Entry::getValue));
            // @formatter:on
        }
    }

    public Map<String,String> getPatternMessages() {
        return patternMessages;
    }

    /**
     * Sets the map of patterns to messages that should be returned if the field keys are seen in a query
     *
     * @param patternMessages
     *            the pattern messages
     */
    public void setPatternMessages(Map<String,String> patternMessages) {
        if (patternMessages == null) {
            this.patternMessages = Collections.emptyMap();
        } else {
            this.patternMessages = Collections.unmodifiableMap(patternMessages);
        }
    }

    @Override
    protected Syntax getSupportedSyntax() {
        return Syntax.JEXL;
    }

    @Override
    public QueryRuleResult validate(QueryValidationConfiguration ruleConfiguration) throws Exception {
        ShardQueryValidationConfiguration ruleConfig = (ShardQueryValidationConfiguration) ruleConfiguration;
        if (log.isDebugEnabled()) {
            log.debug("Validating config against instance '" + getName() + "' of " + getClass() + ": " + ruleConfig);
        }

        QueryRuleResult result = new QueryRuleResult(getName());
        try {
            ASTJexlScript jexlScript = (ASTJexlScript) ruleConfig.getParsedQuery();
            // Fetch the set of fields if any field presence messages were configured.
            if (!fieldMessages.isEmpty()) {
                Set<String> fields = QueryFieldsVisitor.parseQueryFields(jexlScript, ruleConfig.getMetadataHelper());
                // @formatter:off
                Sets.intersection(fieldMessages.keySet(), fields).stream() // Get the matching fields.
                                .map(fieldMessages::get) // Fetch their associated message.
                                .forEach(result::addMessage); // Add the message to the result.
                // @formatter:on
            }

            // Fetch the set of patterns if any pattern messages were configured.
            if (!patternMessages.isEmpty()) {
                Set<String> patterns = QueryPatternsVisitor.findPatterns(jexlScript);
                // @formatter:off
                Sets.intersection(patternMessages.keySet(), patterns).stream() // Get the matching pattern.
                                .map(patternMessages::get) // Fetch their associated message.
                                .forEach(result::addMessage); // Add the message to the result.
                // @formatter:on
            }
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }

        return result;
    }

    @Override
    public QueryRule copy() {
        return new FieldPatternPresenceRule(name, fieldMessages, patternMessages);
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
        FieldPatternPresenceRule rule = (FieldPatternPresenceRule) object;
        return Objects.equals(fieldMessages, rule.fieldMessages) && Objects.equals(patternMessages, rule.patternMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldMessages, patternMessages);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FieldPatternPresenceRule.class.getSimpleName() + "[", "]").add("name='" + name + "'")
                        .add("fieldMessages=" + fieldMessages).add("patternMessages=" + patternMessages).toString();
    }
}
