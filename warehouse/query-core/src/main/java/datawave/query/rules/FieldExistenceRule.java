package datawave.query.rules;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import datawave.query.jexl.visitors.FieldMissingFromSchemaVisitor;

/**
 * A {@link QueryRule} implementation that will check a query for any non-existent fields, i.e. not present in the data dictionary.
 */
public class FieldExistenceRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(FieldExistenceRule.class);

    private Set<String> specialFields = Collections.emptySet();

    public FieldExistenceRule() {}

    public FieldExistenceRule(String name) {
        super(name);
    }

    public FieldExistenceRule(String name, Set<String> specialFields) {
        super(name);
        this.specialFields = specialFields == null ? Set.of() : Set.copyOf(specialFields);
    }

    /**
     * Returns the set of fields that will not trigger a notice if they are present in the query and not in the data dictionary.
     */
    public Set<String> getSpecialFields() {
        return specialFields;
    }

    /**
     * Sets the fields that will not trigger a notice if they are present in the query and not in the data dictionary. If the given collection is null, an empty
     * set will be used. Otherwise, the given collection will be copied, trimmed, and capitalized.
     *
     * @param specialFields
     *            the field exceptions
     */
    public void setSpecialFields(Set<String> specialFields) {
        if (specialFields == null) {
            this.specialFields = Collections.emptySet();
        } else {
            // @formatter:off
            // Ensure any configured fields are trimmed and capitalized.
            this.specialFields = specialFields.stream()
                            .map(String::trim)
                            .map(String::toUpperCase)
                            .collect(Collectors.toUnmodifiableSet());
            // @formatter:on
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
            // Fetch the set of non-existent fields.
            ASTJexlScript jexlQuery = (ASTJexlScript) ruleConfig.getParsedQuery();
            Set<String> nonExistentFields = FieldMissingFromSchemaVisitor.getNonExistentFields(ruleConfig.getMetadataHelper(), jexlQuery,
                            Collections.emptySet(), getSpecialFields());
            // If any non-existent fields were found, add them to the result.
            if (!nonExistentFields.isEmpty()) {
                result.addMessage("Fields not found in data dictionary: " + String.join(", ", nonExistentFields));
            }
        } catch (Exception e) {
            // If an exception occurred, log and preserve it in the result.
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new FieldExistenceRule(name, specialFields);
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
        FieldExistenceRule rule = (FieldExistenceRule) object;
        return Objects.equals(specialFields, rule.specialFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), specialFields);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FieldExistenceRule.class.getSimpleName() + "[", "]").add("name='" + name + "'").add("specialFields=" + specialFields)
                        .toString();
    }
}
