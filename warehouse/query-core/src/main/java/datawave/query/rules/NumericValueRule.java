package datawave.query.rules;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.NumberType;
import datawave.query.jexl.visitors.FieldsWithNumericRangeValuesVisitor;
import datawave.query.util.TypeMetadata;

public class NumericValueRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(NumericValueRule.class);

    private static final String NUMBER_TYPE = NumberType.class.getName();

    public NumericValueRule() {}

    public NumericValueRule(String name) {
        super(name);
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

            // Identify fields that participate in range comparisons (<, >, <=, >=), including those derived from Lucene ranges.
            Set<String> fields = FieldsWithNumericRangeValuesVisitor.getFields(jexlScript);

            // If no range fields were found, or no type metadata is available, nothing to validate.
            TypeMetadata typeMetadata = ruleConfig.getTypeMetadata();
            if (typeMetadata == null) {
                throw new IllegalStateException("TypeMetadata should not be null.");
            }
            if (!fields.isEmpty()) {
                // A temporary cache to avoid unnecessary lookups via TypeMetadata if we see a field more than once.
                Multimap<String,String> types = HashMultimap.create();
                // Maintain insertion order.
                Set<String> nonNumericFields = new LinkedHashSet<>();
                // Find any fields that are not a number type.
                for (String field : fields) {
                    if (!types.containsKey(field)) {
                        types.putAll(field, typeMetadata.getNormalizerNamesForField(field));
                    }

                    // If the field does not have NumberType as one of its normalizers, flag it.
                    if (!types.containsEntry(field, NUMBER_TYPE)) {
                        nonNumericFields.add(field);
                    }
                }
                // If any non-numeric fields were used in a range, add a message to the result.
                if (!nonNumericFields.isEmpty()) {
                    result.addMessage("Range values supplied for non-numeric field(s): " + String.join(", ", nonNumericFields));
                }
            }
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }

        return result;
    }

    @Override
    public QueryRule copy() {
        return new NumericValueRule(name);
    }
}
