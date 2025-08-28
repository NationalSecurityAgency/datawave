package datawave.query.rules;

import java.text.NumberFormat;
import java.util.LinkedHashSet;
import java.util.Set;

import com.ibm.icu.impl.number.parse.ParsedNumber;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.PrintingVisitor;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.NumberType;
import datawave.query.jexl.visitors.FieldsWithNumericValuesVisitor;
import datawave.query.util.TypeMetadata;

/**
 * Implementation of {@link QueryRule} that will verify that fields with numeric values are actually numeric fields.
 */
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
            PrintingVisitor.printQuery(jexlScript);
            // Fetch the set of fields that have numeric values.
            Set<String> fields = FieldsWithNumericValuesVisitor.getFields(jexlScript); // TODO ALLLLLLLL FIELDS WITH A NUMERIC VALUE... wtf this SHOULD return non-REGEX value'd fields.......
            // ooooooo actually!!!!!!! it might not even be this visitor cuz the regex stuff ISNT ACTUALLY REGEX!!!!! it's a range node. look for that


            // If fields with numeric values were found, check the field types.
            if (!fields.isEmpty()) {
                // A temporary cache to avoid unnecessary lookups via TypeMetadata if we see a field more than once.
                Multimap<String,String> types = HashMultimap.create();
                TypeMetadata typeMetadata = ruleConfig.getTypeMetadata();
                // Maintain insertion order.
                Set<String> nonNumericFields = new LinkedHashSet<>();
                // Find any fields that are not a number type.
                for (String field : fields) {
                    if (!types.containsKey(field)) {
                        types.putAll(field, typeMetadata.getNormalizerNamesForField(field));
                    }

                    //get all nodes
                    // for each one check if its type is numeric
                    // if it is, check its value for an actual numbner
                    // if it's not (like if it's regex) then throw the error


//                    Number num = NumberFormat.getInstance().parse(types.get(typeMetadata.getNormalizerNamesForField(field)));
                    //yea but how the heck do i even get the value, there's no value anywhere just the fields

                    if (!types.containsEntry(field, NUMBER_TYPE)) { //ADD IT HERER!!!! tHIS IS WHERE YOU DOUBLE CHECK THAT THE VALUE SAVED ISN'T A CONST LIKE 1234
                        nonNumericFields.add(field);

                        //the prob above is that NUMBER_TYPE can be either a const or regex, right? so i need to cehck if the value is just a double
                    }
                }
                // If any non-numeric fields were specified with numeric values, add a message to the result.
                if (!nonNumericFields.isEmpty()) { //todo: UPDATE THIS TO CARE ONLY ABOUT RANGES. CONSTANTS LIKE 123 DONT MATTER
                    result.addMessage("Numeric values supplied for non-numeric field(s): " + String.join(", ", nonNumericFields));
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
