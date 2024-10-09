package datawave.query.validate;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.FieldMissingFromSchemaVisitor;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;

import java.util.Collections;
import java.util.Set;

public class FieldExistenceValidator implements QueryValidator {
    
    @Override
    public QueryValidationResult validate(String query, QueryValidatorConfiguration config) throws ParseException {
        if (!(config instanceof FieldExistenceValidatorConfiguration)) {
            throw new IllegalArgumentException("Configuration must be instance of " + FieldExistenceValidatorConfiguration.class);
        }
        
        FieldExistenceValidatorConfiguration configuration = (FieldExistenceValidatorConfiguration) config;
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        Set<String> nonExistentFields = FieldMissingFromSchemaVisitor.getNonExistentFields(configuration.getMetadataHelper(), queryTree, Collections.emptySet(),
                        configuration.getSpecialFields());
        QueryValidationResult result = new QueryValidationResult();
        if (!nonExistentFields.isEmpty()) {
            String message = "Fields not found in data dictionary: " + String.join(",", nonExistentFields);
            result.addMessage(message);
        }
        return result;
    }
}
