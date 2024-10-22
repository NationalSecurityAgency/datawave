package datawave.query.validate;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.FieldMissingFromSchemaVisitor;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FieldExistenceValidator implements QueryValidator {
    
    private static final Set<String> supportedSyntaxes = Collections.singleton("JEXL");
    
    private FieldExistenceValidatorConfiguration config;
    
    @Override
    public QueryValidatorConfiguration getConfig() {
        return config;
    }
    
    @Override
    public void setConfig(QueryValidatorConfiguration config) {
        if (config instanceof FieldExistenceValidatorConfiguration) {
            this.config = (FieldExistenceValidatorConfiguration) config;
        } else {
            throw new IllegalArgumentException("Config must be an instance of " + FieldExistenceValidatorConfiguration.class);
        }
    }
    
    @Override
    public Set<String> getSupportedQuerySyntaxes() {
        return supportedSyntaxes;
    }
    
    @Override
    public List<String> validate(QueryValidatorConfiguration config) throws Exception {
        if (!(config instanceof FieldExistenceValidatorConfiguration)) {
            throw new IllegalArgumentException("Config must be instance of " + FieldExistenceValidatorConfiguration.class);
        }
        
        FieldExistenceValidatorConfiguration configuration = (FieldExistenceValidatorConfiguration) config;
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(configuration.getQuery());
        Set<String> nonExistentFields = FieldMissingFromSchemaVisitor.getNonExistentFields(configuration.getMetadataHelper(), queryTree, Collections.emptySet(),
                        configuration.getSpecialFields());
        if (!nonExistentFields.isEmpty()) {
            return Collections.singletonList("Fields not found in data dictionary: " + String.join(",", nonExistentFields));
        } else {
            return Collections.emptyList();
        }
    }
}
