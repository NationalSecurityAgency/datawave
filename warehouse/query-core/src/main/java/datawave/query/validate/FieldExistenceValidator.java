package datawave.query.validate;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.FieldMissingFromSchemaVisitor;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FieldExistenceValidator implements QueryValidator {
    
    private static final Set<String> supportedSyntaxes = Collections.singleton("JEXL");
    
    private Set<String> specialFields;
    
    public Set<String> getSpecialFields() {
        return specialFields;
    }
    
    public void setSpecialFields(Set<String> specialFields) {
        this.specialFields = specialFields;
    }
    
    @Override
    public Set<String> getSupportedQuerySyntaxes() {
        return supportedSyntaxes;
    }
    
    @Override
    public List<String> evaluate(String query, String querySyntax, GenericQueryConfiguration config, MetadataHelper metadataHelper) throws Exception {
        if (!supportedSyntaxes.contains(querySyntax)) {
            throw new IllegalArgumentException("Query syntax " + querySyntax + " is not supported for this evaluator.");
        }
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        Set<String> nonExistentFields = FieldMissingFromSchemaVisitor.getNonExistentFields(metadataHelper, queryTree, Collections.emptySet(),
                        specialFields);
        if (!nonExistentFields.isEmpty()) {
            return Collections.singletonList("Fields not found in data dictionary: " + String.join(",", nonExistentFields));
        } else {
            return Collections.emptyList();
        }
    }
}
