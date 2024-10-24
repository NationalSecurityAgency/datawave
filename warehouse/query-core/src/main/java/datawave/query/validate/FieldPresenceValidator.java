package datawave.query.validate;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldPresenceValidator implements QueryValidator {
    
    private static final Set<String> supportedSyntaxes = Collections.singleton("JEXL");
    
    private Map<String, String> fieldMessages;
    
    public Map<String,String> getFieldMessages() {
        return fieldMessages;
    }
    
    public void setFieldMessages(Map<String,String> fieldMessages) {
        this.fieldMessages = fieldMessages;
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
        Set<String> fields = QueryFieldsVisitor.parseQueryFields(query, metadataHelper);
        List<String> messages = new ArrayList<>();
        for (String field : fieldMessages.keySet()) {
            if (fields.contains(field)) {
                messages.add(fieldMessages.get(field));
            }
        }
        return messages;
    }
}
