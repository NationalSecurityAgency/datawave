package datawave.query.validate;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.MetadataHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TimeFunctionValidator implements QueryValidator {
    
    private static final Set<String> supportedSyntaxes = Collections.singleton("JEXL");
    
    @Override
    public Set<String> getSupportedQuerySyntaxes() {
        return supportedSyntaxes;
    }
    
    @Override
    public List<String> evaluate(String query, String querySyntax, GenericQueryConfiguration config, MetadataHelper metadataHelper) throws Exception {
        
        return null;
    }
}
