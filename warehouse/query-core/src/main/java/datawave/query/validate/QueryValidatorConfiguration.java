package datawave.query.validate;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.util.MetadataHelper;

public interface QueryValidatorConfiguration {

    GenericQueryConfiguration getQueryConfiguration();
    
    void setQueryConfiguration(GenericQueryConfiguration config);
    
    String getQuery();
    
    void setQuery(String query);
    
    String getQuerySyntax();
    
    void setQuerySyntax(String querySyntax);
    
    MetadataHelper getMetadataHelper();
    
    void setMetadataHelper(MetadataHelper metadataHelper);
    
    QueryValidatorConfiguration getBaseCopy();
}
