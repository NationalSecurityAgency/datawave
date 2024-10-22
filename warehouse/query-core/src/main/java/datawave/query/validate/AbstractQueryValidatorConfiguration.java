package datawave.query.validate;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.util.MetadataHelper;

import javax.xml.bind.annotation.XmlTransient;

public abstract class AbstractQueryValidatorConfiguration implements QueryValidatorConfiguration {
    
    @XmlTransient
    protected GenericQueryConfiguration queryConfig;
    
    @XmlTransient
    protected String query;
    
    @XmlTransient
    protected String querySyntax;
    
    @XmlTransient
    protected MetadataHelper metadataHelper;
    
    @Override
    public GenericQueryConfiguration getQueryConfiguration() {
        return queryConfig;
    }
    
    @Override
    public void setQueryConfiguration(GenericQueryConfiguration config) {
        this.queryConfig = queryConfig;
    }
    
    @Override
    public String getQuery() {
        return query;
    }
    
    @Override
    public void setQuery(String query) {
        this.query = query;
    }
    
    @Override
    public String getQuerySyntax() {
        return querySyntax;
    }
    
    @Override
    public void setQuerySyntax(String querySyntax) {
        this.querySyntax = querySyntax;
    }
    
    @Override
    public MetadataHelper getMetadataHelper() {
        return metadataHelper;
    }
    
    @Override
    public void setMetadataHelper(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }
    
    @Override
    public abstract QueryValidatorConfiguration getBaseCopy();
}
