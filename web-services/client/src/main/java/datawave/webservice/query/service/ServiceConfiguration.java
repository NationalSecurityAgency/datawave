package datawave.webservice.query.service;

import datawave.webservice.query.service.config.IndexingConfiguration;

import java.io.Serializable;

/**
 * Provides a centralized place to support configuration to code changes that are specific to various servives, such as indexing and iterators.
 */
public class ServiceConfiguration implements Serializable {
    
    private static final ServiceConfiguration instance = new ServiceConfiguration();
    static {
        instance.setIndexingConfiguration(IndexingConfiguration.getDefaultInstance());
    }
    
    IndexingConfiguration indexingConfiguration;
    
    public IndexingConfiguration getIndexingConfiguration() {
        return indexingConfiguration;
    }
    
    public void setIndexingConfiguration(IndexingConfiguration indexingConfiguration) {
        this.indexingConfiguration = indexingConfiguration;
    }
    
    public static ServiceConfiguration getDefaultInstance() {
        return instance;
    }
    
}
