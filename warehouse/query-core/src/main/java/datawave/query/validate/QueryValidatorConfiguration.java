package datawave.query.validate;

import datawave.query.util.MetadataHelper;

public interface QueryValidatorConfiguration {

    MetadataHelper getMetadataHelper();
    
    void setMetadataHelper(MetadataHelper metadataHelper);
}
