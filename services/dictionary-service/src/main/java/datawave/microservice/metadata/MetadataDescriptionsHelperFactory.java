package datawave.microservice.metadata;

import datawave.webservice.results.datadictionary.DescriptionBase;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

@Component
public class MetadataDescriptionsHelperFactory<DESC extends DescriptionBase<DESC>> {
    @Lookup
    public MetadataDescriptionsHelper<DESC> createMetadataDescriptionsHelper() {
        // return nothing since spring will create a proxy for this method
        return null;
    }
}
