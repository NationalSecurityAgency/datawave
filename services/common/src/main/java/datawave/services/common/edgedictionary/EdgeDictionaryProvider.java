package datawave.services.common.edgedictionary;

import datawave.webservice.query.Query;
import datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import datawave.webservice.results.edgedictionary.MetadataBase;

public interface EdgeDictionaryProvider {
    EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName);
}
