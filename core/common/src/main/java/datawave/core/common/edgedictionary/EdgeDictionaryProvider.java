package datawave.core.common.edgedictionary;

import datawave.microservice.query.Query;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;

public interface EdgeDictionaryProvider {
    EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName);
}
