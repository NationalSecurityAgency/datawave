package datawave.services.common.edgedictionary;

import datawave.webservice.query.Query;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;

public interface EdgeDictionaryProvider {
    EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName);
}
