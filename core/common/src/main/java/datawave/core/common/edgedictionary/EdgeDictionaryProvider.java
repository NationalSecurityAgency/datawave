package datawave.core.common.edgedictionary;

import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;
import datawave.webservice.query.Query;

public interface EdgeDictionaryProvider {
    EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName);
}
