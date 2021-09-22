package datawave.webservice.common.edgedictionary;

import datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import datawave.webservice.results.edgedictionary.MetadataBase;

public interface EdgeDictionaryProvider {
    EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(String metadataTableName, String auths);
}
