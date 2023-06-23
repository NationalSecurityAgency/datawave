package datawave.webservice.edgedictionary;

import datawave.core.common.edgedictionary.EdgeDictionaryProvider;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;
import datawave.webservice.query.Query;

import javax.inject.Inject;

public class EdgeDictionaryProviderImpl implements EdgeDictionaryProvider {
    @Inject
    private RemoteEdgeDictionary remoteEdgeDictionary;

    @Override
    public EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName) {
        return remoteEdgeDictionary.getEdgeDictionary(settings, metadataTableName);
    }
}
