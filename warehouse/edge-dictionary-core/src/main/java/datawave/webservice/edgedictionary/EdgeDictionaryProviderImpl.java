package datawave.webservice.edgedictionary;

import datawave.core.common.edgedictionary.EdgeDictionaryProvider;
import datawave.microservice.query.Query;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;

public class EdgeDictionaryProviderImpl implements EdgeDictionaryProvider {

    private RemoteEdgeDictionary remoteEdgeDictionary;

    @Override
    public EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName) {
        return remoteEdgeDictionary.getEdgeDictionary(settings, metadataTableName);
    }

    public void setRemoteEdgeDictionary(RemoteEdgeDictionary remoteEdgeDictionary) {
        this.remoteEdgeDictionary = remoteEdgeDictionary;
    }

    public RemoteEdgeDictionary getRemoteEdgeDictionary() {
        return remoteEdgeDictionary;
    }
}
