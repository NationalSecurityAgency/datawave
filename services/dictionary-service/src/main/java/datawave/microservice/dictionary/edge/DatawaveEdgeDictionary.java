package datawave.microservice.dictionary.edge;

import datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import datawave.webservice.results.edgedictionary.MetadataBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

public interface DatawaveEdgeDictionary<EDGE extends EdgeDictionaryBase<EDGE,META>,META extends MetadataBase<META>> {
    char COL_SEPARATOR = '/';
    
    EDGE getEdgeDictionary(String metadataTableName, Connector connector, Set<Authorizations> auths, int numThreads) throws Exception;
}
