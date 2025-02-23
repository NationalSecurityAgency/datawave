package datawave.microservice.dictionary.edge;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;

public interface EdgeDictionary<EDGE extends EdgeDictionaryBase<EDGE,META>,META extends MetadataBase<META>> {
    char COL_SEPARATOR = '/';
    
    EDGE getEdgeDictionary(String metadataTableName, AccumuloClient accumuloClient, Set<Authorizations> auths, int numThreads) throws Exception;
}
