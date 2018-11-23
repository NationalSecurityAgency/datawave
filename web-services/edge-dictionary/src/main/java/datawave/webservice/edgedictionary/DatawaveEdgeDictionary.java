package datawave.webservice.edgedictionary;

import datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

/**
 * 
 */
public interface DatawaveEdgeDictionary {
    EdgeDictionaryBase getEdgeDictionary(String metadataTableName, Connector connector, Set<Authorizations> auths, int numThreads) throws Exception;
}
