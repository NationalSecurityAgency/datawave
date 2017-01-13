package nsa.datawave.webservice.edgedictionary;

import java.util.Set;

import nsa.datawave.webservice.results.edgedictionary.EdgeDictionaryBase;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

/**
 * 
 */
public interface DatawaveEdgeDictionary {
    public EdgeDictionaryBase getEdgeDictionary(String metadataTableName, Connector connector, Set<Authorizations> auths, int numThreads) throws Exception;
}
