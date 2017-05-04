package nsa.datawave.webservice.edgedictionary;

import nsa.datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

/**
 * Stub version of an {@link DatawaveEdgeDictionary} for use in integration tests.
 */
public class TestDatawaveEdgeDictionaryImpl implements DatawaveEdgeDictionary {
    @Override
    public EdgeDictionaryBase getEdgeDictionary(String metadataTableName, Connector connector, Set<Authorizations> auths, int numThreads) throws Exception {
        return null;
    }
}
