package datawave.microservice.querymetric.factory;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloClientPoolFactory;
import datawave.webservice.common.connection.WrappedAccumuloClient;

/*
 * The InMemoryInstance in this class doesn't cache anything and is only used to create
 * a wrapped connector because the ScannerHelper expects a WrappedConnector and
 * logs an exception if it gets an unwrapped connector
 */
public class WrappedAccumuloClientPoolFactory extends AccumuloClientPoolFactory {
    private Logger log = LoggerFactory.getLogger(getClass());
    private AccumuloClientPoolFactory accumuloClientPoolFactory;
    private AccumuloClient inMemoryAccumuloClient;
    
    public WrappedAccumuloClientPoolFactory(AccumuloClientPoolFactory accumuloClientPoolFactory) {
        super("", "", "", "");
        this.accumuloClientPoolFactory = accumuloClientPoolFactory;
        try {
            inMemoryAccumuloClient = new InMemoryAccumuloClient("mock", new InMemoryInstance());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    @Override
    public PooledObject<AccumuloClient> makeObject() throws Exception {
        return new DefaultPooledObject(new WrappedAccumuloClient(this.accumuloClientPoolFactory.makeObject().getObject(), inMemoryAccumuloClient));
    }
}
