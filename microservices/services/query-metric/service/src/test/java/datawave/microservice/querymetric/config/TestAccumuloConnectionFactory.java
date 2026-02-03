package datawave.microservice.querymetric.config;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.common.connection.AccumuloClientPoolFactory;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.result.ConnectionPoolsProperties;

class TestAccumuloConnectionFactory extends AccumuloConnectionFactoryImpl {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    public TestAccumuloConnectionFactory(ConnectionPoolsProperties props) {
        super(null, props);
    }

    public static AccumuloConnectionFactory getInstance(ConnectionPoolsProperties config) {
        if (factory == null) {
            synchronized (AccumuloConnectionFactoryImpl.class) {
                if (factory == null) {
                    setFactory(new TestAccumuloConnectionFactory(config));
                }
            }
        }
        return factory;
    }

    @Override
    protected AccumuloClientPool createConnectionPool(ConnectionPoolProperties connectionPoolProperties, int limit) {
        try {
            AccumuloClientPool pool = new AccumuloClientPool(new InMemoryAccumuloClientPoolFactory(connectionPoolProperties));
            pool.setTestOnBorrow(true);
            pool.setTestOnReturn(true);
            pool.setMaxTotal(limit);
            pool.setMaxIdle(-1);

            try {
                pool.addObject();
            } catch (Exception e) {
                log.error("Error pre-populating connection pool", e);
            }
            return pool;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class InMemoryAccumuloClientPoolFactory extends AccumuloClientPoolFactory {

        private ConnectionPoolProperties connectionPoolProperties;

        public InMemoryAccumuloClientPoolFactory(ConnectionPoolProperties connectionPoolProperties) throws Exception {
            super(connectionPoolProperties.getUsername(), connectionPoolProperties.getPassword(), "mock", "mock");
            this.connectionPoolProperties = connectionPoolProperties;
            try (AccumuloClient accumuloClient = new InMemoryAccumuloClient(connectionPoolProperties.getUsername(),
                            new InMemoryInstance(connectionPoolProperties.getInstance()))) {
                accumuloClient.securityOperations().changeUserAuthorizations(accumuloClient.whoami(), new Authorizations("PUBLIC", "A", "B", "C"));
            } catch (AccumuloSecurityException e) {
                log.error(e.getMessage(), e);
            }
        }

        public PooledObject<AccumuloClient> makeObject() throws Exception {
            return new DefaultPooledObject(new InMemoryAccumuloClient(this.connectionPoolProperties.getUsername(),
                            new InMemoryInstance(connectionPoolProperties.getInstance())));
        }
    }
}
