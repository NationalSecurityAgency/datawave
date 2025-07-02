package datawave.core.common.connection;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloClientPoolFactory implements PooledObjectFactory<AccumuloClient> {

    private static final Logger log = LoggerFactory.getLogger(AccumuloClientPoolFactory.class);

    private String username;
    private String password;
    private String instanceName;
    private String zookeepers;

    public AccumuloClientPoolFactory(String username, String password, String zookeepers, String instanceName) {
        this.username = username;
        this.password = password;
        this.instanceName = instanceName;
        this.zookeepers = zookeepers;
    }

    /**
     * Returns a new CB Connection
     */
    public PooledObject<AccumuloClient> makeObject() throws Exception {
        AccumuloClient c = Accumulo.newClient().to(instanceName, zookeepers).as(username, password).build();
        return new DefaultPooledObject<>(c);
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    @Override
    public void destroyObject(PooledObject<AccumuloClient> p) {
        /* no-op */
    }

    @Override
    public boolean validateObject(PooledObject<AccumuloClient> p) {
        if (null == p) {
            final String msg = "Null Connector received in AccumuloConnectionPoolFactory.validateObject";
            log.warn(msg, new NullPointerException(msg));
            return false;
        }

        return true;
    }

    @Override
    public void activateObject(PooledObject<AccumuloClient> p) {
        /* no-op */
    }

    @Override
    public void passivateObject(PooledObject<AccumuloClient> p) {
        /* no-op */
    }

}
