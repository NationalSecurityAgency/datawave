package datawave.webservice.common.connection;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;

public class AccumuloConnectionPoolFactory implements PooledObjectFactory<Connector> {
    
    private static final Logger log = Logger.getLogger(AccumuloConnectionPoolFactory.class);
    
    private String username;
    private String password;
    private Instance instance = null;
    
    public AccumuloConnectionPoolFactory(String username, String password, String zookeepers, String instanceName) {
        this.username = username;
        this.password = password;
        this.instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers));
    }
    
    public AccumuloConnectionPoolFactory(String username, String password, Instance instance) {
        this.username = username;
        this.password = password;
        this.instance = instance;
    }
    
    /**
     * Returns a new CB Connection
     */
    public PooledObject<Connector> makeObject() throws Exception {
        Connector c = instance.getConnector(username, new PasswordToken(password));
        return new DefaultPooledObject<>(c);
    }
    
    String getUsername() {
        return username;
    }
    
    String getPassword() {
        return password;
    }
    
    @Override
    public void destroyObject(PooledObject<Connector> p) throws Exception {
        /* no-op */
    }
    
    @Override
    public boolean validateObject(PooledObject<Connector> p) {
        if (null == p) {
            final String msg = "Null Connector received in AccumuloConnectionPoolFactory.validateObject";
            log.warn(msg, new NullPointerException(msg));
            return false;
        }
        
        return true;
    }
    
    @Override
    public void activateObject(PooledObject<Connector> p) throws Exception {
        /* no-op */
    }
    
    @Override
    public void passivateObject(PooledObject<Connector> p) throws Exception {
        /* no-op */
    }
    
}
