package datawave.core.common.connection;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.webservice.common.connection.WrappedConnector;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.MockType;
import org.easymock.TestSubject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class AccumuloConnectionFactoryTest extends EasyMockSupport {
    
    @Mock(type = MockType.STRICT)
    private AccumuloTableCache cache;
    
    private InMemoryInstance instance = new InMemoryInstance();
    
    @TestSubject
    private AccumuloConnectionFactoryImpl factory = createMockBuilder(AccumuloConnectionFactoryImpl.class).createStrictMock();
    
    @Mock(type = MockType.STRICT)
    private WrappedConnector warehouseConnection;
    
    @Mock(type = MockType.STRICT)
    private WrappedConnector metricsConnection;
    
    @Before
    public void setup() throws Exception {
        
        MyAccumuloConnectionPoolFactory warehouseFactory = Whitebox.newInstance(MyAccumuloConnectionPoolFactory.class);
        Whitebox.setInternalState(warehouseFactory, "username", "root");
        Whitebox.setInternalState(warehouseFactory, "password", "");
        MyAccumuloConnectionPoolFactory metricsFactory = Whitebox.newInstance(MyAccumuloConnectionPoolFactory.class);
        Whitebox.setInternalState(metricsFactory, "username", "root");
        Whitebox.setInternalState(metricsFactory, "password", "");
        warehouseFactory.setConnector(warehouseConnection);
        metricsFactory.setConnector(metricsConnection);
        
        Map<String,ConnectionPoolProperties> configs = new HashMap<>();
        configs.put("WAREHOUSE", null);
        configs.put("METRICS", null);
        ConnectionPoolsProperties conf = new ConnectionPoolsProperties();
        Whitebox.setInternalState(conf, "defaultPool", "WAREHOUSE");
        Whitebox.setInternalState(conf, "pools", configs);
        
        HashMap<String,Map<AccumuloConnectionFactory.Priority,AccumuloConnectionPool>> pools = new HashMap<>();
        MyAccumuloConnectionPool warehousePool = new MyAccumuloConnectionPool(warehouseFactory);
        MyAccumuloConnectionPool metricsPool = new MyAccumuloConnectionPool(metricsFactory);
        for (Map.Entry<String,ConnectionPoolProperties> entry : conf.getPools().entrySet()) {
            AccumuloConnectionPool acp = null;
            switch (entry.getKey()) {
                case "METRICS":
                    acp = metricsPool;
                    break;
                case "WAREHOUSE":
                    acp = warehousePool;
                    break;
                default:
                    fail("Unknown pool name " + entry.getKey());
            }
            Map<AccumuloConnectionFactory.Priority,AccumuloConnectionPool> p = new HashMap<>();
            p.put(AccumuloConnectionFactory.Priority.ADMIN, acp);
            p.put(AccumuloConnectionFactory.Priority.HIGH, acp);
            p.put(AccumuloConnectionFactory.Priority.NORMAL, acp);
            p.put(AccumuloConnectionFactory.Priority.LOW, acp);
            pools.put(entry.getKey(), Collections.unmodifiableMap(p));
        }
        Whitebox.setInternalState(factory, "log", Logger.getLogger(AccumuloConnectionFactoryImpl.class));
        Whitebox.setInternalState(factory, "defaultPoolName", conf.getDefaultPool());
        Whitebox.setInternalState(factory, "pools", pools);
        Whitebox.setInternalState(factory, "cache", cache);
    }
    
    @After
    public void cleanup() throws Exception {
        System.clearProperty("dw.accumulo.classLoader.context");
    }
    
    @Test
    public void testGetConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(instance);
        replayAll();
        Connector con = factory.getConnection(null, null, AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseConnection, ((WrappedConnector) con).getReal());
        assertNull("scannerClassLoaderContext was set when it shouldn't have been", Whitebox.getInternalState(con, "scannerClassLoaderContext"));
    }
    
    @Test
    public void testGetWarehouseConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        replayAll();
        Connector con = factory.getConnection(null, null, "WAREHOUSE", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseConnection, ((WrappedConnector) con).getReal());
    }
    
    @Test
    public void testGetContextConnection() throws Exception {
        System.setProperty("dw.accumulo.classLoader.context", "alternateContext");
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        replayAll();
        Connector con = factory.getConnection(null, null, "WAREHOUSE", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseConnection, ((WrappedConnector) con).getReal());
        Assert.assertEquals("alternateContext", Whitebox.getInternalState(con, "scannerClassLoaderContext"));
    }
    
    @Test
    public void testGetMetricsConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        replayAll();
        Connector con = factory.getConnection(null, null, "METRICS", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(metricsConnection, ((WrappedConnector) con).getReal());
    }
    
    public static class MyAccumuloConnectionPoolFactory extends AccumuloConnectionPoolFactory {
        
        private Connector c = null;
        
        public MyAccumuloConnectionPoolFactory(String username, String password, String zookeepers, String instanceName) {
            super(username, password, zookeepers, instanceName);
        }
        
        public void setConnector(Connector c) {
            this.c = c;
        }
        
        @Override
        public PooledObject<Connector> makeObject() throws Exception {
            return new DefaultPooledObject<>(c);
        }
        
        @Override
        public boolean validateObject(PooledObject<Connector> arg0) {
            return true;
        }
        
    }
    
    public static class MyAccumuloConnectionPool extends AccumuloConnectionPool {
        
        private AccumuloConnectionPoolFactory factory = null;
        
        public MyAccumuloConnectionPool(AccumuloConnectionPoolFactory factory) {
            super(factory);
            this.factory = factory;
        }
        
        @Override
        public Connector borrowObject() throws Exception {
            return this.factory.makeObject().getObject();
        }
        
        @Override
        public void returnObject(Connector connector) {}
    }
}
