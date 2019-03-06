package datawave.webservice.common.connection;

import static org.easymock.MockType.STRICT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.connection.config.ConnectionPoolConfiguration;
import datawave.webservice.common.connection.config.ConnectionPoolsConfiguration;
import org.apache.accumulo.core.client.Connector;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

@RunWith(EasyMockRunner.class)
public class AccumuloConnectionFactoryTest extends EasyMockSupport {
    
    @TestSubject
    private AccumuloConnectionFactoryBean bean = createMockBuilder(AccumuloConnectionFactoryBean.class).addMockedMethods("getCurrentUserDN",
                    "getCurrentProxyServers").createStrictMock();
    
    @Mock(type = STRICT)
    private AccumuloTableCache cache;
    
    private InMemoryInstance instance = new InMemoryInstance();
    
    @Mock(type = STRICT)
    private WrappedConnector warehouseConnection;
    
    @Mock(type = STRICT)
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
        
        Map<String,ConnectionPoolConfiguration> configs = new HashMap<>();
        configs.put("WAREHOUSE", null);
        configs.put("METRICS", null);
        ConnectionPoolsConfiguration conf = new ConnectionPoolsConfiguration();
        Whitebox.setInternalState(conf, "defaultPool", "WAREHOUSE");
        Whitebox.setInternalState(conf, "poolNames", Lists.newArrayList("WAREHOUSE", "METRICS"));
        Whitebox.setInternalState(conf, "pools", configs);
        
        String defaultPoolName = conf.getDefaultPool();
        HashMap<String,Map<Priority,AccumuloConnectionPool>> pools = new HashMap<>();
        MyAccumuloConnectionPool warehousePool = new MyAccumuloConnectionPool(warehouseFactory);
        MyAccumuloConnectionPool metricsPool = new MyAccumuloConnectionPool(metricsFactory);
        for (Entry<String,ConnectionPoolConfiguration> entry : conf.getPools().entrySet()) {
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
            Map<Priority,AccumuloConnectionPool> p = new HashMap<>();
            p.put(Priority.ADMIN, acp);
            p.put(Priority.HIGH, acp);
            p.put(Priority.NORMAL, acp);
            p.put(Priority.LOW, acp);
            pools.put(entry.getKey(), Collections.unmodifiableMap(p));
        }
        Whitebox.setInternalState(bean, ConnectionPoolsConfiguration.class, conf);
        Whitebox.setInternalState(bean, "defaultPoolName", defaultPoolName);
        Whitebox.setInternalState(bean, "pools", pools);
    }
    
    @After
    public void cleanup() throws Exception {
        System.clearProperty("dw.accumulo.classLoader.context");
    }
    
    @Test
    public void testGetConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(instance);
        EasyMock.expect(bean.getCurrentUserDN()).andReturn(null);
        EasyMock.expect(bean.getCurrentProxyServers()).andReturn(null);
        replayAll();
        Connector con = bean.getConnection(Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseConnection, ((WrappedConnector) con).getReal());
        assertNull("scannerClassLoaderContext was set when it shouldn't have been", Whitebox.getInternalState(con, "scannerClassLoaderContext"));
    }
    
    @Test
    public void testGetWarehouseConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        EasyMock.expect(bean.getCurrentUserDN()).andReturn(null);
        EasyMock.expect(bean.getCurrentProxyServers()).andReturn(null);
        replayAll();
        Connector con = bean.getConnection("WAREHOUSE", Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseConnection, ((WrappedConnector) con).getReal());
    }
    
    @Test
    public void testGetContextConnection() throws Exception {
        System.setProperty("dw.accumulo.classLoader.context", "alternateContext");
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        EasyMock.expect(bean.getCurrentUserDN()).andReturn(null);
        EasyMock.expect(bean.getCurrentProxyServers()).andReturn(null);
        replayAll();
        Connector con = bean.getConnection("WAREHOUSE", Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseConnection, ((WrappedConnector) con).getReal());
        assertEquals("alternateContext", Whitebox.getInternalState(con, "scannerClassLoaderContext"));
    }
    
    @Test
    public void testGetMetricsConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        EasyMock.expect(bean.getCurrentUserDN()).andReturn(null);
        EasyMock.expect(bean.getCurrentProxyServers()).andReturn(null);
        replayAll();
        Connector con = bean.getConnection("METRICS", Priority.HIGH, new HashMap<>());
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
        
        private AccumuloConnectionPoolFactory factory;
        
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
