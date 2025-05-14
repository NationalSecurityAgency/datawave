package datawave.core.common.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.MockType;
import org.easymock.TestSubject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.webservice.common.connection.WrappedAccumuloClient;

@RunWith(EasyMockRunner.class)
public class AccumuloConnectionFactoryTest extends EasyMockSupport {

    @Mock(type = MockType.STRICT)
    private AccumuloTableCache cache;

    private InMemoryInstance instance = new InMemoryInstance();

    @TestSubject
    private AccumuloConnectionFactoryImpl factory = createMockBuilder(AccumuloConnectionFactoryImpl.class).createStrictMock();

    @Mock(type = MockType.STRICT)
    private WrappedAccumuloClient warehouseClient;

    @Mock(type = MockType.STRICT)
    private WrappedAccumuloClient metricsClient;

    @Before
    public void setup() throws Exception {

        MyAccumuloClientPoolFactory warehouseFactory = Whitebox.newInstance(MyAccumuloClientPoolFactory.class);
        Whitebox.setInternalState(warehouseFactory, "username", "root");
        Whitebox.setInternalState(warehouseFactory, "password", "");
        MyAccumuloClientPoolFactory metricsFactory = Whitebox.newInstance(MyAccumuloClientPoolFactory.class);
        Whitebox.setInternalState(metricsFactory, "username", "root");
        Whitebox.setInternalState(metricsFactory, "password", "");
        warehouseFactory.setClient(warehouseClient);
        metricsFactory.setClient(metricsClient);

        Map<String,ConnectionPoolProperties> configs = new HashMap<>();
        configs.put("WAREHOUSE", null);
        configs.put("METRICS", null);
        ConnectionPoolsProperties conf = new ConnectionPoolsProperties();
        Whitebox.setInternalState(conf, "defaultPool", "WAREHOUSE");
        Whitebox.setInternalState(conf, "pools", configs);

        HashMap<String,Map<AccumuloConnectionFactory.Priority,AccumuloClientPool>> pools = new HashMap<>();
        MyAccumuloClientPool warehousePool = new MyAccumuloClientPool(warehouseFactory);
        MyAccumuloClientPool metricsPool = new MyAccumuloClientPool(metricsFactory);
        for (Entry<String,ConnectionPoolProperties> entry : conf.getPools().entrySet()) {
            AccumuloClientPool acp = null;
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
            Map<AccumuloConnectionFactory.Priority,AccumuloClientPool> p = new HashMap<>();
            p.put(AccumuloConnectionFactory.Priority.ADMIN, acp);
            p.put(AccumuloConnectionFactory.Priority.HIGH, acp);
            p.put(AccumuloConnectionFactory.Priority.NORMAL, acp);
            p.put(AccumuloConnectionFactory.Priority.LOW, acp);
            pools.put(entry.getKey(), Collections.unmodifiableMap(p));
        }
        Whitebox.setInternalState(factory, "log", LoggerFactory.getLogger(AccumuloConnectionFactoryImpl.class));
        Whitebox.setInternalState(factory, ConnectionPoolsProperties.class, conf);
        Whitebox.setInternalState(factory, "defaultPoolName", conf.getDefaultPool());
        Whitebox.setInternalState(factory, "pools", pools);
        Whitebox.setInternalState(factory, "cache", cache);
    }

    @After
    public void cleanup() {
        System.clearProperty("dw.accumulo.classLoader.context");
    }

    @Test
    public void testGetConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(instance);
        replayAll();
        AccumuloClient con = factory.getClient(null, null, AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseClient, ((WrappedAccumuloClient) con).getReal());
        assertNull("scannerClassLoaderContext was set when it shouldn't have been", Whitebox.getInternalState(con, "scannerClassLoaderContext"));
    }

    @Test
    public void testGetWarehouseConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        replayAll();
        AccumuloClient con = factory.getClient(null, null, "WAREHOUSE", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseClient, ((WrappedAccumuloClient) con).getReal());
    }

    @Test
    public void testGetContextConnection() throws Exception {
        System.setProperty("dw.accumulo.classLoader.context", "alternateContext");
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        replayAll();
        AccumuloClient con = factory.getClient(null, null, "WAREHOUSE", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(warehouseClient, ((WrappedAccumuloClient) con).getReal());
        assertEquals("alternateContext", Whitebox.getInternalState(con, "scannerClassLoaderContext"));
    }

    @Test
    public void testGetMetricsConnection() throws Exception {
        resetAll();
        EasyMock.expect(cache.getInstance()).andReturn(new InMemoryInstance());
        replayAll();
        AccumuloClient con = factory.getClient(null, null, "METRICS", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verifyAll();
        assertNotNull(con);
        assertEquals(metricsClient, ((WrappedAccumuloClient) con).getReal());
    }

    public static class MyAccumuloClientPoolFactory extends AccumuloClientPoolFactory {

        private AccumuloClient c = null;

        public MyAccumuloClientPoolFactory(String username, String password, String zookeepers, String instanceName) {
            super(username, password, zookeepers, instanceName);
        }

        public void setClient(AccumuloClient c) {
            this.c = c;
        }

        @Override
        public PooledObject<AccumuloClient> makeObject() {
            return new DefaultPooledObject<>(c);
        }

        @Override
        public boolean validateObject(PooledObject<AccumuloClient> arg0) {
            return true;
        }

    }

    public static class MyAccumuloClientPool extends AccumuloClientPool {

        private AccumuloClientPoolFactory factory;

        public MyAccumuloClientPool(AccumuloClientPoolFactory factory) {
            super(factory);
            this.factory = factory;
        }

        @Override
        public AccumuloClient borrowObject() throws Exception {
            return this.factory.makeObject().getObject();
        }

        @Override
        public void returnObject(AccumuloClient connector) {}
    }
}
