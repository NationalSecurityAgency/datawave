package datawave.core.common.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.webservice.common.connection.WrappedAccumuloClient;

@ExtendWith(MockitoExtension.class)
public class AccumuloConnectionFactoryTest {

    @Mock
    private AccumuloTableCache cache;

    private InMemoryInstance instance = new InMemoryInstance();

    private AccumuloConnectionFactoryImpl factory = mock(AccumuloConnectionFactoryImpl.class, CALLS_REAL_METHODS);

    @Mock
    private WrappedAccumuloClient warehouseClient;

    @Mock
    private WrappedAccumuloClient metricsClient;

    @BeforeEach
    public void setup() throws Exception {

        MyAccumuloClientPoolFactory warehouseFactory = newInstance(MyAccumuloClientPoolFactory.class);
        setField(warehouseFactory, "username", "root");
        setField(warehouseFactory, "password", "");
        MyAccumuloClientPoolFactory metricsFactory = newInstance(MyAccumuloClientPoolFactory.class);
        setField(metricsFactory, "username", "root");
        setField(metricsFactory, "password", "");
        warehouseFactory.setClient(warehouseClient);
        metricsFactory.setClient(metricsClient);

        Map<String,ConnectionPoolProperties> configs = new HashMap<>();
        configs.put("WAREHOUSE", null);
        configs.put("METRICS", null);
        ConnectionPoolsProperties conf = new ConnectionPoolsProperties();
        setField(conf, "defaultPool", "WAREHOUSE");
        setField(conf, "pools", configs);

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
        setField(factory, "log", LoggerFactory.getLogger(AccumuloConnectionFactoryImpl.class));
        setField(factory, "connectionPoolsConfiguration", conf);
        setField(factory, "defaultPoolName", conf.getDefaultPool());
        setField(factory, "pools", pools);
        setField(factory, "cache", cache);
    }

    @AfterEach
    public void cleanup() {
        System.clearProperty("dw.accumulo.classLoader.context");
    }

    @Test
    public void testGetConnection() throws Exception {
        doReturn(instance).when(cache).getInstance();
        AccumuloClient con = factory.getClient(null, null, AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verify(cache, times(1)).getInstance();
        assertNotNull(con);
        assertEquals(warehouseClient, ((WrappedAccumuloClient) con).getReal());
        assertNull(getField(con, "scannerClassLoaderContext"), "scannerClassLoaderContext was set when it shouldn't have been");

        if (cache != null && mockingDetails(cache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(cache));
        }
        if (metricsClient != null && mockingDetails(metricsClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metricsClient));
        }
        if (warehouseClient != null && mockingDetails(warehouseClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(warehouseClient));
        }
    }

    @Test
    public void testGetWarehouseConnection() throws Exception {
        doReturn(new InMemoryInstance()).when(cache).getInstance();
        AccumuloClient con = factory.getClient(null, null, "WAREHOUSE", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verify(cache, times(1)).getInstance();
        assertNotNull(con);
        assertEquals(warehouseClient, ((WrappedAccumuloClient) con).getReal());

        if (cache != null && mockingDetails(cache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(cache));
        }
        if (metricsClient != null && mockingDetails(metricsClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metricsClient));
        }
        if (warehouseClient != null && mockingDetails(warehouseClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(warehouseClient));
        }
    }

    @Test
    public void testGetContextConnection() throws Exception {
        System.setProperty("dw.accumulo.classLoader.context", "alternateContext");
        doReturn(new InMemoryInstance()).when(cache).getInstance();
        AccumuloClient con = factory.getClient(null, null, "WAREHOUSE", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verify(cache, times(1)).getInstance();
        assertNotNull(con);
        assertEquals(warehouseClient, ((WrappedAccumuloClient) con).getReal());
        assertEquals("alternateContext", getField(con, "scannerClassLoaderContext"));

        if (cache != null && mockingDetails(cache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(cache));
        }
        if (metricsClient != null && mockingDetails(metricsClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metricsClient));
        }
        if (warehouseClient != null && mockingDetails(warehouseClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(warehouseClient));
        }
    }

    @Test
    public void testGetMetricsConnection() throws Exception {
        doReturn(new InMemoryInstance()).when(cache).getInstance();
        AccumuloClient con = factory.getClient(null, null, "METRICS", AccumuloConnectionFactory.Priority.HIGH, new HashMap<>());
        verify(cache, times(1)).getInstance();
        assertNotNull(con);
        assertEquals(metricsClient, ((WrappedAccumuloClient) con).getReal());

        if (cache != null && mockingDetails(cache).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(cache));
        }
        if (metricsClient != null && mockingDetails(metricsClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(metricsClient));
        }
        if (warehouseClient != null && mockingDetails(warehouseClient).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(warehouseClient));
        }
    }

    public static class MyAccumuloClientPoolFactory extends AccumuloClientPoolFactory {

        private AccumuloClient c = null;

        // No-arg constructor for reflection-based instantiation in tests
        private MyAccumuloClientPoolFactory() {
            super(null, null, null, null);
        }

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

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    @SuppressWarnings("unchecked")
    private <T> T newInstance(Class<T> clazz) throws Exception {
        Constructor<T> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }
}
