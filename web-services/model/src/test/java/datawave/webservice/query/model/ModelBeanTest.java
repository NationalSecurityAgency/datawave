package datawave.webservice.query.model;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.model.ModelList;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.util.ReflectionTestUtils;

import javax.ejb.EJBContext;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

@ExtendWith(EasyMockExtension.class)
public class ModelBeanTest extends EasyMockSupport {
    
    private static final String userDN = "CN=Guy Some Other soguy, OU=ou1, OU=ou2, OU=ou3, O=o1, C=US";
    private static final String issuerDN = "CN=CA1, OU=ou3, O=o1, C=US";
    private static final String[] auths = new String[] {"PRIVATE", "PUBLIC"};
    
    private ModelBean bean = null;
    private AccumuloConnectionFactory connectionFactory = null;
    private EJBContext ctx;
    private AccumuloTableCache cache;
    
    private InMemoryInstance instance = null;
    private Connector connector = null;
    private DatawavePrincipal principal = null;
    
    private static long TIMESTAMP = System.currentTimeMillis();
    
    private datawave.webservice.model.Model MODEL_ONE = null;
    private datawave.webservice.model.Model MODEL_TWO = null;
    
    @BeforeEach
    public void setup() throws Exception {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        bean = new ModelBean();
        connectionFactory = createStrictMock(AccumuloConnectionFactory.class);
        ctx = createMock(EJBContext.class);
        cache = createMock(AccumuloTableCache.class);
        ReflectionTestUtils.setField(bean, "ctx", ctx);
        ReflectionTestUtils.setField(bean, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(bean, "cache", cache);
        
        instance = new InMemoryInstance("test");
        connector = instance.getConnector("root", new PasswordToken(""));
        
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, issuerDN), UserType.USER, Arrays.asList(auths), null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
        
        URL m1Url = ModelBeanTest.class.getResource("/ModelBeanTest_m1.xml");
        URL m2Url = ModelBeanTest.class.getResource("/ModelBeanTest_m2.xml");
        JAXBContext ctx = JAXBContext.newInstance(datawave.webservice.model.Model.class);
        Unmarshaller u = ctx.createUnmarshaller();
        MODEL_ONE = (datawave.webservice.model.Model) u.unmarshal(m1Url);
        MODEL_TWO = (datawave.webservice.model.Model) u.unmarshal(m2Url);
        
        Logger.getLogger(ModelBean.class).setLevel(Level.OFF);
    }
    
    public void printTable(String tableName) throws Exception {
        Scanner s = connector.createScanner(tableName, new Authorizations(auths));
        for (Entry<Key,Value> entry : s) {
            System.out.println(entry.getKey());
        }
    }
    
    @AfterEach
    public void tearDown() {
        try {
            connector.tableOperations().delete(ModelBean.DEFAULT_MODEL_TABLE_NAME);
        } catch (Exception e) {}
    }
    
    @Test
    public void testModelImportNoTable() throws Exception {
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        Assertions.assertThrows(DatawaveWebApplicationException.class, () -> bean.importModel(MODEL_ONE, (String) null));
        verifyAll();
    }
    
    private void importModels() throws Exception {
        connector.tableOperations().create(ModelBean.DEFAULT_MODEL_TABLE_NAME);
        
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        replayAll();
        
        bean.importModel(MODEL_ONE, (String) null);
        verifyAll();
        resetAll();
        
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        replayAll();
        
        bean.importModel(MODEL_TWO, (String) null);
        
        verifyAll();
    }
    
    @Test
    public void testListModels() throws Exception {
        importModels();
        resetAll();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replayAll();
        
        ModelList list = bean.listModelNames((String) null);
        verifyAll();
        
        Assertions.assertEquals(2, list.getNames().size());
        Assertions.assertTrue(list.getNames().contains(MODEL_ONE.getName()));
        Assertions.assertTrue(list.getNames().contains(MODEL_TWO.getName()));
    }
    
    @Test
    public void testModelGet() throws Exception {
        importModels();
        resetAll();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replayAll();
        
        datawave.webservice.model.Model model = bean.getModel(MODEL_ONE.getName(), (String) null);
        verifyAll();
        
        Assertions.assertEquals(MODEL_ONE, model);
    }
    
    @Test
    public void testModelDelete() throws Exception {
        importModels();
        resetAll();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        replayAll();
        
        bean.deleteModel(MODEL_TWO.getName(), (String) null);
        verifyAll();
        resetAll();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replayAll();
        try {
            bean.getModel(MODEL_TWO.getName(), (String) null);
            Assertions.fail("getModel should have failed");
        } catch (DatawaveWebApplicationException e) {
            if (e.getResponse().getStatus() == 404) {
                // success
            } else {
                Assertions.fail("getModel did not return a 404, returned: " + e.getResponse().getStatus());
            }
        } catch (Exception ex) {
            Assertions.fail("getModel did not throw a DatawaveWebApplicationException");
        }
        verifyAll();
        resetAll();
        // Ensure model one still intact
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replayAll();
        datawave.webservice.model.Model model1 = bean.getModel(MODEL_ONE.getName(), (String) null);
        verifyAll();
        Assertions.assertEquals(MODEL_ONE, model1);
        
    }
    
    @Test
    public void testModelGetInvalidModelName() throws Exception {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replayAll();
        
        Assertions.assertThrows(DatawaveWebApplicationException.class, () -> bean.getModel(MODEL_ONE.getName(), (String) null));
        verifyAll();
    }
    
    @Test
    public void testCloneModel() throws Exception {
        importModels();
        resetAll();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        connectionFactory.returnConnection(connector);
        replayAll();
        
        bean.cloneModel(MODEL_ONE.getName(), "MODEL2", (String) null);
        verifyAll();
        resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replayAll();
        
        datawave.webservice.model.Model model = bean.getModel("MODEL2", (String) null);
        verifyAll();
        
        MODEL_ONE.setName("MODEL2");
        Assertions.assertEquals(MODEL_ONE, model);
        
    }
    
    @Test
    public void testCheckModelName() throws Exception {
        Method m = ModelBean.class.getDeclaredMethod("checkModelTableName", String.class);
        m.setAccessible(true);
        String modelTableName = (String) m.invoke(bean, (String) null);
        Assertions.assertEquals(ModelBean.DEFAULT_MODEL_TABLE_NAME, modelTableName);
        modelTableName = "foo";
        String response = (String) m.invoke(bean, modelTableName);
        Assertions.assertEquals(modelTableName, response);
        
    }
    
    private void dumpModels() throws Exception {
        System.out.println("******************* Start Dump Models **********************");
        Set<Authorizations> cbAuths = new HashSet<>();
        for (Collection<String> auths : principal.getAuthorizations()) {
            cbAuths.add(new Authorizations(auths.toArray(new String[auths.size()])));
        }
        
        Scanner scanner = ScannerHelper.createScanner(connector, ModelBean.DEFAULT_MODEL_TABLE_NAME, cbAuths);
        for (Entry<Key,Value> entry : scanner) {
            System.out.println(entry.getKey());
        }
        
        System.out.println("******************* End Dump Models **********************");
    }
    
}
