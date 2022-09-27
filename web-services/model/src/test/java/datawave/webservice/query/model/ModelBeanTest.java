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
import org.easymock.Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

@Disabled
@ExtendWith(EasyMockExtension.class)
public class ModelBeanTest {
    
    private static final String userDN = "CN=Guy Some Other soguy, OU=ou1, OU=ou2, OU=ou3, O=o1, C=US";
    private static final String issuerDN = "CN=CA1, OU=ou3, O=o1, C=US";
    private static final String[] auths = new String[] {"PRIVATE", "PUBLIC"};
    
    private ModelBean bean;
    
    @Mock
    private AccumuloConnectionFactory connectionFactory;
    
    @Mock
    EJBContext ctx;
    
    @Mock
    AccumuloTableCache cache;
    
    private InMemoryInstance instance;
    private Connector connector;
    private DatawavePrincipal principal;
    
    private static long TIMESTAMP = System.currentTimeMillis();
    
    private datawave.webservice.model.Model MODEL_ONE = null;
    private datawave.webservice.model.Model MODEL_TWO = null;
    
    @BeforeEach
    public void setup() throws Exception {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        bean = new ModelBean();
        
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
        // PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
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
        EasyMock.replay();
        
        bean.importModel(MODEL_ONE, (String) null);
        EasyMock.verify();
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
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        EasyMock.replay();
        
        bean.importModel(MODEL_ONE, (String) null);
        EasyMock.verify();
        EasyMock.reset();
        
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        EasyMock.replay();
        
        bean.importModel(MODEL_TWO, (String) null);
        
        EasyMock.verify();
    }
    
    @Test
    public void testListModels() throws Exception {
        importModels();
        EasyMock.reset();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.replay();
        
        ModelList list = bean.listModelNames((String) null);
        EasyMock.verify();
        
        Assertions.assertEquals(2, list.getNames().size());
        Assertions.assertTrue(list.getNames().contains(MODEL_ONE.getName()));
        Assertions.assertTrue(list.getNames().contains(MODEL_TWO.getName()));
    }
    
    @Test
    public void testModelGet() throws Exception {
        importModels();
        EasyMock.reset();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.replay();
        
        datawave.webservice.model.Model model = bean.getModel(MODEL_ONE.getName(), (String) null);
        EasyMock.verify();
        
        Assertions.assertEquals(MODEL_ONE, model);
    }
    
    @Test
    public void testModelDelete() throws Exception {
        importModels();
        EasyMock.reset();
        
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
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        
        bean.deleteModel(MODEL_TWO.getName(), (String) null);
        EasyMock.verify();
        EasyMock.reset();
        
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.replay();
        
        DatawaveWebApplicationException e = Assertions.assertThrows(DatawaveWebApplicationException.class,
                        () -> bean.getModel(MODEL_TWO.getName(), (String) null));
        Assertions.assertEquals(404, e.getResponse().getStatus(), "getModel did not return a 404, returned: " + e.getResponse().getStatus());
        Assertions.fail("getModel should have failed");
        
        EasyMock.verify();
        EasyMock.reset();
        // Ensure model one still intact
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.replay();
        
        datawave.webservice.model.Model model1 = bean.getModel(MODEL_ONE.getName(), (String) null);
        EasyMock.verify();
        Assertions.assertEquals(MODEL_ONE, model1);
        
    }
    
    @Test
    public void testModelGetInvalidModelName() throws Exception {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.replay();
        
        bean.getModel(MODEL_ONE.getName(), (String) null);
        EasyMock.verify();
    }
    
    @Test
    public void testCloneModel() throws Exception {
        importModels();
        EasyMock.reset();
        
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
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnConnection(connector);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        
        bean.cloneModel(MODEL_ONE.getName(), "MODEL2", (String) null);
        EasyMock.verify();
        
        EasyMock.reset();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(connector);
        connectionFactory.returnConnection(connector);
        EasyMock.replay();
        
        datawave.webservice.model.Model model = bean.getModel("MODEL2", (String) null);
        EasyMock.verify();
        
        MODEL_ONE.setName("MODEL2");
        Assertions.assertEquals(MODEL_ONE, model);
        
    }
    
    @Test
    public void testCheckModelName() throws Exception {
        Class<ModelBean> clazz = ModelBean.class;
        Method method = clazz.getDeclaredMethod("checkModelTableName", String.class);
        method.setAccessible(true);
        
        String modelTableName = (String) method.invoke(bean, (String) null);
        Assertions.assertEquals(ModelBean.DEFAULT_MODEL_TABLE_NAME, modelTableName);
        modelTableName = "foo";
        String response = (String) method.invoke(bean, modelTableName);
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
