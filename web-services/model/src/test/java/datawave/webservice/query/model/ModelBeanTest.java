package datawave.webservice.query.model;

import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.createStrictMock;

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import javax.ejb.EJBContext;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.model.ModelKeyParser;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.model.ModelList;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ModelBean.class, ModelKeyParser.class})
@PowerMockIgnore({"org.apache.*", "com.sun.xml.*", "javax.xml.bind.*", "com.google.*"})
public class ModelBeanTest {

    private static final String userDN = "CN=Guy Some Other soguy, OU=ou1, OU=ou2, OU=ou3, O=o1, C=US";
    private static final String issuerDN = "CN=CA1, OU=ou3, O=o1, C=US";
    private static final String[] auths = new String[] {"PRIVATE", "PUBLIC"};

    private ModelBean bean = null;
    private AccumuloConnectionFactory connectionFactory = null;
    private EJBContext ctx;
    private AccumuloTableCache cache;

    private InMemoryInstance instance = null;
    private AccumuloClient client = null;
    private DatawavePrincipal principal = null;

    private static long TIMESTAMP = System.currentTimeMillis();

    private datawave.webservice.model.Model MODEL_ONE = null;
    private datawave.webservice.model.Model MODEL_TWO = null;

    @Before
    public void setup() throws Exception {
        System.setProperty(DnUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        bean = new ModelBean();
        connectionFactory = createStrictMock(AccumuloConnectionFactory.class);
        ctx = createMock(EJBContext.class);
        cache = createMock(AccumuloTableCache.class);
        Whitebox.setInternalState(bean, EJBContext.class, ctx);
        Whitebox.setInternalState(bean, AccumuloConnectionFactory.class, connectionFactory);
        Whitebox.setInternalState(bean, AccumuloTableCache.class, cache);

        instance = new InMemoryInstance("test");
        client = new InMemoryAccumuloClient("root", instance);

        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, issuerDN), UserType.USER, Arrays.asList(auths), null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));

        URL m1Url = ModelBeanTest.class.getResource("/ModelBeanTest_m1.xml");
        URL m2Url = ModelBeanTest.class.getResource("/ModelBeanTest_m2.xml");

        /*
         * In jdk 9+, java.lang.Package.getPackageInfo() may fail to load a package-info.class, which is needed to resolve our JAXB package-level annotations.
         * Here, this results in:
         *
         * "javax.xml.bind.UnmarshalException: unexpected element (uri:"http://webservice.datawave/v1", local:"Model"). Expected elements are <{}Model>"
         *
         * As a workaround, we can force it to load here in the same manner as in jdk 8's Package.getPackageInfo(), though there's likely a "better" (i.e.,
         * module-focused) way of handling this in jdk 9+
         *
         * E.g., see https://stackoverflow.com/questions/52157040/
         */
        Class.forName("datawave.webservice.model.package-info", false, this.getClass().getClassLoader());

        JAXBContext ctx = JAXBContext.newInstance(datawave.webservice.model.Model.class);
        Unmarshaller u = ctx.createUnmarshaller();

        MODEL_ONE = (datawave.webservice.model.Model) u.unmarshal(m1Url);
        MODEL_TWO = (datawave.webservice.model.Model) u.unmarshal(m2Url);

        Logger.getLogger(ModelBean.class).setLevel(Level.OFF);
        PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
    }

    public void printTable(String tableName) throws Exception {
        Scanner s = client.createScanner(tableName, new Authorizations(auths));
        for (Entry<Key,Value> entry : s) {
            System.out.println(entry.getKey());
        }
    }

    @After
    public void tearDown() {
        try {
            client.tableOperations().delete(ModelBean.DEFAULT_MODEL_TABLE_NAME);
        } catch (Exception e) {}
    }

    @Test(expected = DatawaveWebApplicationException.class)
    public void testModelImportNoTable() throws Exception {
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        PowerMock.replayAll();

        bean.importModel(MODEL_ONE, (String) null);
        PowerMock.verifyAll();
    }

    private void importModels() throws Exception {
        client.tableOperations().create(ModelBean.DEFAULT_MODEL_TABLE_NAME);

        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnClient(client);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        PowerMock.replayAll();

        bean.importModel(MODEL_ONE, (String) null);
        PowerMock.verifyAll();
        PowerMock.resetAll();

        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnClient(client);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        PowerMock.replayAll();

        bean.importModel(MODEL_TWO, (String) null);

        PowerMock.verifyAll();
    }

    @Test
    public void testListModels() throws Exception {
        importModels();
        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        PowerMock.replayAll();

        ModelList list = bean.listModelNames((String) null);
        PowerMock.verifyAll();

        Assert.assertEquals(2, list.getNames().size());
        Assert.assertTrue(list.getNames().contains(MODEL_ONE.getName()));
        Assert.assertTrue(list.getNames().contains(MODEL_TWO.getName()));
    }

    @Test
    public void testModelGet() throws Exception {
        importModels();
        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        PowerMock.replayAll();

        datawave.webservice.model.Model model = bean.getModel(MODEL_ONE.getName(), (String) null);
        PowerMock.verifyAll();

        Assert.assertEquals(MODEL_ONE, model);
    }

    @Test
    public void testModelDelete() throws Exception {
        importModels();
        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnClient(client);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();

        bean.deleteModel(MODEL_TWO.getName(), (String) null);
        PowerMock.verifyAll();
        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        PowerMock.replayAll();
        try {
            bean.getModel(MODEL_TWO.getName(), (String) null);
            Assert.fail("getModel should have failed");
        } catch (DatawaveWebApplicationException e) {
            if (e.getResponse().getStatus() == 404) {
                // success
            } else {
                Assert.fail("getModel did not return a 404, returned: " + e.getResponse().getStatus());
            }
        } catch (Exception ex) {
            Assert.fail("getModel did not throw a DatawaveWebApplicationException");
        }
        PowerMock.verifyAll();
        PowerMock.resetAll();
        // Ensure model one still intact
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        PowerMock.replayAll();
        datawave.webservice.model.Model model1 = bean.getModel(MODEL_ONE.getName(), (String) null);
        PowerMock.verifyAll();
        Assert.assertEquals(MODEL_ONE, model1);

    }

    @Test(expected = DatawaveWebApplicationException.class)
    public void testModelGetInvalidModelName() throws Exception {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        PowerMock.replayAll();

        bean.getModel(MODEL_ONE.getName(), (String) null);
        PowerMock.verifyAll();
    }

    @Test
    public void testCloneModel() throws Exception {
        importModels();
        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        EasyMock.expect(cache.reloadCache(ModelBean.DEFAULT_MODEL_TABLE_NAME)).andReturn(null);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        connectionFactory.returnClient(client);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();

        bean.cloneModel(MODEL_ONE.getName(), "MODEL2", (String) null);
        PowerMock.verifyAll();
        PowerMock.resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        EasyMock.expect(connectionFactory.getClient(EasyMock.eq(AccumuloConnectionFactory.Priority.LOW), EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        PowerMock.replayAll();

        datawave.webservice.model.Model model = bean.getModel("MODEL2", (String) null);
        PowerMock.verifyAll();

        MODEL_ONE.setName("MODEL2");
        Assert.assertEquals(MODEL_ONE, model);

    }

    @Test
    public void testCheckModelName() throws Exception {
        String modelTableName = Whitebox.invokeMethod(bean, "checkModelTableName", (String) null);
        Assert.assertEquals(ModelBean.DEFAULT_MODEL_TABLE_NAME, modelTableName);
        modelTableName = "foo";
        String response = Whitebox.invokeMethod(bean, "checkModelTableName", modelTableName);
        Assert.assertEquals(modelTableName, response);

    }

    private void dumpModels() throws Exception {
        System.out.println("******************* Start Dump Models **********************");
        Set<Authorizations> cbAuths = new HashSet<>();
        for (Collection<String> auths : principal.getAuthorizations()) {
            cbAuths.add(new Authorizations(auths.toArray(new String[auths.size()])));
        }

        Scanner scanner = ScannerHelper.createScanner(client, ModelBean.DEFAULT_MODEL_TABLE_NAME, cbAuths);
        for (Entry<Key,Value> entry : scanner) {
            System.out.println(entry.getKey());
        }

        System.out.println("******************* End Dump Models **********************");
    }

}
