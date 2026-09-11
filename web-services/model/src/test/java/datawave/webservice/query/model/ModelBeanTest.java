package datawave.webservice.query.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.model.ModelList;

@ExtendWith(MockitoExtension.class)
public class ModelBeanTest {

    private static final String userDN = "CN=Guy Some Other soguy, OU=ou1, OU=ou2, OU=ou3, O=o1, C=US";
    private static final String issuerDN = "CN=CA1, OU=ou3, O=o1, C=US";
    private static final String[] auths = new String[] {"PRIVATE", "PUBLIC"};

    private ModelBean bean = null;
    private AccumuloConnectionFactory connectionFactory = null;
    private EJBContext ctx;
    private AccumuloTableCache cache;

    private AccumuloClient client = null;
    private DatawavePrincipal principal = null;

    private datawave.webservice.model.Model MODEL_ONE = null;
    private datawave.webservice.model.Model MODEL_TWO = null;

    @BeforeEach
    public void beforeEach() throws Exception {
        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        bean = new ModelBean();
        connectionFactory = mock(AccumuloConnectionFactory.class);
        ctx = mock(EJBContext.class);
        cache = mock(AccumuloTableCache.class);

        // descriptive variable names are important...
        ReflectionTestUtils.setField(bean, "ctx", ctx);
        ReflectionTestUtils.setField(bean, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(bean, "cache", cache);

        InMemoryInstance instance = new InMemoryInstance("test");
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
    }

    public void printTable(String tableName) throws Exception {
        Scanner s = client.createScanner(tableName, new Authorizations(auths));
        for (Entry<Key,Value> entry : s) {
            System.out.println(entry.getKey());
        }
    }

    @AfterEach
    public void afterEach() {
        try {
            client.tableOperations().delete(ModelBean.DEFAULT_MODEL_TABLE_NAME);
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModelImportNoTable() throws Exception {
        HashMap<String,String> trackingMap = new HashMap<>();
        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String,String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture()))
                        .thenReturn(client);

        connectionFactory.returnClient(client);
        when(ctx.getCallerPrincipal()).thenReturn(principal);

        assertThrows(DatawaveWebApplicationException.class, () -> bean.importModel(MODEL_ONE, null));
    }

    @SuppressWarnings("unchecked")
    private void importModels() throws Exception {

        client.tableOperations().create(ModelBean.DEFAULT_MODEL_TABLE_NAME);

        HashMap<String,String> trackingMap = new HashMap<>();

        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String,String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture()))
                        .thenReturn(client);

        connectionFactory.returnClient(client);
        when(ctx.getCallerPrincipal()).thenReturn(principal);

        connectionFactory.returnClient(client);

        cache.reloadTableCache(ModelBean.DEFAULT_MODEL_TABLE_NAME);

        bean.importModel(MODEL_ONE, ModelBean.DEFAULT_MODEL_TABLE_NAME);

        connectionFactory.returnClient(client);

        when(ctx.getCallerPrincipal()).thenReturn(principal);

        connectionFactory.returnClient(client);

        cache.reloadTableCache(ModelBean.DEFAULT_MODEL_TABLE_NAME);

        bean.importModel(MODEL_TWO, ModelBean.DEFAULT_MODEL_TABLE_NAME);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListModels() throws Exception {
        importModels();

        when(ctx.getCallerPrincipal()).thenReturn(principal);

        HashMap<String,String> trackingMap = new HashMap<>();
        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String,String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture()))
                        .thenReturn(client);

        connectionFactory.returnClient(client);

        ModelList list = bean.listModelNames(null);

        assertEquals(2, list.getNames().size());
        assertTrue(list.getNames().contains(MODEL_ONE.getName()));
        assertTrue(list.getNames().contains(MODEL_TWO.getName()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModelGet() throws Exception {
        importModels();

        when(ctx.getCallerPrincipal()).thenReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String,String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture()))
                        .thenReturn(client);

        connectionFactory.returnClient(client);

        datawave.webservice.model.Model model = bean.getModel(MODEL_ONE.getName(), null);

        assertEquals(MODEL_ONE, model);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModelDelete() throws Exception {
        importModels();

        when(ctx.getCallerPrincipal()).thenReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String,String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture()))
                        .thenReturn(client);

        connectionFactory.returnClient(client);

        cache.reloadTableCache(ModelBean.DEFAULT_MODEL_TABLE_NAME);

        bean.deleteModel(MODEL_TWO.getName(), null);

        when(ctx.getCallerPrincipal()).thenReturn(principal);
        connectionFactory.returnClient(client);

        try {
            bean.getModel(MODEL_TWO.getName(), null);
            fail("getModel should have failed");
        } catch (DatawaveWebApplicationException e) {
            if (e.getResponse().getStatus() != 404) {
                fail("getModel did not return a 404, returned: " + e.getResponse().getStatus());
            }
        } catch (Exception ex) {
            fail("getModel did not throw a DatawaveWebApplicationException");
        }

        // Ensure model one still intact
        when(ctx.getCallerPrincipal()).thenReturn(principal);
        connectionFactory.returnClient(client);

        datawave.webservice.model.Model model1 = bean.getModel(MODEL_ONE.getName(), null);
        assertEquals(MODEL_ONE, model1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testModelGetInvalidModelName() throws Exception {
        when(ctx.getCallerPrincipal()).thenReturn(principal);

        HashMap<String,String> trackingMap = new HashMap<>();
        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String, String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture())).thenReturn(client);

        connectionFactory.returnClient(client);

        assertThrows(DatawaveWebApplicationException.class, () -> bean.getModel(MODEL_ONE.getName(), null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCloneModel() throws Exception {
        importModels();

        when(ctx.getCallerPrincipal()).thenReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        ArgumentCaptor<StackTraceElement[]> stackTraceCaptor = ArgumentCaptor.forClass(StackTraceElement[].class);
        when(connectionFactory.getTrackingMap(stackTraceCaptor.capture())).thenReturn(trackingMap);

        ArgumentCaptor<String> userDNCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection<String>> proxyCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<AccumuloConnectionFactory.Priority> connectionPriorityCaptor = ArgumentCaptor.forClass(AccumuloConnectionFactory.Priority.class);
        ArgumentCaptor<Map<String,String>> trackingMapCaptor = ArgumentCaptor.forClass(Map.class);
        when(connectionFactory.getClient(userDNCaptor.capture(), proxyCaptor.capture(), connectionPriorityCaptor.capture(), trackingMapCaptor.capture()))
                        .thenReturn(client);

        connectionFactory.returnClient(client);

        cache.reloadTableCache(ModelBean.DEFAULT_MODEL_TABLE_NAME);

        connectionFactory.returnClient(client);

        bean.cloneModel(MODEL_ONE.getName(), "MODEL2", null);

        connectionFactory.returnClient(client);

        datawave.webservice.model.Model model = bean.getModel("MODEL2", null);

        MODEL_ONE.setName("MODEL2");
        assertEquals(MODEL_ONE, model);
    }

    @Test
    public void testCheckModelName() {
        String modelTableName = ReflectionTestUtils.invokeMethod(bean, "checkModelTableName", (String) null);
        assertEquals(ModelBean.DEFAULT_MODEL_TABLE_NAME, modelTableName);

        String response = ReflectionTestUtils.invokeMethod(bean, "checkModelTableName", "foo");
        assertEquals("foo", response);

    }

    private void dumpModels() throws Exception {
        System.out.println("******************* Start Dump Models **********************");
        Set<Authorizations> cbAuths = new HashSet<>();
        for (Collection<String> auths : principal.getAuthorizations()) {
            cbAuths.add(new Authorizations(auths.toArray(new String[0])));
        }

        Scanner scanner = ScannerHelper.createScanner(client, ModelBean.DEFAULT_MODEL_TABLE_NAME, cbAuths);
        for (Entry<Key,Value> entry : scanner) {
            System.out.println(entry.getKey());
        }

        System.out.println("******************* End Dump Models **********************");
    }

}
