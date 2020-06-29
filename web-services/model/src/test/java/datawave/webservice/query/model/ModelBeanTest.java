package datawave.webservice.query.model;

import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.PreConditionFailedException;
import datawave.webservice.model.FieldMapping;
import datawave.webservice.model.Model;
import datawave.webservice.model.ModelList;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.ejb.EJBContext;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ModelBean.class, ModelKeyParser.class})
@PowerMockIgnore({"org.apache.*", "com.sun.xml.*", "javax.xml.bind.*", "com.google.*"})
public class ModelBeanTest {
    
    private static final String USER_DN = "CN=Guy Some Other Guy, OU=ou1, OU=ou2, OU=ou3, O=o1, C=US";
    private static final String ISSUER_DN = "CN=CA1, OU=ou3, O=o1, C=US";
    private static final String[] AUTH_ARRAY = new String[] {"PRIVATE", "PUBLIC"};
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations(AUTH_ARRAY));
    private static final String DEFAULT_TABLE_NAME = "models";
    
    @MockStrict
    private AccumuloConnectionFactory connectionFactory;
    
    @Mock
    private AccumuloTableCache tableCache;
    
    @Mock
    private EJBContext context;
    
    private Model modelOne;
    private Model modelTwo;
    private Model modelThree;
    private List<FieldMapping> modelThreeMappings;
    
    private ModelBean modelBean;
    private Connector connector;
    private DatawavePrincipal principal;
    
    @Before
    public void setUp() throws Exception {
        initModelBean();
        initConnector();
        initPrincipal();
        initModels();
    }
    
    @After
    public void tearDown() throws Exception {
        if (connector != null && connector.tableOperations().exists(DEFAULT_TABLE_NAME)) {
            connector.tableOperations().delete(DEFAULT_TABLE_NAME);
        }
    }
    
    private void initModelBean() {
        System.setProperty(DnUtils.NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        
        modelBean = new ModelBean();
        Whitebox.setInternalState(modelBean, AccumuloConnectionFactory.class, connectionFactory);
        Whitebox.setInternalState(modelBean, AccumuloTableCache.class, tableCache);
        Whitebox.setInternalState(modelBean, EJBContext.class, context);
        Whitebox.setInternalState(modelBean, "defaultModelTableName", DEFAULT_TABLE_NAME);
    }
    
    private void initConnector() throws AccumuloSecurityException, AccumuloException {
        connector = new InMemoryInstance("test").getConnector("root", new PasswordToken(""));
    }
    
    private void initPrincipal() {
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(USER_DN, ISSUER_DN), DatawaveUser.UserType.USER, Arrays.asList(AUTH_ARRAY), null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
    }
    
    private void initModels() throws JAXBException {
        URL modelOneUrl = ModelBeanTest.class.getResource("/ModelBeanTest_model1.xml");
        URL modelTwoUrl = ModelBeanTest.class.getResource("/ModelBeanTest_model2.xml");
        URL modelThreeUrl = ModelBeanTest.class.getResource("/ModelBeanTest_model3.xml");
        
        JAXBContext context = JAXBContext.newInstance(Model.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        
        modelOne = (Model) unmarshaller.unmarshal(modelOneUrl);
        modelTwo = (Model) unmarshaller.unmarshal(modelTwoUrl);
        modelThree = (Model) unmarshaller.unmarshal(modelThreeUrl);
        modelThreeMappings = new ArrayList<>(modelThree.getFields());
    }
    
    @Test
    public void listModelNames_givenNonExistentTable_throwException() throws Exception { // Prepare test.
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.listModelNames("nonExistentTable"));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not list model names.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void listModelNames_givenBlankTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.listModelNames(" "));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not list model names.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void listModelNames_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.listModelNames(null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not list model names.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void listModelNames_givenValidTable_returnNames() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        ModelList modelList = modelBean.listModelNames(DEFAULT_TABLE_NAME);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify result.
        assertThat(modelList.getNames()).containsExactly(modelOne.getName(), modelTwo.getName(), modelThree.getName());
    }
    
    @Test
    public void importModel_givenNonExistantTable_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.importModel(modelOne, "nonExistentTable"));
        
        // Verify expected calls.
        verifyAll();
        
        // Assert result.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not list model names.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void importModel_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.importModel(modelOne, " "));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not list model names.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void importModel_givenModelWithNameAlreadyExists_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.importModel(modelOne, DEFAULT_TABLE_NAME));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception.
        assertException(exception, PreConditionFailedException.class, "HTTP 412 Precondition Failed");
    }
    
    @Test
    public void importModel_givenModelThatDoesNotAlreadyExist_importModel() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectCacheToBeReloaded();
        createDefaultTable();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        modelBean.importModel(modelOne, DEFAULT_TABLE_NAME);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the mappings were written.
        Scanner scanner = ScannerHelper.createScanner(connector, DEFAULT_TABLE_NAME, AUTHS);
        Set<FieldMapping> fields = new HashSet<>();
        scanner.forEach(entry -> fields.add(ModelKeyParser.parseKey(entry.getKey())));
        assertThat(fields).isEqualTo(modelOne.getFields());
    }
    
    @Test
    public void deleteModel_givenNonExistentTable_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteModel("model", "nonExistentTable"));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void deleteModel_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteModel("model", null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void deleteModel_givenBlankTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteModel("model", " "));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void deleteModel_givenNonExistentModel_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteModel("nonExistentModel", DEFAULT_TABLE_NAME));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 404 Not Found");
    }
    
    @Test
    public void deleteModel_givenValidModelName_deleteModelMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectCacheToBeReloaded();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        modelBean.deleteModel(modelOne.getName(), DEFAULT_TABLE_NAME);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify that no field mappings from modelOne remain.
        Scanner scanner = ScannerHelper.createScanner(connector, DEFAULT_TABLE_NAME, AUTHS);
        Set<FieldMapping> fields = new HashSet<>();
        scanner.forEach(entry -> fields.add(ModelKeyParser.parseKey(entry.getKey())));
        
        assertThat(fields).containsAll(modelTwo.getFields());
        assertThat(fields).containsAll(modelThree.getFields());
    }
    
    @Test
    public void cloneModel_givenNonExistentTable_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.cloneModel("model", "new_model", "nonExistentTable"));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void cloneModel_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.cloneModel("model", "new_model", null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
    }
    
    @Test
    public void cloneModel_givenBlankTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.cloneModel("model", "new_model", null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void cloneModel_givenNonExistentModel_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.cloneModel("nonExistentModel", "new_model", null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 404 Not Found");
    }
    
    @Test
    public void cloneModel_givenValidModel_cloneModel() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectCacheToBeReloaded();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        modelBean.cloneModel(modelOne.getName(), "newModel", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify that the new_model mappings match that of modelOne.
        Scanner scanner = ScannerHelper.createScanner(connector, DEFAULT_TABLE_NAME, AUTHS);
        IteratorSetting regexFilter = new IteratorSetting(21, "colfRegex", RegExFilter.class.getName());
        regexFilter.addOption(RegExFilter.COLF_REGEX, "^newModel(\\x00.*)?");
        scanner.addScanIterator(regexFilter);
        Set<FieldMapping> fields = new HashSet<>();
        scanner.forEach(entry -> fields.add(ModelKeyParser.parseKey(entry.getKey())));
        assertThat(fields).isEqualTo(modelOne.getFields());
    }
    
    @Test
    public void getModel_givenNonExistentTable_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        Exception exception = captureException(() -> modelBean.getModel("model", "nonExistentTable", -1, 0, null, null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void getModel_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.getModel("model", null, -1, 0, null, null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
    }
    
    @Test
    public void getModel_givenBlankTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.getModel("model", " ", -1, 0, null, null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not get model.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void getModel_givenNonExistentModel_throwException() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.getModel("nonExistentModel", DEFAULT_TABLE_NAME, -1, 0, null, null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception .
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 404 Not Found");
    }
    
    @Test
    public void getModel_givenExistentModel_returnModel() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelOne.getName(), DEFAULT_TABLE_NAME, -1, 0, null, null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify model.
        assertThat(model).isEqualTo(modelOne);
    }
    
    @Test
    public void getModel_givenLimit_returnMappingSubset() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, 3, 0, null, null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 1, 2);
    }
    
    @Test
    public void getModel_givenOffset_returnMappingSubset() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 3, null, null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 3, 4, 5);
    }
    
    @Test
    public void getModel_givenLimitAndOffset_returnMappingSubset() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, 3, 2, null, null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 2, 3, 4);
    }
    
    @Test
    public void getModel_givenAscendingSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 1, 2, 3, 4, 5);
    }
    
    @Test
    public void getModel_givenAscendingSortWithColon_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc:", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 1, 2, 3, 4, 5);
    }
    
    @Test
    public void getModel_givenDescendingSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 5, 4, 3, 2, 1, 0);
    }
    
    @Test
    public void getModel_givenDescendingSortWithColon_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc:", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 5, 4, 3, 2, 1, 0);
    }
    
    @Test
    public void getModel_givenAscendingVisibilitySort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc:Visibility", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 1, 3, 5, 0, 2, 4);
    }
    
    @Test
    public void getModel_givenDescendingVisibilitySort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc:Visibility", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 2, 4, 1, 3, 5);
    }
    
    @Test
    public void getModel_givenAscendingFieldNameSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc:FieldName", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 5, 4, 3, 2, 1, 0);
    }
    
    @Test
    public void getModel_givenDescendingFieldNameSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc:FieldName", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 1, 2, 3, 4, 5);
    }
    
    @Test
    public void getModel_givenAscendingDataTypeSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc:DataType", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 1, 2, 3, 4, 5);
    }
    
    @Test
    public void getModel_givenDescendingDataTypeSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc:DataType", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 5, 4, 3, 2, 1, 0);
    }
    
    @Test
    public void getModel_givenAscendingModelFieldNameSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc:ModelFieldName", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 2, 3, 0, 4, 1, 5);
    }
    
    @Test
    public void getModel_givenDescendingModelFieldNameSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc:ModelFieldName", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 5, 1, 4, 0, 3, 2);
    }
    
    @Test
    public void getModel_givenAscendingDirectionSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "asc:Direction", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0, 2, 4, 1, 3, 5);
    }
    
    @Test
    public void getModel_givenDescendingDirectionSort_returnSortedMappings() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, "desc:Direction", null);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 1, 3, 5, 0, 2, 4);
    }
    
    @Test
    public void getModel_givenSearchTermThatMatchesEntry_returnMatch() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, null, "D1");
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the field mappings that were returned.
        assertMappingsInOrder(model, 0);
    }
    
    @Test
    public void getModel_givenSearchTermThatMatchesNone_returnEmptyModel() throws Exception {
        // Prepare test.
        expectContextToReturnPrincipal();
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        Model model = modelBean.getModel(modelThree.getName(), DEFAULT_TABLE_NAME, -1, 0, null, "no_match");
        
        // Verify expected calls.
        verifyAll();
        
        // Verify that no field mappings were returned.
        assertTrue(model.getFields().isEmpty());
    }
    
    @Test
    public void insertMapping_givenNonExistentTable_throwException() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.insertMapping(modelOne, "nonExistentTable"));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not insert mapping.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void insertMapping_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.insertMapping(modelOne, null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not insert mapping.");
    }
    
    @Test
    public void insertMapping_givenBlankTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.insertMapping(modelOne, " "));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not insert mapping.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void insertMapping_givenValidModel_insertMappings() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectCacheToBeReloaded();
        createDefaultTable();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test.
        modelBean.insertMapping(modelOne, DEFAULT_TABLE_NAME);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify results.
        Scanner scanner = ScannerHelper.createScanner(connector, DEFAULT_TABLE_NAME, AUTHS);
        Set<FieldMapping> fields = new HashSet<>();
        scanner.forEach(entry -> fields.add(ModelKeyParser.parseKey(entry.getKey())));
        assertEquals(modelOne.getFields(), fields);
    }
    
    @Test
    public void deleteMappings_givenNonExistentTable_throwException() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteMappings(modelOne, "nonExistentTable"));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not delete mapping.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class,
                        "Table nonExistentTable (Id=nonExistentTable) does not exist (no such table)");
    }
    
    @Test
    public void deleteMappings_givenNullTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteMappings(modelOne, null));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not delete mapping.");
    }
    
    @Test
    public void deleteMappings_givenBlankTableName_useDefaultTableName() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        
        // Make mocks available.
        replayAll();
        
        // Capture the expected exception.
        Exception exception = captureException(() -> modelBean.deleteMappings(modelOne, " "));
        
        // Verify expected calls.
        verifyAll();
        
        // Verify exception originally caused by table not found and that the original error messages refers to the default table name.
        assertException(exception, DatawaveWebApplicationException.class, "HTTP 500 Internal Server Error");
        assertException(exception.getCause(), QueryException.class, "Could not delete mapping.");
        assertException(exception.getCause().getCause(), TableNotFoundException.class, "Table " + DEFAULT_TABLE_NAME + " (Id=" + DEFAULT_TABLE_NAME
                        + ") does not exist (no such table)");
    }
    
    @Test
    public void deleteMappings_givenNonExistentModel_notDeleteAnything() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectCacheToBeReloaded();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test and trap expected exception.
        Model model = new Model();
        model.setName("modelThree");
        modelBean.deleteMappings(model, DEFAULT_TABLE_NAME);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify no mappings were deleted.
        Set<FieldMapping> expected = Sets.newHashSet(modelOne.getFields());
        expected.addAll(modelTwo.getFields());
        expected.addAll(modelThree.getFields());
        
        Scanner scanner = ScannerHelper.createScanner(connector, DEFAULT_TABLE_NAME, AUTHS);
        Set<FieldMapping> fields = new HashSet<>();
        scanner.forEach(entry -> fields.add(ModelKeyParser.parseKey(entry.getKey())));
        
        assertEquals(expected, fields);
    }
    
    @Test
    public void deleteMappings_givenValidModel_deleteMappings() throws Exception {
        // Prepare test.
        expectConnectionFactoryToReturnConnector();
        expectConnectorToBeReturnedToConnectionFactory();
        expectCacheToBeReloaded();
        loadTestData();
        
        // Make mocks available.
        replayAll();
        
        // Call method under test and trap expected exception.
        modelBean.deleteMappings(modelOne, DEFAULT_TABLE_NAME);
        
        // Verify expected calls.
        verifyAll();
        
        // Verify the modelOne mappings were deleted.
        Scanner scanner = ScannerHelper.createScanner(connector, DEFAULT_TABLE_NAME, AUTHS);
        Set<FieldMapping> fields = new HashSet<>();
        scanner.forEach(entry -> fields.add(ModelKeyParser.parseKey(entry.getKey())));
        
        assertThat(fields).containsAll(modelTwo.getFields());
        assertThat(fields).containsAll(modelThree.getFields());
    }
    
    private Exception captureException(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            return e;
        }
        return null;
    }
    
    private void expectContextToReturnPrincipal() {
        expect(context.getCallerPrincipal()).andReturn(principal);
    }
    
    private void expectConnectionFactoryToReturnConnector() throws Exception {
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getConnection(eq(AccumuloConnectionFactory.Priority.LOW), eq(trackingMap))).andReturn(connector);
    }
    
    private void expectConnectorToBeReturnedToConnectionFactory() throws Exception {
        connectionFactory.returnConnection(connector);
    }
    
    private void expectCacheToBeReloaded() {
        expect(tableCache.reloadCache(DEFAULT_TABLE_NAME)).andReturn(null);
    }
    
    private void assertException(Throwable throwable, Class<? extends Throwable> clazz, String message) {
        assertTrue(clazz.isInstance(throwable));
        assertEquals(message, throwable.getMessage());
    }
    
    private void loadTestData() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        createDefaultTable();
        BatchWriter writer = connector.createBatchWriter(DEFAULT_TABLE_NAME, new BatchWriterConfig());
        addMutations(modelOne, writer);
        addMutations(modelTwo, writer);
        addMutations(modelThree, writer);
        writer.close();
    }
    
    private void createDefaultTable() throws TableExistsException, AccumuloSecurityException, AccumuloException {
        connector.tableOperations().create(DEFAULT_TABLE_NAME);
    }
    
    private void addMutations(Model model, BatchWriter writer) throws MutationsRejectedException {
        for (FieldMapping field : model.getFields()) {
            writer.addMutation(ModelKeyParser.createMutation(field, model.getName()));
        }
    }
    
    private void assertMappingsInOrder(Model model, int... indices) {
        List<FieldMapping> fields = new ArrayList<>(model.getFields());
        for (int i = 0; i < indices.length; i++) {
            assertThat(fields.get(i)).isEqualTo(modelThreeMappings.get(indices[i]));
        }
    }
}
