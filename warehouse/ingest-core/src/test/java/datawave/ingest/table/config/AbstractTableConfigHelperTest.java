package datawave.ingest.table.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.ingest.table.aggregator.CombinerConfiguration;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

@SuppressWarnings("deprecation")
public class AbstractTableConfigHelperTest {
    
    private static final String BAD_TABLE_NAME = "VERY_BAD_TABLE_NAME";
    private static final Logger logger = Logger.getLogger(AbstractTableConfigHelperTest.class);
    private static Level testDriverLevel;
    
    @BeforeClass
    public static void adjustLogLevels() {
        
        Level desiredLevel = Level.ALL;
        
        Logger log = Logger.getLogger(AbstractTableConfigHelperTest.class);
        AbstractTableConfigHelperTest.testDriverLevel = log.getLevel();
        log.setLevel(desiredLevel);
    }
    
    @AfterClass
    public static void resetLogLevels() {
        
        Logger log = Logger.getLogger(AbstractTableConfigHelperTest.class);
        log.setLevel(AbstractTableConfigHelperTest.testDriverLevel);
    }
    
    public static class TestAbstractTableConfigHelperImpl extends AbstractTableConfigHelper {
        
        public AbstractTableConfigHelperTest parent;
        
        protected Logger createMockLogger() {
            
            Logger log = PowerMock.createMock(Logger.class);
            
            if (null == debugMessages) {
                
                debugMessages = new ArrayList<>();
            }
            
            if (null == infoMessages) {
                
                infoMessages = new ArrayList<>();
            }
            
            log.debug(EasyMock.anyObject(String.class));
            EasyMock.expectLastCall().andAnswer(() -> {
                
                String msg = (String) EasyMock.getCurrentArguments()[0];
                
                debugMessages.add(msg);
                
                return null;
            }).anyTimes();
            
            log.info(EasyMock.anyObject(String.class));
            EasyMock.expectLastCall().andAnswer(() -> {
                
                String msg = (String) EasyMock.getCurrentArguments()[0];
                
                infoMessages.add(msg);
                
                return null;
            }).anyTimes();
            
            PowerMock.replay(log);
            
            return log;
        }
        
        protected static final String aggClassName = datawave.ingest.table.aggregator.TextIndexAggregator.class.getName();
        
        protected List<CombinerConfiguration> createWorkingCombinerConfigurations() {
            
            List<CombinerConfiguration> lag = new ArrayList<>();
            
            for (int counter = 0; counter < 3; counter++) {
                
                String colFamilyName = String.format("column-family-%d", counter);
                
                lag.add(new CombinerConfiguration(new Column(new Text(colFamilyName)), new IteratorSetting(10, "agg", aggClassName)));
            }
            
            return lag;
        }
        
        @Override
        public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {}
        
        @Override
        public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {}
        
        public void exposeSetLocalityGroupConfigurationIfNecessary() throws AssertionError, AccumuloSecurityException, AccumuloException,
                        TableNotFoundException {
            
            Assert.assertNotNull("Unit Under Test not configured correctly - Parent missing.", this.parent);
            
            try {
                
                setLocalityGroupConfigurationCalled = false;
                areLocalityGroupsConfiguredCalled = false;
                overrideAreLocalityGroupsConfigured = true;
                resultsForAreLocalityGroupsConfigured = true;
                
                TableOperations tops = parent.mockUpTableOperations();
                Logger log = createMockLogger();
                Map<String,Set<Text>> newLocalityGroups = new HashMap<>();
                newLocalityGroups.put("hello, world!", new HashSet<>());
                
                this.setLocalityGroupConfigurationIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, newLocalityGroups, tops, log);
                
                Assert.assertTrue("SetLocalityGroupConfigurationIfNecessary failed to call areAggregatorsConfigured()", areLocalityGroupsConfiguredCalled);
                int actualSize = this.debugMessages.size();
                Assert.assertEquals("SetLocalityGroupConfigurationIfNecessary failed to set the expected number of debug Log Messages", 1, actualSize);
                this.debugMessages.clear();
                actualSize = this.infoMessages.size();
                Assert.assertEquals("SetLocalityGroupConfigurationIfNecessary set more then the expected number of info Log Messages", 0, actualSize);
                this.infoMessages.clear();
                
                areLocalityGroupsConfiguredCalled = false;
                overrideAreLocalityGroupsConfigured = true;
                resultsForAreLocalityGroupsConfigured = false;
                
                this.setLocalityGroupConfigurationIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, newLocalityGroups, tops, log);
                
                Assert.assertTrue("SetLocalityGroupConfigurationIfNecessary failed to call areAggregatorsConfigured()", areLocalityGroupsConfiguredCalled);
                actualSize = this.debugMessages.size();
                Assert.assertEquals("SetLocalityGroupConfigurationIfNecessary failed to set the expected number of debug Log Messages", 0, actualSize);
                this.debugMessages.clear();
                actualSize = this.infoMessages.size();
                Assert.assertEquals("SetLocalityGroupConfigurationIfNecessary set more then the expected number of info Log Messages", 2, actualSize);
                this.infoMessages.clear();
                
                newLocalityGroups.clear();
                Set<Text> values = new HashSet<>();
                
                values.add(new Text("hello, world!"));
                newLocalityGroups.put(AbstractTableConfigHelperTest.FIXED_KEYS[0], values);
                
                areLocalityGroupsConfiguredCalled = false;
                overrideAreLocalityGroupsConfigured = true;
                resultsForAreLocalityGroupsConfigured = false;
                
                this.setLocalityGroupConfigurationIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, newLocalityGroups, tops, log);
                
                Assert.assertTrue("SetLocalityGroupConfigurationIfNecessary failed to call areAggregatorsConfigured()", areLocalityGroupsConfiguredCalled);
                actualSize = this.debugMessages.size();
                Assert.assertEquals("SetLocalityGroupConfigurationIfNecessary failed to set the expected number of debug Log Messages", 0, actualSize);
                this.debugMessages.clear();
                actualSize = this.infoMessages.size();
                Assert.assertEquals("SetLocalityGroupConfigurationIfNecessary set more then the expected number of info Log Messages", 2, actualSize);
                this.infoMessages.clear();
                
            } finally {
                
                setLocalityGroupConfigurationCalled = false;
                areLocalityGroupsConfiguredCalled = false;
                overrideAreLocalityGroupsConfigured = false;
                resultsForAreLocalityGroupsConfigured = true;
                
            }
            
        }
        
        public void exposeAreLocalityGroupsConfigured() throws AssertionError, AccumuloSecurityException, AccumuloException, TableNotFoundException {
            
            Assert.assertNotNull("Unit Under Test not configured correctly - Parent missing", this.parent);
            
            String tableName = AbstractTableConfigHelperTest.TABLE_NAME;
            Map<String,Set<Text>> newLocalityGroups = new HashMap<>();
            TableOperations tops = parent.mockUpTableOperations();
            
            newLocalityGroups.put("hello, world!", new HashSet<>());
            
            overrideAreLocalityGroupsConfigured = true;
            resultsForAreLocalityGroupsConfigured = true;
            areLocalityGroupsConfiguredCalled = false;
            
            this.areLocalityGroupsConfigured(tableName, newLocalityGroups, tops);
            
            Assert.assertTrue("AreAggregatorsConfigured() failed to call areAggregatorsConfigured", areLocalityGroupsConfiguredCalled);
            
            overrideAreLocalityGroupsConfigured = false;
            areLocalityGroupsConfiguredCalled = false;
            
            boolean results = this.areLocalityGroupsConfigured(tableName, newLocalityGroups, tops);
            
            Assert.assertTrue("AreAggregatorsConfigured() failed to call areAggregatorsConfigured", areLocalityGroupsConfiguredCalled);
            Assert.assertFalse("AreAggregatorsConfigured() unexpectedly found a family", results);
            
            newLocalityGroups.clear();
            newLocalityGroups.put(AbstractTableConfigHelperTest.FIXED_KEYS[0], new HashSet<>());
            
            results = this.areLocalityGroupsConfigured(tableName, newLocalityGroups, tops);
            
            Assert.assertTrue("AreAggregatorsConfigured() failed to call areAggregatorsConfigured", areLocalityGroupsConfiguredCalled);
            Assert.assertTrue("AreAggregatorsConfigured() unexpectedly found a family", results);
            
            newLocalityGroups.clear();
            Set<Text> values = new HashSet<>();
            
            values.add(new Text(AbstractTableConfigHelperTest.FIXED_VALUES[0]));
            values.add(new Text(AbstractTableConfigHelperTest.FIXED_VALUES[1]));
            values.add(new Text("hello, world!"));
            newLocalityGroups.put(AbstractTableConfigHelperTest.FIXED_KEYS[0], values);
            
            results = this.areLocalityGroupsConfigured(tableName, newLocalityGroups, tops);
            
            Assert.assertTrue("AreAggregatorsConfigured() failed to call areAggregatorsConfigured", areLocalityGroupsConfiguredCalled);
            Assert.assertFalse("AreAggregatorsConfigured() unexpectedly failed to find a family", results);
        }
        
        public void exposeAreAggregatorsConfigured() throws AssertionError, TableNotFoundException, AccumuloSecurityException, AccumuloException {
            
            Assert.assertNotNull("Unit Under Test not configured correctly - Parent missing", this.parent);
            
            try {
                
                String tableName = AbstractTableConfigHelperTest.TABLE_NAME;
                List<CombinerConfiguration> aggregators = createWorkingCombinerConfigurations();
                TableOperations tops = parent.mockUpTableOperations();
                
                overrideAreAggregatorsConfigured = false;
                areAggregatorsConfiguredCalled = false;
                AbstractTableConfigHelperTest.GET_PROPERTIES_THROWS_ACCUMULO_EXCEPTION = false;
                
                boolean results = this.areAggregatorsConfigured(tableName, aggregators, tops);
                
                Assert.assertTrue("AreAggregatorsConfigured called", areAggregatorsConfiguredCalled);
                Assert.assertFalse("AreAggregatorsConfigured returned and unexpected results", results);
                
                parent.tableProperties.clear();
                Map<String,String> props = generateInitialTableProperties();
                props.putAll(AbstractTableConfigHelper.generateAggTableProperties(aggregators));
                
                int counter = 0;
                for (String key : props.keySet()) {
                    
                    String value = props.get(key);
                    
                    ++counter;
                    
                    if (0 == (counter % 2)) {
                        
                        value = String.format("%s-%d", value, counter);
                    }
                    
                    parent.tableProperties.put(key, value);
                }
                
                results = this.areAggregatorsConfigured(tableName, aggregators, tops);
                
                Assert.assertTrue("AreAggregatorsConfigured called", areAggregatorsConfiguredCalled);
                Assert.assertFalse("AreAggregatorsConfigured returned and unexpected results", results);
                
                parent.tableProperties.clear();
                props = generateInitialTableProperties();
                props.putAll(AbstractTableConfigHelper.generateAggTableProperties(aggregators));
                
                parent.tableProperties.putAll(props);
                
                results = this.areAggregatorsConfigured(tableName, aggregators, tops);
                
                Assert.assertTrue("AreAggregatorsConfigured called", areAggregatorsConfiguredCalled);
                Assert.assertTrue("AreAggregatorsConfigured returned and unexpected results", results);
                
                try {
                    
                    overrideAreAggregatorsConfigured = false;
                    areAggregatorsConfiguredCalled = false;
                    AbstractTableConfigHelperTest.GET_PROPERTIES_THROWS_ACCUMULO_EXCEPTION = false;
                    
                    tableName = AbstractTableConfigHelperTest.BAD_TABLE_NAME;
                    
                    this.areAggregatorsConfigured(tableName, aggregators, tops);
                    
                    Assert.fail("AreAggregratorsConfigured failed to throw the expected exception.");
                    
                } catch (RuntimeException re) {
                    
                    Throwable cause = re.getCause();
                    
                    Assert.assertEquals(
                                    "AreAggregatorsConfigured throw the expected exception, but the type of the wrapped cause exception not the expected type.",
                                    TableNotFoundException.class, cause.getClass());
                }
                
                try {
                    
                    overrideAreAggregatorsConfigured = false;
                    areAggregatorsConfiguredCalled = false;
                    AbstractTableConfigHelperTest.GET_PROPERTIES_THROWS_ACCUMULO_EXCEPTION = true;
                    
                    tableName = AbstractTableConfigHelperTest.TABLE_NAME;
                    
                    this.areAggregatorsConfigured(tableName, aggregators, tops);
                    
                    Assert.fail("AreAggregratorsConfigured failed to throw the expected exception.");
                    
                } catch (RuntimeException re) {
                    
                    Throwable cause = re.getCause();
                    
                    Assert.assertEquals(
                                    "AreAggregatorsConfigured throw the expected exception, but the type of the wrapped cause exception not the expected type.",
                                    AccumuloException.class, cause.getClass());
                }
                
            } finally {
                
                overrideAreAggregatorsConfigured = false;
                areAggregatorsConfiguredCalled = false;
                AbstractTableConfigHelperTest.GET_PROPERTIES_THROWS_ACCUMULO_EXCEPTION = false;
            }
        }
        
        public void exposeSetCombinerConfigurationIfNecessaryForTest() throws AssertionError, AccumuloSecurityException, AccumuloException,
                        TableNotFoundException {
            
            Assert.assertNotNull("Unit Under Test not configured correctly - Parent missing", this.parent);
            
            String tableName = AbstractTableConfigHelperTest.TABLE_NAME;
            List<CombinerConfiguration> aggregators = createWorkingCombinerConfigurations();
            TableOperations tops = parent.mockUpTableOperations();
            Logger log = createMockLogger();
            
            overrideAreAggregatorsConfigured = true;
            resultsForOverridenAreAggregatorsConfigured = true;
            areAggregatorsConfiguredCalled = false;
            
            this.setAggregatorConfigurationIfNecessary(tableName, aggregators, tops, log);
            
            Assert.assertTrue("SetCombinerConfigurationIfNecessary() failed to call areAggregatorsConfigured", areAggregatorsConfiguredCalled);
            Assert.assertEquals("SetCombinerConfigurationIfNecessary() failed to generate the expected number of debug messages.", 1, debugMessages.size());
            Assert.assertEquals("SetCombinerConfigurationIfNecessary() failed to generate the expected number of info messages.", 0, infoMessages.size());
            
            debugMessages.clear();
            overrideAreAggregatorsConfigured = true;
            resultsForOverridenAreAggregatorsConfigured = false;
            areAggregatorsConfiguredCalled = false;
            
            this.setAggregatorConfigurationIfNecessary(tableName, aggregators, tops, log);
            
            Assert.assertTrue("SetCombinerConfigurationIfNecessary() failed to call areAggregatorsConfigured", areAggregatorsConfiguredCalled);
            Assert.assertEquals("SetCombinerConfigurationIfNecessary() failed to generate the expected number of debug messages.", 0, debugMessages.size());
            Assert.assertEquals("SetCombinerConfigurationIfNecessary() failed to generate the expected number of info messages.", 1, infoMessages.size());
            
            Map<String,String> props = generateInitialTableProperties();
            props.putAll(AbstractTableConfigHelper.generateAggTableProperties(aggregators));
            for (int counter = 0; counter < AbstractTableConfigHelperTest.FIXED_KEYS.length; counter++) {
                
                props.put(AbstractTableConfigHelperTest.FIXED_KEYS[counter], AbstractTableConfigHelperTest.FIXED_VALUES[counter]);
            }
            
            boolean foundAllExpectedProperties = true;
            
            for (String key : props.keySet()) {
                
                foundAllExpectedProperties &= parent.tableProperties.containsKey(key);
            }
            
            for (String key : parent.tableProperties.keySet()) {
                
                foundAllExpectedProperties &= props.containsKey(key);
            }
            
            Assert.assertTrue("Generated Table Properties not what was expected.", foundAllExpectedProperties);
        }
        
        protected List<String> debugMessages = null;
        protected List<String> infoMessages = null;
        public boolean areAggregatorsConfiguredCalled = false;
        public boolean overrideAreAggregatorsConfigured = false;
        public boolean resultsForOverridenAreAggregatorsConfigured = false;
        
        public boolean areLocalityGroupsConfiguredCalled = false;
        public boolean overrideAreLocalityGroupsConfigured = false;
        public boolean resultsForAreLocalityGroupsConfigured = false;
        
        public boolean setLocalityGroupConfigurationCalled = false;
        public boolean overrideSetLocalityGroupConfigured = false;
        public boolean resultsForSetLocalityGroupsConfigured = false;
        
        @Override
        protected boolean areAggregatorsConfigured(String tableName, List<CombinerConfiguration> aggregators, TableOperations tops)
                        throws TableNotFoundException {
            
            boolean results = false;
            
            areAggregatorsConfiguredCalled = true;
            
            if (overrideAreAggregatorsConfigured) {
                
                results = resultsForOverridenAreAggregatorsConfigured;
            } else {
                
                results = super.areAggregatorsConfigured(tableName, aggregators, tops);
            }
            
            return results;
        }
        
        @Override
        protected boolean areLocalityGroupsConfigured(String tableName, Map<String,Set<Text>> newLocalityGroups, TableOperations tops)
                        throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
            
            boolean results = false;
            
            areLocalityGroupsConfiguredCalled = true;
            
            if (overrideAreLocalityGroupsConfigured) {
                
                results = resultsForAreLocalityGroupsConfigured;
                
            } else {
                
                results = super.areLocalityGroupsConfigured(tableName, newLocalityGroups, tops);
                
            }
            
            return results;
        }
        
        @Override
        protected void setLocalityGroupConfigurationIfNecessary(String tableName, Map<String,Set<Text>> newLocalityGroups, TableOperations tops, Logger log)
                        throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
            
            setLocalityGroupConfigurationCalled = true;
            
            if (!overrideSetLocalityGroupConfigured) {
                
                super.setLocalityGroupConfigurationIfNecessary(tableName, newLocalityGroups, tops, log);
            }
        }
        
    }
    
    private static final String DEFAULT_EXCEPTION_MESSAGE = "This is a test exception.  NOTHING IS ACTUALLY WRONG....";
    public static final String TABLE_NAME = "UNIT_TEST_TABLE";
    public static final String USER = "UNIT_TEST_PSEUDO_USER";
    public static final String PROPERTY_NAME = "name";
    public static final String PROPERTY_VALUE = "value";
    public Map<String,String> tableProperties;
    public Map<String,Set<Text>> localityGroups;
    
    protected static boolean GET_PROPERTIES_CALLED = false;
    protected static boolean GET_PROPERTIES_THROWS_ACCUMULO_EXCEPTION = false;
    
    protected static boolean SET_PROPERTIES_CALLED = false;
    protected static boolean SET_PROPERTIES_THROWS_ACCUMULO_SECUIRTY_EXCEPTION = false;
    
    protected static boolean ARE_LOCALITY_GROUPS_CONFIGURED_CALLED = false;
    protected static boolean ARE_LOCALITY_GROUPS_CONFIGURED_THROWS_ACCUMULO_SECUIRTY_EXCEPTION = false;
    
    protected static boolean SET_LOCALITY_GROUPS_CONFIGURED_CALLED = false;
    protected static boolean SET_LOCALITY_GROUPS_CONFIGURED_THROWS_ACCUMULO_SECUIRTY_EXCEPTION = false;
    protected static boolean SET_LOCALITY_GROUPS_CONFIGURED_THROWS_ACCUMULO_EXCEPTION = false;
    
    protected static String[] FIXED_KEYS = new String[] {"name-0", "name-1", "name-2"};
    protected static String[] FIXED_VALUES = new String[] {"value-0", "value-1", "value-2"};
    
    @SuppressWarnings("unchecked")
    public TableOperations mockUpTableOperations() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        
        tableProperties = new HashMap<>();
        for (int index = 0; index < AbstractTableConfigHelperTest.FIXED_KEYS.length; index++) {
            
            tableProperties.put(AbstractTableConfigHelperTest.FIXED_KEYS[index], AbstractTableConfigHelperTest.FIXED_VALUES[index]);
        }
        
        localityGroups = new HashMap<>();
        
        for (int index = 0; index < AbstractTableConfigHelperTest.FIXED_KEYS.length; index++) {
            Set<Text> values = new HashSet<>();
            
            localityGroups.put(AbstractTableConfigHelperTest.FIXED_KEYS[index], values);
            
            for (int counter = 0; counter < AbstractTableConfigHelperTest.FIXED_VALUES.length; counter++) {
                
                values.add(new Text(AbstractTableConfigHelperTest.FIXED_VALUES[counter]));
            }
        }
        
        TableOperations mock = PowerMock.createMock(TableOperations.class);
        
        mock.getProperties(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            if (AbstractTableConfigHelperTest.GET_PROPERTIES_THROWS_ACCUMULO_EXCEPTION) {
                
                throw new AccumuloException(AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            return tableProperties.entrySet();
        }).anyTimes();
        
        mock.setProperty(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            if (AbstractTableConfigHelperTest.SET_PROPERTIES_THROWS_ACCUMULO_SECUIRTY_EXCEPTION) {
                
                throw new AccumuloSecurityException(AbstractTableConfigHelperTest.USER, SecurityErrorCode.PERMISSION_DENIED);
            }
            
            String name = (String) EasyMock.getCurrentArguments()[1];
            String value = (String) EasyMock.getCurrentArguments()[2];
            
            tableProperties.put(name, value);
            return null;
        }).anyTimes();
        
        mock.getLocalityGroups(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.ARE_LOCALITY_GROUPS_CONFIGURED_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            if (AbstractTableConfigHelperTest.ARE_LOCALITY_GROUPS_CONFIGURED_THROWS_ACCUMULO_SECUIRTY_EXCEPTION) {
                
                throw new AccumuloSecurityException(AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE, SecurityErrorCode.DEFAULT_SECURITY_ERROR);
            }
            
            return localityGroups;
        }).anyTimes();
        
        Map<String,Set<Text>> groups = new HashMap<>();
        
        mock.setLocalityGroups(EasyMock.anyObject(String.class), EasyMock.anyObject(groups.getClass()));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            if (AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_THROWS_ACCUMULO_SECUIRTY_EXCEPTION) {
                
                throw new AccumuloSecurityException(AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE, SecurityErrorCode.DEFAULT_SECURITY_ERROR);
            }
            
            if (AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_THROWS_ACCUMULO_EXCEPTION) {
                
                throw new AccumuloException(AbstractTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            Map<String,Set<Text>> groups1 = (Map<String,Set<Text>>) EasyMock.getCurrentArguments()[1];
            
            localityGroups.putAll(groups1);
            
            return null;
        }).anyTimes();
        
        // prepare it for use...
        PowerMock.replay(mock);
        
        return mock;
    }
    
    @Test
    public void testSetPropertyIfNecessary() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() called.");
        
        try {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            TableOperations tops = this.mockUpTableOperations();
            Assert.assertNotNull("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() failed to create a mock TableOperations instance.", tops);
            
            int propertySize = tableProperties.size();
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            String name = "name";
            String value = "value";
            
            uut.setPropertyIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, name, value, tops, null);
            
            ++propertySize;
            
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.getProperties as expected.",
                            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED);
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.setProperties as expected.",
                            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED);
            Assert.assertEquals(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new property as expected",
                            propertySize, tableProperties.size());
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new key, 'name'.",
                            tableProperties.containsKey(name));
            Assert.assertTrue(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new value, 'value'.",
                            tableProperties.containsValue(value));
            
        } finally {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() completed.");
        }
        
    }
    
    @Test
    public void testSetPropertyIfNecessaryWithExistingValue() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() called.");
        
        try {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            TableOperations tops = this.mockUpTableOperations();
            Assert.assertNotNull("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() failed to create a mock TableOperations instance.", tops);
            
            int propertySize = tableProperties.size();
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            String name = "name";
            String value = "value-0";
            
            uut.setPropertyIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, name, value, tops, null);
            
            ++propertySize;
            
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.getProperties as expected.",
                            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED);
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.setProperties as expected.",
                            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED);
            Assert.assertEquals(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new property as expected",
                            propertySize, tableProperties.size());
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new key, 'name'.",
                            tableProperties.containsKey(name));
            Assert.assertTrue(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new value, 'value'.",
                            tableProperties.containsValue(value));
            
        } finally {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() completed.");
        }
        
    }
    
    @Test
    public void testSetPropertyIfNecessaryWithExistingProperty() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessaryWithExistingProperty() called.");
        
        try {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            TableOperations tops = this.mockUpTableOperations();
            Assert.assertNotNull("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() failed to create a mock TableOperations instance.", tops);
            
            int propertySize = tableProperties.size();
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            String name = "name-0";
            String value = "value-0";
            
            uut.setPropertyIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, name, value, tops, null);
            
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.getProperties as expected.",
                            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED);
            Assert.assertFalse("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.setProperties as expected.",
                            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED);
            Assert.assertEquals(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new property as expected",
                            propertySize, tableProperties.size());
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new key, 'name'.",
                            tableProperties.containsKey(name));
            Assert.assertTrue(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new value, 'value'.",
                            tableProperties.containsValue(value));
            
        } finally {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessaryWithExistingProperty() completed.");
        }
        
    }
    
    @Test
    public void testSetPropertyIfNecessaryWithExistingKeyProperty() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessaryWithExistingProperty() called.");
        
        try {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            TableOperations tops = this.mockUpTableOperations();
            Assert.assertNotNull("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() failed to create a mock TableOperations instance.", tops);
            
            int propertySize = tableProperties.size();
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            String name = "name-0";
            String value = "value";
            
            uut.setPropertyIfNecessary(AbstractTableConfigHelperTest.TABLE_NAME, name, value, tops, null);
            
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.getProperties as expected.",
                            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED);
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.setProperties as expected.",
                            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED);
            Assert.assertEquals(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new property as expected",
                            propertySize, tableProperties.size());
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new key, 'name'.",
                            tableProperties.containsKey(name));
            Assert.assertTrue(
                            "AbstractTableConfigHelper.setProperityIfNecessary() call to TableOperations.setProperties failed to add the new value, 'value'.",
                            tableProperties.containsValue(value));
            
        } finally {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessaryWithExistingProperty() completed.");
        }
        
    }
    
    @Test
    public void testSetPropertyIfNecessaryWithBadTableName() throws AccumuloSecurityException, AccumuloException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessaryWithBadTableName() called.");
        
        try {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            TableOperations tops = this.mockUpTableOperations();
            Assert.assertNotNull("AbstractTableConfigHelperTest.testSetPropertyIfNecessary() failed to create a mock TableOperations instance.", tops);
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            
            uut.setPropertyIfNecessary(AbstractTableConfigHelperTest.BAD_TABLE_NAME, "name", "value", this.mockUpTableOperations(), null);
            
            Assert.fail("AbstractTableConfigHelper.setProperityIfNecessary() failed to throw the expected excepion.");
            
        } catch (TableNotFoundException tnfe) {
            
            Assert.assertTrue("AbstractTableConfigHelper.setProperityIfNecessary() failed to call TableOperations.getProperties as expected.",
                            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED);
            Assert.assertFalse("AbstractTableConfigHelper.setProperityIfNecessary() actually called TableOperations.setProperties unexpectedly.",
                            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED);
            
        } finally {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = false;
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = false;
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetPropertyIfNecessaryWithBadTableName() completed.");
        }
        
    }
    
    @Test
    public void testSetCombinerConfigurationIfNecessary() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetCombinerConfigurationIfNecessary() called.");
        
        try {
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            
            uut.parent = this;
            
            uut.exposeSetCombinerConfigurationIfNecessaryForTest();
            
        } finally {
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetCombinerConfigurationIfNecessary() completed.");
        }
    }
    
    @Test
    public void testAreAggregatorsConfigured() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testAreAggregatorsConfigured() called.");
        
        try {
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            
            uut.parent = this;
            
            uut.exposeAreAggregatorsConfigured();
            
        } finally {
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testAreAggregatorsConfigured() completed.");
        }
        
    }
    
    @Test
    public void testAreLocalityGroupsConfigured() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testAreLocalityGroupsConfigured() called.");
        
        try {
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            
            uut.parent = this;
            
            uut.exposeAreLocalityGroupsConfigured();
            
        } finally {
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testAreLocalityGroupsConfigured() completed.");
        }
    }
    
    @Test
    public void testSetLocalityGroupConfigurationIfNecessary() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetLocalityGroupConfigurationIfNecessary() called.");
        
        try {
            
            AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl uut = new AbstractTableConfigHelperTest.TestAbstractTableConfigHelperImpl();
            Assert.assertNotNull("AbstractTableConfigHelper.cTor failed to create an instance", uut);
            
            uut.parent = this;
            
            uut.exposeSetLocalityGroupConfigurationIfNecessary();
            
        } finally {
            
            AbstractTableConfigHelperTest.logger.info("AbstractTableConfigHelperTest.testSetLocalityGroupConfigurationIfNecessary() completed.");
        }
    }
    
}
