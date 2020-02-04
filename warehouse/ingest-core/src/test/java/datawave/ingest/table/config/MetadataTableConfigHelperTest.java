package datawave.ingest.table.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

public class MetadataTableConfigHelperTest {
    
    public static class WrappedMetadataTableConfigHelper extends MetadataTableConfigHelper {
        
        public String exposeTableName() {
            
            return tableName;
        }
    }
    
    private static final Logger logger = Logger.getLogger(MetadataTableConfigHelperTest.class);
    private static Level testDriverLevel;
    
    public static final String DEFAULT_EXCEPTION_MESSAGE = "This is a test exception.  NOTHING IS ACTUALLY WRONG....";
    public static final String TABLE_NAME = "UNIT_TEST_TABLE";
    public static final String BAD_TABLE_NAME = "VERY_BAD_TABLE_NAME";
    
    protected List<String> debugMessages = null;
    protected List<String> infoMessages = null;
    protected Map<String,String> configuration = null;
    public Map<String,String> tableProperties;
    public Map<String,Set<Text>> localityGroups;
    
    @SuppressWarnings("unchecked")
    public TableOperations mockUpTableOperations() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        
        tableProperties = new HashMap<>();
        localityGroups = new HashMap<>();
        
        TableOperations mock = PowerMock.createMock(TableOperations.class);
        
        mock.getProperties(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            return tableProperties.entrySet();
        }).anyTimes();
        
        mock.setProperty(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
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
                
                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            return localityGroups;
        }).anyTimes();
        
        Map<String,Set<Text>> groups = new HashMap<>();
        
        mock.setLocalityGroups(EasyMock.anyObject(String.class), EasyMock.anyObject(groups.getClass()));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_CALLED = true;
            
            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];
            
            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {
                
                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }
            
            Map<String,Set<Text>> groups1 = (Map<String,Set<Text>>) EasyMock.getCurrentArguments()[1];
            
            localityGroups.putAll(groups1);
            
            return null;
        }).anyTimes();
        
        // prepare it for use...
        PowerMock.replay(mock);
        
        return mock;
    }
    
    public Configuration createMockConfiguration() {
        
        if (null == configuration) {
            
            configuration = new HashMap<>();
        }
        
        Configuration mock = PowerMock.createMock(Configuration.class);
        
        mock.get(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String results = null;
            String key = (String) EasyMock.getCurrentArguments()[0];
            String defaultValue = (String) EasyMock.getCurrentArguments()[1];
            
            if (configuration.containsKey(key)) {
                
                results = configuration.get(key);
            } else {
                
                results = defaultValue;
            }
            
            return results;
        }).anyTimes();
        
        mock.getBoolean(EasyMock.anyObject(String.class), EasyMock.anyBoolean());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            boolean results = (Boolean) EasyMock.getCurrentArguments()[1];
            
            if (configuration.containsKey(key)) {
                
                results = Boolean.parseBoolean(configuration.get(key));
            }
            
            return results;
        }).anyTimes();
        
        PowerMock.replay(mock);
        
        return mock;
    }
    
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
    
    @Before
    public void setup() {
        Level desiredLevel = Level.ALL;
        
        Logger log = Logger.getLogger(MetadataTableConfigHelperTest.class);
        MetadataTableConfigHelperTest.testDriverLevel = log.getLevel();
        log.setLevel(desiredLevel);
    }
    
    @After
    public void teardown() {
        
        MetadataTableConfigHelperTest.logger.setLevel(MetadataTableConfigHelperTest.testDriverLevel);
    }
    
    @Test
    public void testSetupNothingDefinedInConfiguration() {
        
        MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");
        
        try {
            
            WrappedMetadataTableConfigHelper uut = new WrappedMetadataTableConfigHelper();
            
            Configuration config = this.createMockConfiguration();
            Logger log = this.createMockLogger();
            
            uut.setup(MetadataTableConfigHelperTest.TABLE_NAME, config, log);
            
        } catch (IllegalArgumentException iae) {
            
            String msg = iae.getMessage();
            
            Assert.assertTrue("MetadataTableConfigHelper.setup threw the expected exception, but the message was not the expected message.",
                            msg.startsWith("No Such Table: "));
        } finally {
            
            MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }
        
    }
    
    @Test
    public void testSetupOneShardTableDefinedInConfiguration() {
        
        MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");
        
        try {
            
            WrappedMetadataTableConfigHelper uut = new WrappedMetadataTableConfigHelper();
            
            Configuration config = this.createMockConfiguration();
            Logger log = this.createMockLogger();
            
            this.configuration.put(ShardedDataTypeHandler.METADATA_TABLE_NAME, MetadataTableConfigHelperTest.TABLE_NAME);
            
            uut.setup(MetadataTableConfigHelperTest.TABLE_NAME, config, log);
            
            Assert.assertEquals("MetadataTableConfigHelper.setup incorrectly identified the TableName", MetadataTableConfigHelperTest.TABLE_NAME,
                            uut.exposeTableName());
            
        } finally {
            
            MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }
    }
    
    @Test
    public void testSetupOneShardTableDefinedInConfigurationButNotTableNameParameter() {
        
        MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");
        
        try {
            
            WrappedMetadataTableConfigHelper uut = new WrappedMetadataTableConfigHelper();
            
            Configuration config = this.createMockConfiguration();
            Logger log = this.createMockLogger();
            
            this.configuration.put(ShardedDataTypeHandler.METADATA_TABLE_NAME, MetadataTableConfigHelperTest.TABLE_NAME);
            try {
                
                uut.setup(MetadataTableConfigHelperTest.BAD_TABLE_NAME, config, log);
                
            } catch (IllegalArgumentException iae) {
                
                String msg = iae.getMessage();
                
                Assert.assertTrue("MetadataTableConfigHelper.setup threw the expected exception, but the message was not the expected message.",
                                msg.startsWith("No Such Table: "));
            }
            
        } finally {
            
            MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }
        
    }
    
    @Test
    public void testConfigureCalledBeforeSetup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testConfigureCalledBeforeSetup called.");
        
        try {
            
            WrappedMetadataTableConfigHelper uut = new WrappedMetadataTableConfigHelper();
            
            TableOperations tops = mockUpTableOperations();
            
            uut.configure(tops);
            
            Assert.assertTrue("MetadataTableConfigHelper.configure incorrectly populated the Table Properties.", this.tableProperties.isEmpty());
            
        } finally {
            
            MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testConfigureCalledBeforeSetup completed.");
        }
    }
    
    @Test
    public void testConfigureShardTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        
        MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testConfigureShardTable called.");
        
        try {
            
            Configuration config = createMockConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();
            
            WrappedMetadataTableConfigHelper uut = new WrappedMetadataTableConfigHelper();
            
            this.configuration.put(ShardedDataTypeHandler.METADATA_TABLE_NAME, MetadataTableConfigHelperTest.TABLE_NAME);
            
            this.tableProperties.clear();
            this.localityGroups.clear();
            
            uut.setup(MetadataTableConfigHelperTest.TABLE_NAME, config, log);
            
            Assert.assertEquals("MetadataTableConfigHelper.setup incorrectly identified the TableName", MetadataTableConfigHelperTest.TABLE_NAME,
                            uut.exposeTableName());
            
            uut.configure(tops);
            
            Assert.assertFalse("MetadataTableConfigHelper.configureShardTable failed to populate the Table Properties collection.",
                            this.tableProperties.isEmpty());
            Assert.assertTrue("MetadataTableConfigHelper.configureShardTable caused the the Locality Groups collection to be populated.",
                            this.localityGroups.isEmpty());
            
            uut = new WrappedMetadataTableConfigHelper();
            
            this.configuration.put(ShardedDataTypeHandler.METADATA_TABLE_NAME, MetadataTableConfigHelperTest.TABLE_NAME);
            
            this.tableProperties.clear();
            this.localityGroups.clear();
            
            uut.setup(MetadataTableConfigHelperTest.TABLE_NAME, config, log);
            
            Assert.assertEquals("MetadataTableConfigHelper.setup incorrectly identified the TableName", MetadataTableConfigHelperTest.TABLE_NAME,
                            uut.exposeTableName());
            
            uut.configure(tops);
            
            Assert.assertFalse("MetadataTableConfigHelper.configureShardTable failed to populate the Table Properties collection.",
                            this.tableProperties.isEmpty());
            Assert.assertTrue("MetadataTableConfigHelper.configureShardTable caused the the Locality Groups collection to be populated.",
                            this.localityGroups.isEmpty());
            
        } finally {
            
            MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testConfigureShardTable completed.");
        }
    }
    
}
