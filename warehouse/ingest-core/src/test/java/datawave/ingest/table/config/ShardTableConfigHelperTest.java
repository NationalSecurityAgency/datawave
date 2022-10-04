package datawave.ingest.table.config;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.table.aggregator.GlobalIndexUidAggregator;
import datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator;
import datawave.ingest.table.config.ShardTableConfigHelper.ShardTableType;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShardTableConfigHelperTest {

    private static final Logger logger = Logger.getLogger(ShardTableConfigHelperTest.class);
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

        TableOperations mock = EasyMock.createMock(TableOperations.class);

        mock.getProperties(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {

            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return tableProperties.entrySet();
        }).anyTimes();

        mock.setProperty(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {

            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
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

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return localityGroups;
        }).anyTimes();

        Map<String,Set<Text>> groups = new HashMap<>();

        mock.setLocalityGroups(EasyMock.anyObject(String.class), EasyMock.anyObject(groups.getClass()));
        EasyMock.expectLastCall().andAnswer(() -> {

            AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_CALLED = true;

            String tableNameParameter = (String) EasyMock.getCurrentArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            Map<String,Set<Text>> groups1 = (Map<String,Set<Text>>) EasyMock.getCurrentArguments()[1];

            localityGroups.putAll(groups1);

            return null;
        }).anyTimes();

        // prepare it for use...
        EasyMock.replay(mock);

        return mock;
    }

    public Configuration createMockConfiguration() {

        if (null == configuration) {

            configuration = new HashMap<>();
        }

        Configuration mock = EasyMock.createMock(Configuration.class);

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

        EasyMock.replay(mock);

        return mock;
    }

    protected Logger createMockLogger() {

        Logger log = EasyMock.createMock(Logger.class);

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

        EasyMock.replay(log);

        return log;
    }

    @BeforeEach
    public void setup() {
        Level desiredLevel = Level.ALL;

        Logger log = Logger.getLogger(ShardTableConfigHelperTest.class);
        ShardTableConfigHelperTest.testDriverLevel = log.getLevel();
        log.setLevel(desiredLevel);
    }

    @AfterEach
    public void teardown() {

        ShardTableConfigHelperTest.logger.setLevel(ShardTableConfigHelperTest.testDriverLevel);
    }

    @Test
    public void testSetupNothingDefinedInConfiguration() {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            Configuration config = this.createMockConfiguration();
            Logger log = this.createMockLogger();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

        } catch (IllegalArgumentException iae) {

            String msg = iae.getMessage();

            Assertions.assertEquals(
                    "No Shard Tables Defined", msg, "ShardTableConfigHelper .setup threw the expected exception, but the message was not the expected message.");

        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }

    }

    @Test
    public void testSetupOneShardTableDefinedInConfiguration() {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            Configuration config = this.createMockConfiguration();
            Logger log = this.createMockLogger();

            this.configuration.put(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.SHARD;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            this.configuration.clear();

            this.configuration.put(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GIDX;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            this.configuration.clear();

            this.configuration.put(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GRIDX;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }
    }

    @Test
    public void testSetupOneShardTableDefinedInConfigurationButNotTableNameParameter() {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            Configuration config = this.createMockConfiguration();
            Logger log = this.createMockLogger();

            this.configuration.put(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(ShardTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                Assertions.assertTrue(msg.startsWith("Invalid Shard Table Definition For: "),
                        "ShardTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

            this.configuration.clear();
            this.configuration.put(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(ShardTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                Assertions.assertTrue(msg.startsWith("Invalid Shard Table Definition For: "),
                        "ShardTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

            this.configuration.clear();
            this.configuration.put(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(ShardTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                Assertions.assertTrue(msg.startsWith("Invalid Shard Table Definition For: "),
                        "ShardTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }

    }

    @Test
    public void testConfigureCalledBeforeSetup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureCalledBeforeSetup called.");

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            TableOperations tops = mockUpTableOperations();

            Assertions.assertThrows(NullPointerException.class, () -> uut.configure(tops), "ShardTableConfigHelper.configure failed to throw expected exception.");

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureCalledBeforeSetup completed.");

    }

    @Test
    public void testConfigureShardTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureShardTable called.");

        try {

            Configuration config = createMockConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.SHARD;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            Assertions.assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            Assertions.assertFalse(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Locality Groups collection.");

            uut = new ShardTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            this.configuration.put(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, "true");

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.SHARD;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            Assertions.assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            Assertions.assertFalse(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populae the Locality Groups collection.");
        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureShardTable completed.");
        }
    }

    @Test
    public void testConfigureGidxTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGidxTable called.");

        try {

            Configuration config = createMockConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.GIDX;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            Assertions.assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            Assertions.assertTrue(this.localityGroups.isEmpty(),
                    "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");

            uut = new ShardTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            this.configuration.put(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, "true");

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GIDX;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            Assertions.assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            Assertions.assertTrue(this.localityGroups.isEmpty(),
                    "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");
        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGidxTable completed.");
        }
    }

    @Test
    public void testConfigureGridxTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGridxTable called.");

        try {

            Configuration config = createMockConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.GRIDX;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            Assertions.assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            Assertions.assertTrue(this.localityGroups.isEmpty(),
                    "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");

            uut = new ShardTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            this.configuration.put(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, "true");

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GRIDX;

            Assertions.assertEquals(expectedTableType,
                    uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            Assertions.assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            Assertions.assertTrue(this.localityGroups.isEmpty(),
                    "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");
        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGridxTable completed.");
        }
    }

    @Test
    public void testKeepCountOnlyEnabledAndDisabled() throws Exception {
        // We use KeepCountOnlyUidAggregator when enabled
        testKeepCountOnlyConfig(ShardedDataTypeHandler.SHARD_GIDX_TNAME, true, KeepCountOnlyUidAggregator.class);
        testKeepCountOnlyConfig(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, true, KeepCountOnlyUidAggregator.class);

        // We use GlobalIndexUidAggregator when disabled
        testKeepCountOnlyConfig(ShardedDataTypeHandler.SHARD_GIDX_TNAME, false, GlobalIndexUidAggregator.class);
        testKeepCountOnlyConfig(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, false, GlobalIndexUidAggregator.class);
    }

    private void testKeepCountOnlyConfig(String tableProperty, boolean keepCountOnlyIndexEntries, Class<?> expectedAggregatorClass) throws Exception {
        Configuration config = createMockConfiguration();
        Logger log = createMockLogger();
        TableOperations tops = mockUpTableOperations();

        ShardTableConfigHelper uut = new ShardTableConfigHelper();

        this.configuration.put(tableProperty, ShardTableConfigHelperTest.TABLE_NAME);
        this.configuration.put(ShardTableConfigHelper.KEEP_COUNT_ONLY_INDEX_ENTRIES, Boolean.toString(keepCountOnlyIndexEntries));

        this.tableProperties.clear();
        this.localityGroups.clear();

        uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);
        uut.configure(tops);

        Assertions.assertEquals(expectedAggregatorClass.getName(), tableProperties.get("table.iterator.majc.UIDAggregator.opt.*"));
        Assertions.assertEquals(expectedAggregatorClass.getName(), tableProperties.get("table.iterator.minc.UIDAggregator.opt.*"));
        Assertions.assertEquals(expectedAggregatorClass.getName(), tableProperties.get("table.iterator.scan.UIDAggregator.opt.*"));
    }
}