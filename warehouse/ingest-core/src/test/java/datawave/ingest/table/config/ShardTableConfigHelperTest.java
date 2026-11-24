package datawave.ingest.table.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.table.aggregator.GlobalIndexUidAggregator;
import datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator;
import datawave.ingest.table.config.ShardTableConfigHelper.ShardTableType;

public class ShardTableConfigHelperTest {

    private static final Logger logger = Logger.getLogger(ShardTableConfigHelperTest.class);
    private static Level testDriverLevel;

    public static final String DEFAULT_EXCEPTION_MESSAGE = "This is a test exception.  NOTHING IS ACTUALLY WRONG....";
    public static final String TABLE_NAME = "UNIT_TEST_TABLE";
    public static final String BAD_TABLE_NAME = "VERY_BAD_TABLE_NAME";

    protected List<String> debugMessages = null;
    protected List<String> infoMessages = null;
    protected Configuration configuration = null;
    public Map<String,String> tableProperties;
    public Map<String,Set<Text>> localityGroups;

    @SuppressWarnings("unchecked")
    public TableOperations mockUpTableOperations() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {

        tableProperties = new HashMap<>();
        localityGroups = new HashMap<>();

        TableOperations mock = mock(TableOperations.class);

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return tableProperties.entrySet();
        }).when(mock).getProperties(nullable(String.class));

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            String name = (String) invocation.getArguments()[1];
            String value = (String) invocation.getArguments()[2];

            tableProperties.put(name, value);
            return null;
        }).when(mock).setProperty(nullable(String.class), nullable(String.class), nullable(String.class));

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.ARE_LOCALITY_GROUPS_CONFIGURED_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return localityGroups;
        }).when(mock).getLocalityGroups(nullable(String.class));

        Map<String,Set<Text>> groups = new HashMap<>();

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, ShardTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            Map<String,Set<Text>> groups1 = (Map<String,Set<Text>>) invocation.getArguments()[1];

            localityGroups.putAll(groups1);

            return null;
        }).when(mock).setLocalityGroups(nullable(String.class), nullable(groups.getClass()));

        // prepare it for use...

        return mock;
    }

    public Configuration createConfiguration() {
        if (configuration == null) {
            configuration = new Configuration(false);
        }
        return configuration;
    }

    protected Logger createMockLogger() {

        Logger log = mock(Logger.class);

        if (null == debugMessages) {

            debugMessages = new ArrayList<>();
        }

        if (null == infoMessages) {

            infoMessages = new ArrayList<>();
        }

        lenient().doAnswer(invocation -> {

            String msg = (String) invocation.getArguments()[0];

            debugMessages.add(msg);

            return null;
        }).when(log).debug(nullable(String.class));

        lenient().doAnswer(invocation -> {

            String msg = (String) invocation.getArguments()[0];

            infoMessages.add(msg);

            return null;
        }).when(log).info(nullable(String.class));

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

            Configuration config = this.createConfiguration();
            Logger log = this.createMockLogger();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

        } catch (IllegalArgumentException iae) {

            String msg = iae.getMessage();

            assertEquals("No Shard Tables Defined", msg,
                            "ShardTableConfigHelper .setup threw the expected exception, but the message was not the expected message.");

        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }

    }

    @Test
    public void testSetupOneShardTableDefinedInConfiguration() {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            Configuration config = this.createConfiguration();
            Logger log = this.createMockLogger();

            this.configuration.set(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.SHARD;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            this.configuration.clear();

            this.configuration.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GIDX;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            this.configuration.clear();

            this.configuration.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GRIDX;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }
    }

    @Test
    public void testSetupOneShardTableDefinedInConfigurationButNotTableNameParameter() {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            Configuration config = this.createConfiguration();
            Logger log = this.createMockLogger();

            this.configuration.set(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(ShardTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                assertTrue(msg.startsWith("Invalid Shard Table Definition For: "),
                                "ShardTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

            this.configuration.clear();
            this.configuration.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(ShardTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                assertTrue(msg.startsWith("Invalid Shard Table Definition For: "),
                                "ShardTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

            this.configuration.clear();
            this.configuration.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(ShardTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                assertTrue(msg.startsWith("Invalid Shard Table Definition For: "),
                                "ShardTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }

    }

    @Test
    public void testConfigureCalledBeforeSetup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureCalledBeforeSetup called.");

        try {

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            TableOperations tops = mockUpTableOperations();

            assertThrows(NullPointerException.class, () -> uut.configure(tops));
        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureCalledBeforeSetup completed.");
        }
    }

    @Test
    public void testConfigureShardTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureShardTable called.");

        try {

            Configuration config = createConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            this.configuration.set(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.SHARD;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertFalse(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Locality Groups collection.");

            uut = new ShardTableConfigHelper();

            this.configuration.set(ShardedDataTypeHandler.SHARD_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            this.configuration.set(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, "true");

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.SHARD;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertFalse(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populae the Locality Groups collection.");
        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureShardTable completed.");
        }
    }

    @Test
    public void testConfigureGidxTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGidxTable called.");

        try {

            Configuration config = createConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            this.configuration.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.GIDX;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertTrue(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");

            uut = new ShardTableConfigHelper();

            this.configuration.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            this.configuration.set(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, "true");

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GIDX;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertTrue(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");
        } finally {

            ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGidxTable completed.");
        }
    }

    @Test
    public void testConfigureGridxTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        ShardTableConfigHelperTest.logger.info("ShardTableConfigHelperTest.testConfigureGridxTable called.");

        try {

            Configuration config = createConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            ShardTableConfigHelper uut = new ShardTableConfigHelper();

            this.configuration.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            ShardTableType expectedTableType = ShardTableType.GRIDX;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertTrue(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");

            uut = new ShardTableConfigHelper();

            this.configuration.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, ShardTableConfigHelperTest.TABLE_NAME);
            this.configuration.set(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, "true");

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);

            expectedTableType = ShardTableType.GRIDX;

            assertEquals(expectedTableType, uut.tableType, "ShardTableConfigHelper.setup incorrectly identified the ShardTableType of the table identified");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "ShardTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertTrue(this.localityGroups.isEmpty(), "ShardTableConfigHelper.configureShardTable caused the Locality Groups collection to be populated.");
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
        Configuration config = createConfiguration();
        Logger log = createMockLogger();
        TableOperations tops = mockUpTableOperations();

        ShardTableConfigHelper uut = new ShardTableConfigHelper();

        this.configuration.set(tableProperty, ShardTableConfigHelperTest.TABLE_NAME);
        this.configuration.set(ShardTableConfigHelper.KEEP_COUNT_ONLY_INDEX_ENTRIES, Boolean.toString(keepCountOnlyIndexEntries));

        this.tableProperties.clear();
        this.localityGroups.clear();

        uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);
        uut.configure(tops);

        assertEquals(expectedAggregatorClass.getName(), tableProperties.get("table.iterator.majc.UIDAggregator.opt.*"));
        assertEquals(expectedAggregatorClass.getName(), tableProperties.get("table.iterator.minc.UIDAggregator.opt.*"));
        assertEquals(expectedAggregatorClass.getName(), tableProperties.get("table.iterator.scan.UIDAggregator.opt.*"));
    }

    @Test
    public void testBitSetCombiner() throws Exception {
        assertBitSetCombinerExists(ShardedDataTypeHandler.SHARD_DAY_INDEX_TABLE_NAME);
        assertBitSetCombinerExists(ShardedDataTypeHandler.SHARD_YEAR_INDEX_TABLE_NAME);
    }

    private void assertBitSetCombinerExists(String tableProperty) throws Exception {
        Configuration config = createConfiguration();
        Logger log = createMockLogger();
        TableOperations tops = mockUpTableOperations();

        ShardTableConfigHelper uut = new ShardTableConfigHelper();

        this.configuration.set(tableProperty, ShardTableConfigHelperTest.TABLE_NAME);

        this.tableProperties.clear();
        this.localityGroups.clear();

        uut.setup(ShardTableConfigHelperTest.TABLE_NAME, config, log);
        uut.configure(tops);

        String expectedIter = "19,datawave.ingest.table.aggregator.BitSetCombiner";
        String expectedOpts = "datawave.ingest.table.aggregator.BitSetCombiner";
        for (IteratorScope scope : IteratorScope.values()) {
            String iterKey = "table.iterator." + scope.name() + ".bits";
            String optsKey = "table.iterator." + scope.name() + ".bits.opt.*";
            assertEquals(expectedIter, tableProperties.get(iterKey));
            assertEquals(expectedOpts, tableProperties.get(optsKey));
        }
    }
}
