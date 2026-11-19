package datawave.ingest.table.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

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

        TableOperations mock = mock(TableOperations.class);

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.GET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return tableProperties.entrySet();
        }).when(mock).getProperties(nullable(String.class));

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
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

                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return localityGroups;
        }).when(mock).getLocalityGroups(nullable(String.class));

        Map<String,Set<Text>> groups = new HashMap<>();

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, MetadataTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            Map<String,Set<Text>> groups1 = (Map<String,Set<Text>>) invocation.getArguments()[1];

            localityGroups.putAll(groups1);

            return null;
        }).when(mock).setLocalityGroups(nullable(String.class), nullable(groups.getClass()));

        // prepare it for use...

        return mock;
    }

    public Configuration createMockConfiguration() {

        if (null == configuration) {

            configuration = new HashMap<>();
        }

        Configuration mock = mock(Configuration.class);

        lenient().doAnswer(invocation -> {

            String results = null;
            String key = (String) invocation.getArguments()[0];
            String defaultValue = (String) invocation.getArguments()[1];

            if (configuration.containsKey(key)) {

                results = configuration.get(key);
            } else {

                results = defaultValue;
            }

            return results;
        }).when(mock).get(nullable(String.class), nullable(String.class));

        lenient().doAnswer(invocation -> {

            String key = (String) invocation.getArguments()[0];
            boolean results = (Boolean) invocation.getArguments()[1];

            if (configuration.containsKey(key)) {

                results = Boolean.parseBoolean(configuration.get(key));
            }

            return results;
        }).when(mock).getBoolean(nullable(String.class), anyBoolean());

        return mock;
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

        Logger log = Logger.getLogger(MetadataTableConfigHelperTest.class);
        MetadataTableConfigHelperTest.testDriverLevel = log.getLevel();
        log.setLevel(desiredLevel);
    }

    @AfterEach
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

            assertTrue(msg.startsWith("No Such Table: "),
                            "MetadataTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
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

            assertEquals(MetadataTableConfigHelperTest.TABLE_NAME, uut.exposeTableName(),
                            "MetadataTableConfigHelper.setup incorrectly identified the TableName");

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

                assertTrue(msg.startsWith("No Such Table: "),
                                "MetadataTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
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

            assertTrue(this.tableProperties.isEmpty(), "MetadataTableConfigHelper.configure incorrectly populated the Table Properties.");

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

            assertEquals(MetadataTableConfigHelperTest.TABLE_NAME, uut.exposeTableName(),
                            "MetadataTableConfigHelper.setup incorrectly identified the TableName");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "MetadataTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertTrue(this.localityGroups.isEmpty(),
                            "MetadataTableConfigHelper.configureShardTable caused the the Locality Groups collection to be populated.");

            uut = new WrappedMetadataTableConfigHelper();

            this.configuration.put(ShardedDataTypeHandler.METADATA_TABLE_NAME, MetadataTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(MetadataTableConfigHelperTest.TABLE_NAME, config, log);

            assertEquals(MetadataTableConfigHelperTest.TABLE_NAME, uut.exposeTableName(),
                            "MetadataTableConfigHelper.setup incorrectly identified the TableName");

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(), "MetadataTableConfigHelper.configureShardTable failed to populate the Table Properties collection.");
            assertTrue(this.localityGroups.isEmpty(),
                            "MetadataTableConfigHelper.configureShardTable caused the the Locality Groups collection to be populated.");

        } finally {

            MetadataTableConfigHelperTest.logger.info("MetadataTableConfigHelperTest.testConfigureShardTable completed.");
        }
    }

}
