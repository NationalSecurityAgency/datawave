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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.handler.dateindex.DateIndexDataTypeHandler;

public class DateIndexTableConfigHelperTest {

    private static final Logger logger = Logger.getLogger(DateIndexTableConfigHelperTest.class);
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

                throw new TableNotFoundException(null, tableNameParameter, DateIndexTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return tableProperties.entrySet();
        }).when(mock).getProperties(nullable(String.class));

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.SET_PROPERTIES_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, DateIndexTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
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

                throw new TableNotFoundException(null, tableNameParameter, DateIndexTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
            }

            return localityGroups;
        }).when(mock).getLocalityGroups(nullable(String.class));

        Map<String,Set<Text>> groups = new HashMap<>();

        lenient().doAnswer(invocation -> {

            AbstractTableConfigHelperTest.SET_LOCALITY_GROUPS_CONFIGURED_CALLED = true;

            String tableNameParameter = (String) invocation.getArguments()[0];

            if (!AbstractTableConfigHelperTest.TABLE_NAME.equals(tableNameParameter)) {

                throw new TableNotFoundException(null, tableNameParameter, DateIndexTableConfigHelperTest.DEFAULT_EXCEPTION_MESSAGE);
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

        Logger log = Logger.getLogger(DateIndexTableConfigHelperTest.class);
        DateIndexTableConfigHelperTest.testDriverLevel = log.getLevel();
        log.setLevel(desiredLevel);
    }

    @AfterEach
    public void teardown() {

        DateIndexTableConfigHelperTest.logger.setLevel(DateIndexTableConfigHelperTest.testDriverLevel);
    }

    @Test
    public void testSetupNothingDefinedInConfiguration() {

        DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            DateIndexTableConfigHelper uut = new DateIndexTableConfigHelper();

            Configuration config = this.createConfiguration();
            Logger log = this.createMockLogger();

            uut.setup(DateIndexTableConfigHelperTest.TABLE_NAME, config, log);

        } catch (IllegalArgumentException iae) {

            String msg = iae.getMessage();

            assertEquals("No DateIndex Table Defined", msg,
                            "DateIndexTableConfigHelper .setup threw the expected exception, but the message was not the expected message.");

        } finally {

            DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }

    }

    @Test
    public void testSetupOneDateIndexTableDefinedInConfiguration() {

        DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            DateIndexTableConfigHelper uut = new DateIndexTableConfigHelper();

            Configuration config = this.createConfiguration();
            Logger log = this.createMockLogger();

            this.configuration.set(DateIndexDataTypeHandler.DATEINDEX_TNAME, DateIndexTableConfigHelperTest.TABLE_NAME);

            uut.setup(DateIndexTableConfigHelperTest.TABLE_NAME, config, log);

            this.configuration.clear();

        } finally {

            DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }
    }

    @Test
    public void testSetupOneDateIndexTableDefinedInConfigurationButNotTableNameParameter() {

        DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testSetupNothingDefinedInConfiguration called.");

        try {

            DateIndexTableConfigHelper uut = new DateIndexTableConfigHelper();

            Configuration config = this.createConfiguration();
            Logger log = this.createMockLogger();

            this.configuration.set(DateIndexDataTypeHandler.DATEINDEX_TNAME, DateIndexTableConfigHelperTest.TABLE_NAME);
            try {

                uut.setup(DateIndexTableConfigHelperTest.BAD_TABLE_NAME, config, log);

            } catch (IllegalArgumentException iae) {

                String msg = iae.getMessage();

                assertTrue(msg.startsWith("Invalid DateIndex Table Definition For: "),
                                "DateIndexTableConfigHelper.setup threw the expected exception, but the message was not the expected message.");
            }

            this.configuration.clear();
        } finally {

            DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testSetupNothingDefinedInConfiguration completed.");
        }

    }

    @Test
    public void testConfigureCalledBeforeSetup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testConfigureCalledBeforeSetup called.");

        try {

            DateIndexTableConfigHelper uut = new DateIndexTableConfigHelper();

            TableOperations tops = mockUpTableOperations();

            assertThrows(TableNotFoundException.class, () -> uut.configure(tops));
        } finally {

            DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testConfigureCalledBeforeSetup completed.");
        }
    }

    @Test
    public void testConfigureDateIndexTable() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testConfigureDateIndexTable called.");

        try {

            Configuration config = createConfiguration();
            Logger log = createMockLogger();
            TableOperations tops = mockUpTableOperations();

            DateIndexTableConfigHelper uut = new DateIndexTableConfigHelper();

            this.configuration.set(DateIndexDataTypeHandler.DATEINDEX_TNAME, DateIndexTableConfigHelperTest.TABLE_NAME);

            this.tableProperties.clear();
            this.localityGroups.clear();

            uut.setup(DateIndexTableConfigHelperTest.TABLE_NAME, config, log);

            uut.configure(tops);

            assertFalse(this.tableProperties.isEmpty(),
                            "DateIndexTableConfigHelper.configureDateIndexTable failed to populate the Table Properties collection.");
            assertFalse(this.localityGroups.isEmpty(), "DateIndexTableConfigHelper.configureDateIndexTable failed to populate the Locality Groups collection.");
        } finally {

            DateIndexTableConfigHelperTest.logger.info("DateIndexTableConfigHelperTest.testConfigureDateIndexTable completed.");
        }
    }

}
