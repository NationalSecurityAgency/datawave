package datawave.query;

import static datawave.query.testframework.CitiesDataType.CityEntry;
import static datawave.query.testframework.CitiesDataType.CityField;
import static datawave.query.testframework.CitiesDataType.getManager;
import static datawave.query.testframework.CitiesDataType.getTestAuths;
import static datawave.query.testframework.RawDataManager.EQ_OP;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.QueryImpl;
import datawave.query.iterator.QueryLogIterator;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;

public class TserverLoggingTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    private static final Logger log = Logger.getLogger(TserverLoggingTest.class);

    private final Set<Authorizations> authSet = Collections.singleton(getTestAuths());

    @BeforeClass
    public static void filterSetup() throws Exception {
        FieldConfig generic = new GenericCityFields();
        Set<String> comp = new HashSet<>();
        comp.add(CityField.CITY.name());
        comp.add(CityField.COUNTRY.name());
        generic.addCompositeField(comp);
        generic.addIndexField(CityField.COUNTRY.name());
        generic.addIndexOnlyField(CityField.STATE.name());

        accumuloSetup.setData(FileType.CSV, new CitiesDataType(CityEntry.generic, generic));
        client = accumuloSetup.loadTables(log);
    }

    public TserverLoggingTest() {
        super(getManager());
    }

    @After
    public void tearDown() throws Exception {
        this.logic = null;
    }

    /**
     * Verify that when tserver logging active is set to false via the config, that the {@link QueryLogIterator} is not added to the iterator stack.
     */
    @Test
    public void testTserverLoggingActiveFalseConfig() throws Exception {
        logic.getConfig().setTserverLoggingActive(false);

        initializeQuery(Collections.emptyMap());

        assertQueryLogIteratorPresence(false);
    }

    /**
     * Verify that when tserver logging active is set to true via the config, that the {@link QueryLogIterator} is added to the iterator stack.
     */
    @Test
    public void testTserverLoggingActiveTrueConfig() throws Exception {
        logic.getConfig().setTserverLoggingActive(true);

        initializeQuery(Collections.emptyMap());

        assertQueryLogIteratorPresence(true);
    }

    /**
     * Verify that when tserver logging active is set to false via the query parameters, that the {@link QueryLogIterator} is not added to the iterator stack.
     */
    @Test
    public void testTserverLoggingActiveFalseOption() throws Exception {
        Map<String,String> options = new HashMap<>();
        options.put(QueryParameters.TSERVER_LOGGING_ACTIVE, "false");

        initializeQuery(options);

        assertQueryLogIteratorPresence(false);
    }

    /**
     * Verify that when tserver logging active is set to true via the query parameters, that the {@link QueryLogIterator} is added to the iterator stack.
     */
    @Test
    public void testTserverLoggingActiveTrueOption() throws Exception {
        Map<String,String> options = new HashMap<>();
        options.put(QueryParameters.TSERVER_LOGGING_ACTIVE, "true");

        initializeQuery(options);

        assertQueryLogIteratorPresence(true);
    }

    /**
     * Verify that when tserver logging active is set to false via the config, but overridden by the query parameters with a value of true, that the
     * {@link QueryLogIterator} is added to the iterator stack.
     */
    @Test
    public void testQueryParameterOverridesConfigForValueOfTrue() throws Exception {
        logic.getConfig().setTserverLoggingActive(false);
        Map<String,String> options = new HashMap<>();
        options.put(QueryParameters.TSERVER_LOGGING_ACTIVE, "true");

        initializeQuery(options);

        assertQueryLogIteratorPresence(true);
    }

    /**
     * Verify that when tserver logging active is set to false via the config, but overridden by the query parameters with a value of true, that the
     * {@link QueryLogIterator} is not added to the iterator stack.
     */
    @Test
    public void testQueryParameterOverridesConfigForValueOfFalse() throws Exception {
        logic.getConfig().setTserverLoggingActive(true);
        Map<String,String> options = new HashMap<>();
        options.put(QueryParameters.TSERVER_LOGGING_ACTIVE, "false");

        initializeQuery(options);

        assertQueryLogIteratorPresence(false);
    }

    private void initializeQuery(Map<String,String> options) throws Exception {
        String query = CityField.STATE.name() + EQ_OP + "'ohio'";

        QueryImpl q = new QueryImpl();
        q.setBeginDate(this.dataManager.getShardStartEndDate()[0]);
        q.setEndDate(this.dataManager.getShardStartEndDate()[1]);
        q.setQuery(query);
        q.setParameters(options);

        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());
        this.logic.initialize(client, q, authSet);
    }

    private void assertQueryLogIteratorPresence(boolean presenceRequired) {
        // Fetch the query data.
        Iterator<QueryData> iterator = this.logic.getConfig().getQueriesIter();
        while (iterator.hasNext()) {
            QueryData queryData = iterator.next();
            Assert.assertEquals(presenceRequired, isQueryLogIteratorPresent(queryData));
        }
    }

    private boolean isQueryLogIteratorPresent(QueryData data) {
        return data.getSettings().stream().map(IteratorSetting::getIteratorClass).anyMatch(c -> c.equals(QueryLogIterator.class.getName()));
    }

    @Override
    protected void testInit() {
        this.auths = getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
