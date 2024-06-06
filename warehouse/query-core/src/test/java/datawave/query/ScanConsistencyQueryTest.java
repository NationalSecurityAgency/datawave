package datawave.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.helpers.PrintUtility;
import datawave.microservice.query.QueryImpl;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.util.TableName;

/**
 * Tests for setting and persisting of scan consistency level
 */
public class ScanConsistencyQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(ScanConsistencyQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.NUM.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);
        client = accumuloSetup.loadTables(log);
    }

    public ScanConsistencyQueryTest() {
        super(CitiesDataType.getManager());
    }

    @Test
    public void testSimpleQuery() throws Exception {

        // for now just assert the consistency level on the final batch scanner
        Map<String,ScannerBase.ConsistencyLevel> consistencyLevels = new HashMap<>();
        consistencyLevels.put(TableName.SHARD, ScannerBase.ConsistencyLevel.EVENTUAL);
        logic.setTableConsistencyLevels(consistencyLevels);

        String query = "CITY == 'london'";
        Set<String> expected = Sets.newHashSet("ldn-usa-mi-10", "ldn-uk-7", "ldn-fra-lle-11", "ldn-usa-oh-8", "ldn-usa-mo-8");
        runTest(query, expected);
    }

    @Override
    protected void runTestQuery(Collection<String> expected, String queryStr, Date startDate, Date endDate, Map<String,String> options,
                    List<QueryLogicTestHarness.DocumentChecker> checkers, Set<Authorizations> authSet) throws Exception {

        if (authSet == null || authSet.isEmpty()) {
            authSet = this.authSet;
        }

        QueryImpl q = new QueryImpl();
        q.setBeginDate(startDate);
        q.setEndDate(endDate);
        q.setQuery(queryStr);
        q.setParameters(options);

        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        GenericQueryConfiguration config = this.logic.initialize(client, q, authSet);
        config.setTableConsistencyLevels(Collections.singletonMap(TableName.SHARD, ScannerBase.ConsistencyLevel.IMMEDIATE));

        this.logic.setupQuery(config);

        assertScannerConsistency(this.logic);
    }

    public void assertScannerConsistency(BaseQueryLogic<Entry<Key,Value>> logic) {
        Map<String,ScannerBase.ConsistencyLevel> expected = Collections.singletonMap(TableName.SHARD, ScannerBase.ConsistencyLevel.IMMEDIATE);
        assertEquals(expected, logic.getConfig().getTableConsistencyLevels());
        assertEquals(expected, logic.getTableConsistencyLevels());
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
