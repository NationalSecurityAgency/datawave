package datawave.query;

import datawave.microservice.querymetric.QueryMetric;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static datawave.query.testframework.RawDataManager.RE_OP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * QueryPlanTest verifies that the query plan is being properly set in the query metrics, even in cases where the query fails during creation.
 */
public class QueryPlanTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(AnyFieldQueryTest.class);
    
    // To be inspected
    private QueryMetric metric;
    
    public QueryPlanTest() {
        super(CitiesDataType.getManager());
    }
    
    @Override
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
    }
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CitiesDataType.CityField.STATE.name());
        generic.addReverseIndexField(CitiesDataType.CityField.STATE.name());
        generic.addReverseIndexField(CitiesDataType.CityField.CONTINENT.name());
        DataTypeHadoopConfig dataType = new CitiesDataType(CitiesDataType.CityEntry.generic, generic);
        accumuloSetup.setData(FileType.CSV, dataType);
        
        connector = accumuloSetup.loadTables(log);
    }
    
    @Before
    public void before() {
        // Use RunningQuery to test that query metrics being updated with plan
        this.useRunningQuery();
        
        // Provide a QueryMetric to test harness to verify it's updated
        metric = new QueryMetric();
        this.withMetric(metric);
    }
    
    @Test
    public void verifyQueryStringUpdatesWithQueryTree() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setQueryString("1");
        assertEquals("1", config.getQueryString());
        config.setQueryTree(JexlASTHelper.parseAndFlattenJexlQuery("A == B"));
        assertEquals("A == B", config.getQueryString());
        config.setQueryTree(JexlASTHelper.parseAndFlattenJexlQuery("B == B"));
        assertEquals("B == B", config.getQueryString());
    }
    
    @Test
    public void planInMetricsAfterInvalidQueryException() throws Exception {
        String query = "species != " + "'dog'";
        String expectedPlan = "!(SPECIES == 'dog')";
        try {
            runTest(query, query);
            fail("Expected InvalidQueryException.");
        } catch (InvalidQueryException e) {
            assertEquals(expectedPlan, metric.getPlan());
        }
    }
    
    @Test
    public void planInMetricsAfterMissingIndexException() throws Exception {
        String query = "CITY == 'london' && CITY != 'london'";
        String expectedPlan = "CITY == 'london' && !(CITY == 'london')";
        this.logic.setIndexTableName("missing");
        try {
            runTest(query, query);
            fail("Expected RuntimeException.");
        } catch (RuntimeException e) {
            assertEquals(expectedPlan, metric.getPlan());
        }
    }
    
    @Test
    public void planInMetricsAfterTableNotFoundException() throws Exception {
        String query = Constants.ANY_FIELD + " != " + "'" + TestCities.london + "'";
        String expectedPlan = "!(_ANYFIELD_ == 'london')";
        
        this.logic.setMetadataTableName("missing");
        try {
            runTest(query, query);
            fail("Expected DatawaveFatalQueryException.");
        } catch (DatawaveFatalQueryException e) {
            assertEquals(expectedPlan, metric.getPlan());
        }
    }
    
    @Test
    public void planInMetricsAfterFTSDException() throws Exception {
        String query = Constants.ANY_FIELD + " != " + "'" + TestCities.london + "'";
        String expectedPlan = "(((!(_ANYFIELD_ == 'london') && !(CITY == 'london') && !(STATE == 'london'))))";
        try {
            runTest(query, query);
            fail("Expected FullTableScanDisallowedException.");
        } catch (FullTableScansDisallowedException e) {
            assertEquals(expectedPlan, metric.getPlan());
        }
    }
    
    @Test
    public void planInMetricsAfterDNPOQException() throws Exception {
        String query = Constants.ANY_FIELD + RE_OP + "'.*iss.*'";
        String expectedPlan = query;
        
        try {
            runTest(query, query);
            fail("Expected DoNotPerformOptimizedQueryException.");
        } catch (DoNotPerformOptimizedQueryException e) {
            assertEquals(expectedPlan, metric.getPlan());
        }
    }
}
