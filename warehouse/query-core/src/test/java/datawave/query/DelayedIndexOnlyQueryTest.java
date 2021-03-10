package datawave.query;

import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests to confirm operation of delayed index evaluation until jexl evaluation
 */
public class DelayedIndexOnlyQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(DelayedIndexOnlyQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexOnlyField(CitiesDataType.CityField.STATE.name());
        generic.addIndexOnlyField(CitiesDataType.CityField.GEO.name());
        
        // remove any pre-defined composites
        for (Set<String> fields : generic.getCompositeFields()) {
            generic.removeCompositeField(fields);
        }
        
        // composite field must be non-index only
        Set<String> compositeFields = new HashSet<>();
        compositeFields.add(CitiesDataType.CityField.CITY.name());
        compositeFields.add(CitiesDataType.CityField.NUM.name());
        generic.addCompositeField(compositeFields);
        
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.generic, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public DelayedIndexOnlyQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
        ((DefaultQueryPlanner) this.logic.getQueryPlanner()).setExecutableExpansion(false);
    }
    
    @Test
    public void testSingleDelay() throws Exception {
        log.info("------  testIndex  ------");
        String query = "CITY == 'rome' && (COUNTRY == 'United States' || STATE == 'ohio')";
        runTest(query, query);
    }
    
    @Test
    public void testMultiDelay() throws Exception {
        log.info("------  testMultiDelay  ------");
        String query = "CITY == 'rome' && (COUNTRY == 'United States' || STATE == 'ohio' || STATE == 'Lazio')";
        runTest(query, query);
    }
    
    @Test
    public void testMultiDelayTrees() throws Exception {
        log.info("------  testMultiDelayTrees  ------");
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || STATE == 'ohio') && (Country == 'United States' || STATE == 'lazio')";
        runTest(query, query);
    }
    
    @Test
    public void testRegexExpansionDelayed() throws Exception {
        log.info("------  testRegexExpansionDelayed  ------");
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || (STATE =~ 'm.*'))";
        runTest(query, query);
    }
    
    // ivarateRegex delayed
    @Test
    public void testExceededValueThresholdRegexDelayed() throws Exception {
        log.info("------  testExceededValueThresholdRegexDelayed  ------");
        logic.setMaxValueExpansionThreshold(1);
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || (STATE =~ 'm.*'))";
        ivaratorConfig();
        runTest(query, query);
    }
    
    // ivarateRange delayed
    @Test
    public void testExceededValueThresholdRangeDelayed() throws Exception {
        log.info("------  testExceededValueThresholdRangeDelayed  ------");
        logic.setMaxValueExpansionThreshold(1);
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || ((_Bounded_ = true) && (STATE > 'm' && STATE < 'n')))";
        ivaratorConfig();
        runTest(query, query);
    }
    
    // ivarateRange delayed, filter converted due to index only field match
    @Test
    public void testExceededValueThresholdRangeDelayedFilterToRENode() throws Exception {
        log.info("------  testExceededValueThresholdRangeDelayedFilterToRENode  ------");
        logic.setMaxValueExpansionThreshold(1);
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || (((_Bounded_ = true) && (STATE > 'm' && STATE < 'n')) || filter:includeRegex(STATE,'Hainaut.*')))";
        String expectedQuery = "CITY == 'rome' && (COUNTRY == 'Italy' || ((STATE > 'm' && STATE < 'n') || STATE =~ 'Hainaut.*'))";
        ivaratorConfig();
        runTest(query, expectedQuery);
    }
    
    // ivarateFilter delayed
    @Test
    public void testExceededValueThresholdDelayedFilterNode() throws Exception {
        log.info("------  testExceededValueThresholdDelayedFilterNode  ------");
        logic.setMaxValueExpansionThreshold(1);
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || geo:within_bounding_box(" + CitiesDataType.CityField.GEO.name() + ",'-90_-180','90_180'))";
        String expectedQuery = "CITY == 'rome'";
        ivaratorConfig();
        runTest(query, expectedQuery);
    }
    
    // ivarateList delayed
    @Test
    public void testExceededOrThesholdDelayed() throws Exception {
        log.info("------  testExceededValueThresholdDelayedFilterNode  ------");
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || STATE == 'mississippi' || STATE == 'ohio')";
        String expectedQuery = "CITY == 'rome' && (COUNTRY == 'Italy' || STATE == 'mississippi' || STATE == 'ohio')";
        ivaratorConfig();
        runTest(query, expectedQuery);
    }
    
    // negated ivarateList delayed
    @Test
    public void testNegatedExceededOrThesholdDelayed() throws Exception {
        log.info("------  testNegatedExceededOrThesholdDelayed  ------");
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        String query = "CITY == 'rome' && !(COUNTRY == 'Italy' || STATE == 'mississippi' || STATE == 'ohio')";
        String expectedQuery = "CITY == 'rome' && !(COUNTRY == 'Italy' || STATE == 'mississippi' || STATE == 'ohio')";
        ivaratorConfig();
        runTest(query, expectedQuery);
    }
    
    // ivarateList fst delayed
    @Test
    public void testExceededOrThesholdFSTDelayed() throws Exception {
        log.info("------  testExceededOrThesholdFSTDelayed  ------");
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        logic.setMaxOrExpansionFstThreshold(1);
        String query = "CITY == 'rome' && (COUNTRY == 'Italy' || STATE == 'mississippi' || STATE == 'ohio')";
        String expectedQuery = "CITY == 'rome' && (COUNTRY == 'Italy' || STATE == 'mississippi' || STATE == 'ohio')";
        ivaratorFstConfig();
        runTest(query, expectedQuery);
    }
    
    // composite index-event-only delayed
    @Test
    public void testCompositeIndexOnlyEventOnlyDelayed() throws Exception {
        log.info("------  testCompositeIndexOnlyEventOnlyDelayed  ------");
        
        String query = "STATE == 'ohio' && ((NUM == '100' && CITY == 'paris') || STATE == 'ohio' || COUNTRY == 'Italy')";
        String expectedQuery = "STATE == 'ohio' && ((NUM == '100' && CITY == 'paris') || STATE == 'ohio' || COUNTRY == 'Italy')";
        runTest(query, expectedQuery);
    }
    
    // composite range index-event-only delayed
    @Test
    public void testCompositeRangeIndexOnlyEventOnlyDelayed() throws Exception {
        log.info("------  testCompositeRangeIndexOnlyEventOnlyDelayed  ------");
        
        String query = "STATE == 'ohio' && ((NUM > '0' && NUM < '200' && CITY == 'paris') || STATE == 'ohio' || COUNTRY == 'Italy')";
        String expectedQuery = "STATE == 'ohio' && ((NUM > '0' && NUM < 200 && CITY == 'paris') || STATE == 'ohio' || COUNTRY == 'Italy')";
        runTest(query, expectedQuery);
    }
}
