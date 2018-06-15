package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.CitiesDataType.CityShardId;
import datawave.query.testframework.IDataTypeHadoopConfig;
import datawave.query.testframework.IRawData;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.testframework.QueryParserHelper;
import datawave.query.testframework.ResponseFieldChecker;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs query test where specific returned fields are specified setting the {@link QueryParameters#RETURN_FIELDS} and
 * {@link QueryParameters#BLACKLISTED_FIELDS} parameter.
 */
public class FilterFieldsQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(FilterFieldsQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<IDataTypeHadoopConfig> dataTypes = new ArrayList<>();
        for (final CitiesDataType.CityEntry entry : CitiesDataType.CityEntry.values()) {
            dataTypes.add(new CitiesDataType(entry));
        }
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public FilterFieldsQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testEqCityAndEqContinent() throws Exception {
        log.debug("------  testEqCityAndEqCountry  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String cont = "north america";
            String query = CityField.CITY.name() + " == '" + city + "' and " + CityField.CONTINENT.name() + " == '" + cont + "'";
            runTest(query, true, false);
        }
    }
    
    @Test
    public void testEqCityAndEqContinentBlackList() throws Exception {
        log.debug("------  testEqCityAndEqContinentBlackList  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String cont = "europe";
            String query = CityField.CITY.name() + " == '" + city + "' and " + CityField.CONTINENT.name() + " == '" + cont + "'";
            runTest(query, false, false);
        }
    }
    
    @Test
    public void testEqCityAndEqContinentHitList() throws Exception {
        log.debug("------  testEqCityAndEqContinentHitList  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String cont = "europe";
            String query = CityField.CITY.name() + " == '" + city + "' and " + CityField.CONTINENT.name() + " == '" + cont + "'";
            runTest(query, true, true);
        }
    }
    
    @Test
    public void testAnyField() throws Exception {
        log.debug("------  testAnyField  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String query = Constants.ANY_FIELD + " == '" + city + "'";
            runTest(query, true, false);
        }
    }
    
    @Test
    public void testWhiteListWithMultiValueIncluded() throws Exception {
        log.debug("------  testWhiteListWithMultiValueIncluded  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String cont = "europe";
            String query = CityField.CITY.name() + " == '" + city + "' and " + CityField.CONTINENT.name() + " == '" + cont + "'";
            final Set<String> fields = CityField.getRandomReturnFields(true);
            // include STATE field
            fields.add(CityField.STATE.name());
            runTest(query, true, true, fields);
        }
    }
    
    @Test
    public void testWhiteListWithMultiValueExcluded() throws Exception {
        log.debug("------  testWhiteListWithMultiValueExcluded  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String cont = "europe";
            String query = CityField.CITY.name() + " == '" + city + "' and " + CityField.CONTINENT.name() + " == '" + cont + "'";
            final Set<String> fields = CityField.getRandomReturnFields(true);
            // remove STATE field
            fields.remove(CityField.STATE.name());
            runTest(query, true, true, fields);
        }
    }
    
    @Ignore
    // TODO WIP
    @Test
    public void testWhiteListWithMultiValue() throws Exception {
        log.debug("------  testWhiteListWithMultiValueIncluded  ------");
        
        for (final CityEntry city : CityEntry.values()) {
            String state = "texas";
            // "oregon";
            String s2 = "oregon";
            Date startDate = CityShardId.DATE_2015_0707.date();
            Date endDate = CityShardId.DATE_2015_0909.date();
            String query = CityField.CITY.name() + " == '" + city + "'" + " and " +
            // String query =
            // "(" +
                            CityField.STATE.name() + " == '" + state + "'"
            // + " OR " +
            // CityField.STATE.name() + " == '" + s2 + "')"
            ;
            runTest(query, startDate, endDate, true, true);
        }
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
    
    // ============================================
    // private methods
    
    private void runTest(final String query, final boolean whiteList, final boolean hitList) throws Exception {
        Date[] startEndDate = this.dataManager.getRandomStartEndDate();
        final Set<String> fields = CityField.getRandomReturnFields(whiteList);
        runTest(query, startEndDate[0], startEndDate[1], whiteList, hitList, fields);
    }
    
    private void runTest(final String query, final Date startDate, final Date endDate, final boolean whiteList, final boolean hitList) throws Exception {
        final Set<String> fields = CityField.getRandomReturnFields(whiteList);
        runTest(query, startDate, endDate, whiteList, hitList, fields);
    }
    
    private void runTest(final String query, final boolean whiteList, final boolean hitList, Set<String> fields) throws Exception {
        Date[] startEndDate = this.dataManager.getRandomStartEndDate();
        runTest(query, startEndDate[0], startEndDate[1], whiteList, hitList, fields);
    }
    
    /**
     * Base helper method for execution of a unit test.
     *
     * @param query
     *            query string for execution
     * @param startDate
     *            start date of query
     * @param endDate
     *            end date of query
     * @param whiteList
     *            true to return specific fields; false to specify blacklist fields
     * @param hitList
     *            when true the option {@link QueryParameters#HIT_LIST} is set to true
     * @param fields
     *            return fields or blacklist fields
     * @throws Exception
     *             something failed - go figure it out
     */
    private void runTest(final String query, final Date startDate, final Date endDate, final boolean whiteList, final boolean hitList, final Set<String> fields)
                    throws Exception {
        QueryParserHelper queryHelper = new QueryParserHelper(query, this.dataManager, startDate, endDate);
        final Set<IRawData> allData = queryHelper.findMatchers();
        final Set<String> expected = this.dataManager.getKeyField(allData);
        
        // TODO - temp
        fields.add(CityField.STATE.name());
        final Set<String> otherFields = new HashSet<>(CityField.headers());
        otherFields.removeAll(fields);
        
        final String queryFields = String.join(",", fields);
        
        Map<String,String> options = new HashMap<>();
        final List<QueryLogicTestHarness.DocumentChecker> queryChecker = new ArrayList<>();
        if (whiteList) {
            // NOTE CityField.EVENT_ID MUST be included in blacklisted fields
            options.put(QueryParameters.RETURN_FIELDS, queryFields);
            queryChecker.add(new ResponseFieldChecker(fields, otherFields));
        } else {
            // NOTE CityField.EVENT_ID CANNOT be included in blacklisted fields
            options.put(QueryParameters.BLACKLISTED_FIELDS, queryFields);
            queryChecker.add(new ResponseFieldChecker(otherFields, fields));
        }
        if (hitList) {
            options.put(QueryParameters.HIT_LIST, "true");
        }
        
        runTestQuery(expected, query, startDate, endDate, options, queryChecker);
    }
}
