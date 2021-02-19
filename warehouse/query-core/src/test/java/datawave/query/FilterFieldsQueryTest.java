package datawave.query;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.QueryJexl;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.testframework.ResponseFieldChecker;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

/**
 * Performs query test where specific returned fields are specified setting the {@link QueryParameters#RETURN_FIELDS} and
 * {@link QueryParameters#BLACKLISTED_FIELDS} parameter.
 */
public class FilterFieldsQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(FilterFieldsQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public FilterFieldsQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testEqCityAndEqState() throws Exception {
        log.info("------  testEqCityAndEqContinent  ------");
        
        for (final TestCities city : TestCities.values()) {
            String cont = "'ohio'";
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + EQ_OP + cont;
            runTest(query, true, false);
        }
    }
    
    @Test
    public void testEqCityAndEqContinentHitList() throws Exception {
        log.info("------  testEqCityAndEqContinentHitList  ------");
        
        String cont = "'north america'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont;
            runTest(query, true, true);
        }
    }
    
    @Test
    public void testAnyField() throws Exception {
        log.info("------  testAnyField  ------");
        
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String query = Constants.ANY_FIELD + cityPhrase;
            String expect = this.dataManager.convertAnyField(cityPhrase);
            runTest(query, expect, true, false);
        }
    }
    
    @Test
    public void testDisjunctionAnyField() throws Exception {
        log.info("------  testDisjunctionAnyField  ------");
        String noMatchPhrase = EQ_OP + "'no-match-found'";
        String nothingPhrase = EQ_OP + "'nothing-here'";
        String query = Constants.ANY_FIELD + noMatchPhrase + OR_OP + Constants.ANY_FIELD + nothingPhrase;
        String anyNoMatch = this.dataManager.convertAnyField(noMatchPhrase);
        String anyNothing = this.dataManager.convertAnyField(nothingPhrase);
        String expect = anyNoMatch + OR_OP + anyNothing;
        runTest(query, expect, true, false);
    }
    
    @Test
    public void testDisjunctionAnyField_withMatch() throws Exception {
        log.info("------  testDisjunctionAnyField with a matching phrase ------");
        String noMatchPhrase = EQ_OP + "'no-match-found'";
        String matchingPhrase = EQ_OP + "'rome'";
        String query = Constants.ANY_FIELD + noMatchPhrase + OR_OP + Constants.ANY_FIELD + matchingPhrase;
        String anyNoMatch = this.dataManager.convertAnyField(noMatchPhrase);
        String anyMatch = this.dataManager.convertAnyField(matchingPhrase);
        String expect = anyNoMatch + OR_OP + anyMatch;
        runTest(query, expect, true, false);
    }
    
    @Test
    public void testConjunctionAnyField_noMatch() throws Exception {
        log.info("------  testConjunctionAnyField with no matching phrases ------");
        String noMatchPhrase = EQ_OP + "'no-match-found'";
        String nothingPhrase = EQ_OP + "'nothing-here'";
        String query = Constants.ANY_FIELD + noMatchPhrase + AND_OP + Constants.ANY_FIELD + nothingPhrase;
        String anyNoMatch = this.dataManager.convertAnyField(noMatchPhrase);
        String anyNothing = this.dataManager.convertAnyField(nothingPhrase);
        String expect = anyNoMatch + AND_OP + anyNothing;
        runTest(query, expect, true, false);
    }
    
    @Test
    public void testConjunctionAnyField_withMatch() throws Exception {
        log.info("------  testConjunctionAnyField with a matching phrase ------");
        String noMatchPhrase = " == 'no-match-found'";
        String matchingPhrase = " == 'rome'";
        String op = " and ";
        String query = Constants.ANY_FIELD + noMatchPhrase + op + Constants.ANY_FIELD + matchingPhrase;
        String anyNoMatch = this.dataManager.convertAnyField(noMatchPhrase);
        String anyMatch = this.dataManager.convertAnyField(matchingPhrase);
        String expect = anyNoMatch + op + anyMatch;
        runTest(query, expect, true, false);
    }
    
    @Test
    public void testAnyFieldFilterIncludeRegex() throws Exception {
        log.info("------  testAnyFieldFilterIncludeRegex  ------");
        String state = "'ohio'";
        String phrase = EQ_OP + state;
        String anyState = this.dataManager.convertAnyField(phrase);
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + " filter:includeRegex(" + Constants.ANY_FIELD + "," + state + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + anyState;
            runTest(query, expectQuery, true, false);
        }
    }
    
    @Test
    public void testAnyFieldLuceneInclude() throws Exception {
        log.info("------  testAnyFieldLuceneInclude  ------");
        String state = "ohio";
        String anyState = this.dataManager.convertAnyField(EQ_OP + "'" + state + "'");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + ":" + city.name() + AND_OP + " #INCLUDE(" + Constants.ANY_FIELD + ",ohio)";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + anyState;
            this.logic.setParser(new LuceneToJexlQueryParser());
            runTest(query, expectQuery, true, false);
        }
    }
    
    @Test
    public void testAnyFieldLuceneText() throws Exception {
        log.info("------  testAnyFieldLuceneText  ------");
        String state = "ohio";
        String anyState = this.dataManager.convertAnyField(EQ_OP + "'" + state + "'");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + ":" + city.name() + AND_OP + " #TEXT(Ohio)";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + anyState;
            this.logic.setParser(new LuceneToJexlQueryParser());
            runTest(query, expectQuery, true, false);
        }
    }
    
    @Test
    public void testOccurrenceFunction() throws Exception {
        log.info("------  testOccurrenceFunction  ------");
        String cont = "'europe'";
        String query = CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + " filter:occurrence(" + CityField.CONTINENT.name() + ",'<', 2)";
        String expectQuery = CityField.CONTINENT.name() + EQ_OP + cont;
        runTest(query, expectQuery, true, false);
    }
    
    @Test
    public void testZeroOccurrenceFunction() throws Exception {
        log.info("------  testZeroOccurrenceFunction  ------");
        String cont = "'europe'";
        String query = CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + " filter:occurrence(" + CityField.CONTINENT.name() + ",'>', 1)";
        String expectQuery = CityField.CONTINENT.name() + EQ_OP + "'no-such-name'";
        runTest(query, expectQuery, true, false);
    }
    
    @Test
    public void testBlackListMultiValueIncluded() throws Exception {
        log.info("------  testBlackListMultiValueIncluded  ------");
        
        String cont = "'europe'";
        String state = "'mississippi'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP
                            + CityField.STATE.name() + EQ_OP + state + ")";
            final Set<String> fields = CityField.getRandomReturnFields(false);
            // remove CITY field
            fields.remove(CityField.CITY.name());
            runTest(query, false, false, fields);
        }
    }
    
    @Test
    public void testBlackListMultiValueExcluded() throws Exception {
        log.info("------  testBlackListMultiValueExcluded  ------");
        
        String cont = "'europe'";
        String state = "'mississippi'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP
                            + CityField.STATE.name() + EQ_OP + state + ")";
            final Set<String> fields = CityField.getRandomReturnFields(false);
            // include CITY field
            fields.add(CityField.CITY.name());
            runTest(query, false, false, fields);
        }
    }
    
    @Test
    public void testEqCityAndEqContinentBlackList() throws Exception {
        log.info("------  testEqCityAndEqContinentBlackList  ------");
        
        String state = "'ohio'";
        String mizzu = "'missouri'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + EQ_OP + state + OR_OP
                            + CityField.STATE.name() + EQ_OP + mizzu + ")";
            runTest(query, false, false);
        }
    }
    
    @Test
    public void testRegexBlacklist() throws Exception {
        log.info("------  testRegexBlacklist  ------");
        
        String regex = "'miss.*'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, false, false);
        }
    }
    
    @Test
    public void testRegexWhitelist() throws Exception {
        log.info("------  testRegexWhitelist  ------");
        
        String regex = "'miss.*'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, true, false);
        }
    }
    
    @Test
    public void testEqCityAndEqContinentBlackListWithHitList() throws Exception {
        log.info("------  testEqCityAndEqContinentBlackListWithHitList  ------");
        
        String cont = "'europe'";
        String state = "'missouri'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + EQ_OP + state + OR_OP
                            + CityField.CONTINENT.name() + EQ_OP + cont + ")";
            runTest(query, false, true);
        }
    }
    
    @Test
    public void testWhiteListWithMultiValueIncluded() throws Exception {
        log.info("------  testWhiteListWithMultiValueIncluded  ------");
        
        String cont = "'north america'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont;
            final Set<String> fields = CityField.getRandomReturnFields(true);
            // include STATE field
            fields.add(CityField.STATE.name());
            runTest(query, true, true, fields);
        }
    }
    
    @Test
    public void testWhiteListWithMultiValueExcluded() throws Exception {
        log.info("------  testWhiteListWithMultiValueExcluded  ------");
        
        String cont = "'north america'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont;
            final Set<String> fields = CityField.getRandomReturnFields(true);
            // remove STATE field
            fields.remove(CityField.STATE.name());
            runTest(query, true, true, fields);
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
    
    private void runTest(final String query, final String expectQuery, final boolean whiteList, final boolean hitList) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        final Set<String> fields = CityField.getRandomReturnFields(whiteList);
        runTest(query, expectQuery, startEndDate[0], startEndDate[1], whiteList, hitList, fields);
    }
    
    private void runTest(final String query, final boolean whiteList, final boolean hitList) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        final Set<String> fields = CityField.getRandomReturnFields(whiteList);
        runTest(query, query, startEndDate[0], startEndDate[1], whiteList, hitList, fields);
    }
    
    private void runTest(final String query, final boolean whiteList, final boolean hitList, Set<String> fields) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTest(query, query, startEndDate[0], startEndDate[1], whiteList, hitList, fields);
    }
    
    /**
     * Base helper method for execution of a unit test.
     *
     * @param query
     *            query string for execution
     * @param expectQuery
     *            query string to use to calculate expected results
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
    private void runTest(final String query, final String expectQuery, final Date startDate, final Date endDate, final boolean whiteList,
                    final boolean hitList, final Set<String> fields) throws Exception {
        
        QueryJexl jexl = new QueryJexl(expectQuery, this.dataManager, startDate, endDate);
        final Set<Map<String,String>> allData = jexl.evaluate();
        final Set<String> expected = this.dataManager.getKeys(allData);
        
        final Set<String> otherFields = new HashSet<>(this.dataManager.getHeaders());
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
