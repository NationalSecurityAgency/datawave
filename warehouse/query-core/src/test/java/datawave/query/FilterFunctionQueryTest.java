package datawave.query;

import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;

public class FilterFunctionQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(FilterFunctionQueryTest.class);
    
    private static final String IncludeRegex = "filter:includeRegex(";
    private static final String ExcludeRegex = FILTER_EXCLUDE_REGEX + "(";
    private static final String GetAllMatches = "filter:getAllMatches(";
    private static final String IsNull = "filter:isNull(";
    private static final String IsNotNull = "filter:isNotNull(";
    private static final String MatchesAtLeastCountOf = "filter:matchesAtLeastCountOf(";
    private static final String Occurrence = "filter:occurrence(";
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.CODE.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        dataTypes.add(new CitiesDataType(CityEntry.nullState, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public FilterFunctionQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    // =============================================
    // test cases
    @Test
    public void testIncludeRegex() throws Exception {
        log.info("------  testIncludeRegex  ------");
        
        String state = "'ohio'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + IncludeRegex + CityField.STATE.name() + ","
                            + state + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + state;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testIncludeRegexWithNumeric() throws Exception {
        log.info("------  testIncludeRegexWithNumeric  ------");
        
        String num = "'110'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + IncludeRegex + CityField.STATE.name() + "||"
                            + CityField.NUM.name() + "," + num + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + RE_OP + num + " or "
                            + CityField.NUM.name() + RE_OP + num + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testIncludeRegexAnyField() throws Exception {
        log.info("------  testIncludeRegexAnyField  ------");
        
        String usa = "'usa'";
        String anyRegex = this.dataManager.convertAnyField(RE_OP + usa);
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + IncludeRegex + Constants.ANY_FIELD + "," + usa + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + anyRegex;
            runTest(query, expectQuery);
        }
    }
    
    // Order of fields should not affect the number of results
    @Test
    public void testExerciseBugWithHowOrNodesAreHandled() throws Exception {
        String orig = "CITY == 'london' and filter:includeRegex(STATE||NUM,'110')";
        String next = "CITY == 'london' and (STATE =~ '110' or NUM =~ '110')";
        runTest(orig, next);
        
        orig = "CITY == 'london' and filter:includeRegex(NUM||STATE,'110')";
        next = "CITY == 'london' and (NUM =~ '110' or STATE =~ '110')";
        runTest(orig, next);
    }
    
    @Test
    public void testIncludeRegexWildCard() throws Exception {
        log.info("------  testIncludeRegexWildCard  ------");
        
        String code = "'uSa'";
        String regex = "'.*o.*'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + IncludeRegex + CityField.CITY.name() + "," + regex + ")";
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.CITY.name() + RE_OP + regex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testNotIncludeRegexWildCard() throws Exception {
        log.info("------  testNotIncludeRegexWildCard  ------");
        
        String code = "'uSa'";
        String regex = "'.*o.*'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + "not " + IncludeRegex + CityField.CITY.name() + "," + regex + ")";
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + " not (" + CityField.CITY.name() + RE_OP + regex + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testIncludeRegexWildCardAnyField() throws Exception {
        log.info("------  testIncludeRegexWildCardAnyField  ------");
        
        String code = "'uSa'";
        String regex = "'.*isS.*'";
        
        String query = CityField.CODE.name() + EQ_OP + code + AND_OP + IncludeRegex + Constants.ANY_FIELD + "," + regex + ")";
        String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.STATE.name() + RE_OP + regex;
        runTest(query, expectQuery);
        
    }
    
    @Test
    public void testExcludeRegex() throws Exception {
        log.info("------  testExcludeRegex  ------");
        
        String code = "'uSa'";
        String regex = "'ohio'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + ExcludeRegex + CityField.CITY.name() + "," + regex + ")";
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.CITY.name() + RN_OP + regex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testExcludeRegexAnyField() throws Exception {
        log.info("------  testExcludeRegexAnyField  ------");
        
        String usa = "'usa'";
        String anyRegex = this.dataManager.convertAnyField(RN_OP + usa, AND_OP);
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + ExcludeRegex + Constants.ANY_FIELD + "," + usa + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + anyRegex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testExcludeRegexWildCard() throws Exception {
        log.info("------  testExcludeRegexWildCard  ------");
        
        String code = "'uSa'";
        String regex = "'.*o.*'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + ExcludeRegex + CityField.CITY.name() + "," + regex + ")";
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.CITY.name() + RN_OP + regex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testRegexMultiWildCard() throws Exception {
        log.info("------  testRegexMultiWildCard  ------");
        
        String code = "'uSa'";
        String regex = "'m.*s.*'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.STATE.name() + RE_OP + regex;
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testGetAllMatches() throws Exception {
        log.info("------  testGetAllMatches  ------");
        
        String regex = "'mi.*'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + GetAllMatches + CityField.STATE.name() + "," + regex + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testIsNullInvalidField() throws Exception {
        log.info("------  testIsNullInvalidField  ------");
        
        String empty = "''";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + IsNull + CityField.STATE.name() + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + CityField.STATE + EQ_OP + empty;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testIsNull() throws Exception {
        log.info("------  testIsNull  ------");
        
        String empty = "''";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + IsNull + CityField.STATE.name() + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + CityField.STATE + EQ_OP + empty;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testAnyFieldIsNull() throws Exception {
        log.info("------  testAnyFieldIsNull  ------");
        
        String query = "(" + Constants.ANY_FIELD + EQ_OP + "'none' or " + Constants.ANY_FIELD + EQ_OP + "'none again') and not(" + IsNull + ""
                        + CityField.ACCESS.name() + "))";
        String expectQuery = CityField.CODE.name() + EQ_OP + "'xxx'";
        runTest(query, expectQuery);
    }
    
    @Test
    public void testIsNotNull() throws Exception {
        log.info("------  testIsNotNull  ------");
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + IsNotNull + CityField.CONTINENT.name() + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city + "'" + "" + AND_OP + CityField.CONTINENT.name() + NE_OP + "''";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testIsNotNullState() throws Exception {
        log.info("------  testIsNotNullState  ------");
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + IsNotNull + CityField.STATE.name() + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + CityField.STATE.name() + NE_OP + "''";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testMatchesCountValid() throws Exception {
        log.info("------  testMatchesCountValid  ------");
        
        String cities = "'" + StringUtils.join(TestCities.values(), "','") + "'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + MatchesAtLeastCountOf + "'1','" + city.name() + "'," + cities
                            + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testMatchesCountInvalid() throws Exception {
        log.info("------  testMatchesCountInvalid  ------");
        
        String cities = "'" + StringUtils.join(TestCities.values(), "','") + "'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + MatchesAtLeastCountOf + "'" + "2" + "','" + city.name() + "',"
                            + cities + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'none'";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testOccurrence() throws Exception {
        log.info("------  testOccurrence  ------");
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + Occurrence + CityField.CITY.name() + ",'<', 2)";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + "";
            runTest(query, expectQuery);
        }
    }
    
    // ============================================
    // error conditions
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorFilterOnly() throws Exception {
        log.info("------  testFilterOnly  ------");
        
        String state = "'.+iSs.+'";
        
        String query = IncludeRegex + CityField.STATE.name() + "," + state + ")";
        String expectQuery = CityField.STATE.name() + RE_OP + state;
        runTest(query, expectQuery);
    }
    
    @Test(expected = InvalidQueryException.class)
    public void testErrorIndexDatatype() throws Exception {
        log.info("------  testErrorDatatype  ------");
        
        String query = CityField.CODE.name() + RE_OP + "'ab.*?'" + AND_OP + IncludeRegex + "NONE, 'xx') " + AND_OP + IsNotNull + CityField.STATE.name() + ")";
        String expectQuery = CityField.CODE.name() + EQ_OP + "'xxx'";
        runTest(query, expectQuery);
    }
    
    @Test(expected = InvalidQueryException.class)
    public void testErrorNoIndexDatatype() throws Exception {
        log.info("------  testErrorNoIndexDatatype  ------");
        
        String query = "Unindex" + RE_OP + "'ab.*?'" + AND_OP + IncludeRegex + "NONE, 'ab') " + AND_OP + IsNotNull + CityField.STATE.name() + ")";
        String expectQuery = CityField.CODE.name() + EQ_OP + "'xxx'";
        runTest(query, expectQuery);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
    
    // ============================================
    // private methods
}
