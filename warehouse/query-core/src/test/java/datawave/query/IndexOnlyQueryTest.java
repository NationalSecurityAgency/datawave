package datawave.query;

import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class IndexOnlyQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(IndexOnlyQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        FieldConfig generic = new GenericCityFields();
        Set<String> comp = new HashSet<>();
        comp.add(CityField.CITY.name());
        comp.add(CityField.COUNTRY.name());
        generic.addCompositeField(comp);
        generic.addIndexField(CityField.COUNTRY.name());
        generic.addIndexOnlyField(CityField.STATE.name());
        
        accumuloSetup.setData(FileType.CSV, new CitiesDataType(CitiesDataType.CityEntry.generic, generic));
        connector = accumuloSetup.loadTables(log);
    }
    
    public IndexOnlyQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testIndex() throws Exception {
        log.info("------  testIndex  ------");
        String state = "'ohio'";
        String query = CityField.STATE.name() + EQ_OP + state;
        runTest(query, query);
    }
    
    @Test
    public void testAnd() throws Exception {
        log.info("------  testAnd  ------");
        String state = "'ohio'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + " and " + "(" + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndNot() throws Exception {
        log.info("------  testAndNot  ------");
        String state = "'ohio'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + " and " + "!(" + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndNoIndex() throws Exception {
        log.info("------  testAndNoIndex  ------");
        String state = "'ohio'";
        String code = "'fra'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + " and " + "filter:includeRegex(" + CityField.CODE.name() + ","
                            + code + "))" + OR_OP + "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + " and " + CityField.STATE.name() + EQ_OP
                            + state + ")";
            String expectQuery = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + " and " + CityField.CODE.name() + RE_OP + code + ") or ("
                            + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + " and " + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testDelayedPredicate() throws Exception {
        log.info("------  testDelayedPredicate  ------");
        String cont = "'north america'";
        String state = "'ohio'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP + CityField.CITY.name().toLowerCase() + " == 'none')" + " and "
                            +
                            // NOTE: the ASTDelayedPredicate will not normalize the value - convert to lower case before query
                            "((_Delayed_ = true) and (" + CityField.CITY.name() + EQ_OP + "'" + city.name().toLowerCase() + "'" + "))" + " and "
                            + CityField.STATE.name() + EQ_OP + state;
            String expectQuery = "(" + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP + CityField.CITY.name() + " == 'none')" + " and " + "("
                            + CityField.CITY.name() + EQ_OP + "'" + city.name().toLowerCase() + "')" + " and " + CityField.STATE.name() + EQ_OP + state;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testOneDelayedPredicate() throws Exception {
        log.info("------  testOneDelayedPredicate  ------");
        for (final TestCities city : TestCities.values()) {
            // NOTE: the ASTDelayedPredicate will not normalize the value - convert to lower case before query
            String query = "((_Delayed_ = true) and (" + CityField.CITY.name() + EQ_OP + "'" + city.name().toLowerCase() + "'))";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name().toLowerCase() + "'";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testOr() throws Exception {
        log.info("------  testOr  ------");
        String state = "'usa'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + "(" + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testRegex() throws Exception {
        log.info("------  testRegex  ------");
        String state = "'o.*'";
        String query = state + RE_OP + CityField.STATE.name();
        String eQuery = CityField.STATE.name() + RE_OP + state;
        runTest(query, eQuery);
    }
    
    // ============================================
    // error conditions
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorOrNot() throws Exception {
        log.info("------  testErrorOrNot  ------");
        String state = "'usa'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + "!(" + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, query);
        }
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorNotRegex() throws Exception {
        log.info("------  testErrorNotRegex  ------");
        String state = "'or.*'";
        String query = CityField.STATE.name() + " !~ " + state;
        runTest(query, query);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
