package datawave.query;

import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
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

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class PushdownQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(PushdownQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        // add CODE as an index only field
        generic.addIndexField(CityField.CODE.name());
        generic.addIndexOnlyField(CityField.CODE.name());
        generic.addReverseIndexField(CityField.CODE.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public PushdownQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testAnd() throws Exception {
        log.info("------  testAnd  ------");
        String state = "'ohio'";
        String country = "'itaLY'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + EQ_OP + state + OR_OP
                            + CityField.COUNTRY.name() + EQ_OP + country + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testCompoundAnd() throws Exception {
        log.info("------  testCompoundAnd  ------");
        String state = "'ohio'";
        String country = "'itaLY'";
        String code = "'ITa'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CODE.name() + EQ_OP + code + ")" + AND_OP + "("
                            + CityField.STATE.name() + EQ_OP + state + OR_OP + CityField.COUNTRY.name() + EQ_OP + country + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndFilterIncludeRegex() throws Exception {
        log.info("------  testAndFilterIncludeRegex  ------");
        String state = "'ohio'";
        String country = "'itaLY'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + " == '" + city.name() + "' and (" + CityField.STATE.name() + EQ_OP + state + " or " + "filter:includeRegex("
                            + CityField.COUNTRY.name() + "," + country + "))";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + EQ_OP + state + " or "
                            + CityField.COUNTRY.name() + RE_OP + country + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testCompoundAndFilterIncludeRegex() throws Exception {
        log.info("------  testCompoundAndFilterIncludeRegex  ------");
        String state = "'ohio'";
        String country = "'itaLY'";
        String code = "'.*a'";
        for (final TestCities city : TestCities.values()) {
            String qOne = "(" + CityField.CITY.name() + " == '" + city.name() + "' and " + CityField.CODE.name() + RE_OP + code + ")" + AND_OP + "("
                            + CityField.STATE.name() + EQ_OP + state;
            String query = qOne + " or filter:includeRegex(" + CityField.COUNTRY.name() + "," + country + "))";
            String expectQuery = qOne + OR_OP + CityField.COUNTRY.name() + RE_OP + country + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testNumericLt() throws Exception {
        log.info("------  testNumericLt  ------");
        String cont = "'europe'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP
                            + CityField.NUM.name() + LT_OP + "104)";
            runTest(query, query);
        }
    }
    
    @Test
    public void testDelayedIndexOnly() throws Exception {
        log.info("------  testErrorIndexOnly  ------");
        String query = CityField.CITY.name() + EQ_OP + "'PARIS'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + "'usa'" + OR_OP + CityField.NUM.name()
                        + LT_OP + "104)";
        ((DefaultQueryPlanner) logic.getQueryPlanner()).setExecutableExpansion(false);
        runTest(query, query);
    }
    
    @Test
    public void testDelayedFilterIncludeRegex() throws Exception {
        log.info("------  testErrorFilterIncludeRegex  ------");
        String state = "'ohio'";
        String code = "'itA'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + code + OR_OP
                            + "filter:includeRegex(" + CityField.STATE.name() + "," + state + "))";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + code + OR_OP
                            + CityField.STATE.name() + RE_OP + state + ")";
            ((DefaultQueryPlanner) logic.getQueryPlanner()).setExecutableExpansion(false);
            runTest(query, expectQuery);
        }
    }
    
    // ============================================
    // error conditions
    @Test
    public void testErrorIndexOnlyExpansion() throws Exception {
        log.info("------  testErrorIndexOnly  ------");
        String query = CityField.CITY.name() + EQ_OP + "'PARIS'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + "'usa'" + OR_OP + CityField.NUM.name()
                        + LT_OP + "104)";
        runTest(query, query);
    }
    
    @Test
    public void testExecutableExpansionRegex() throws Exception {
        log.info("------  testExecutableExpansionRegex  ------");
        String state = "'ohio'";
        String code = "'itA'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + code + OR_OP
                            + "filter:includeRegex(" + CityField.STATE.name() + "," + state + "))";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + code + OR_OP
                            + CityField.STATE.name() + RE_OP + state + ")";
            runTest(query, expectQuery);
        }
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
