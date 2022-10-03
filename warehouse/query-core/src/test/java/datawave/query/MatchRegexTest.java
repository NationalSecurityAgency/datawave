package datawave.query;

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

import static datawave.query.testframework.CitiesDataType.CityEntry;
import static datawave.query.testframework.CitiesDataType.CityEntry.nullState;
import static datawave.query.testframework.CitiesDataType.CityField;
import static datawave.query.testframework.CitiesDataType.getManager;
import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

/**
 * Contains functional tests for the f:matchRegex query function.
 */
public class MatchRegexTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(MatchRegexTest.class);
    
    private static final String MatchRegex = "f:matchRegex(";
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.STATE.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        dataTypes.add(new CitiesDataType(nullState, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public MatchRegexTest() {
        super(getManager());
    }
    
    @Test
    public void testStandardQuery() throws Exception {
        log.info("----- testStandardQuery -----");
        
        String state = "'ohio'";
        
        for (final TestCities city : TestCities.values()) {
            
            String query = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + MatchRegex + CityField.STATE.name() + "," + state
                            + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + state;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testStandardQueryWithNumeric() throws Exception {
        log.info("------  testStandardQueryWithNumeric  ------");
        
        String num = "'110'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + MatchRegex + CityField.STATE.name() + "||"
                            + CityField.NUM.name() + "," + num + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + RE_OP + num + " or "
                            + CityField.NUM.name() + RE_OP + num + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testAnyField() throws Exception {
        log.info("------  testAnyField  ------");
        
        String usa = "'usa'";
        String anyRegex = this.dataManager.convertAnyField(RE_OP + usa);
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + MatchRegex + Constants.ANY_FIELD + "," + usa + ")";
            String expectQuery = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + anyRegex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testWildCard() throws Exception {
        log.info("------  testWildCard  ------");
        
        this.logic.setFullTableScanEnabled(true);
        
        String code = "'uSa'";
        String regex = "'.*o.*'";
        
        for (final TestCities ignored : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + MatchRegex + CityField.CITY.name() + "," + regex + ")";
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.CITY.name() + RE_OP + regex;
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testNotWildCard() throws Exception {
        log.info("------  testNotWildCard  ------");
        
        this.logic.setFullTableScanEnabled(true);
        
        String code = "'uSa'";
        String regex = "'.*o.*'";
        
        for (final TestCities ignored : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + "not " + MatchRegex + CityField.CITY.name() + "," + regex + ")";
            String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + " not (" + CityField.CITY.name() + RE_OP + regex + ")";
            runTest(query, expectQuery);
        }
    }
    
    @Test
    public void testWildCardAnyField() throws Exception {
        log.info("------  testWildCardAnyField  ------");
        
        this.logic.setFullTableScanEnabled(true);
        
        String code = "'uSa'";
        String regex = "'.*isS.*'";
        
        String query = CityField.CODE.name() + EQ_OP + code + AND_OP + MatchRegex + Constants.ANY_FIELD + "," + regex + ")";
        String expectQuery = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.STATE.name() + RE_OP + regex;
        runTest(query, expectQuery);
    }
    
    @Override
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
