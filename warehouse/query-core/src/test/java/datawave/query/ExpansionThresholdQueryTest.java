package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class ExpansionThresholdQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(ExpansionThresholdQueryTest.class);
    
    private final List<File> cacheFiles = new ArrayList<>();
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.COUNTRY.name());
        generic.addIndexField(CityField.CODE.name());
        Set<String> idx = generic.getIndexFields();
        for (String field : idx) {
            generic.addReverseIndexField(field);
        }
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public ExpansionThresholdQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testThresholdMarker() throws Exception {
        log.info("------  testThresholdMaker  ------");
        String state = "'.*ss.*'";
        String country = "'un.*'";
        String countryQuery = CitiesDataType.CityField.COUNTRY.name() + RE_OP + country + AND_OP + CitiesDataType.CityField.COUNTRY.name() + RE_OP
                        + country.toUpperCase();
        String query = "((ExceededValueThresholdMarkerJexlNode = true)" + AND_OP + CitiesDataType.CityField.STATE.name() + RE_OP + state + ") && "
                        + countryQuery;
        String expect = "(" + CitiesDataType.CityField.STATE.name() + RE_OP + state + ")" + AND_OP + countryQuery;
        
        this.logic.setMaxValueExpansionThreshold(6);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        
        // NOTE: Issue #156 addresses an issue where the ivarator is not configured
        // it does not throw an exception; just return no results
        // try {
        // runTest(query, expect);
        // Assert.fail("exception expected");
        // } catch (DatawaveFatalQueryException e) {
        // // expected
        // }
        
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testThresholdMarkerAnyField() throws Exception {
        log.info("------  testThresholdMarkerAnyField  ------");
        //
        String anyRegex = RE_OP + "'.*a'";
        String country = RE_OP + "'un.*'";
        String countryQuery = CitiesDataType.CityField.COUNTRY.name() + country + AND_OP + CitiesDataType.CityField.COUNTRY.name() + country.toUpperCase();
        String marker = "(ExceededValueThresholdMarkerJexlNode = true)" + AND_OP;
        String query = "(" + marker + Constants.ANY_FIELD + anyRegex + ") && " + countryQuery;
        String anyCity = this.dataManager.convertAnyField(anyRegex);
        String anyCountry = this.dataManager.convertAnyField(country);
        String expect = "(" + anyCity + ")" + AND_OP + anyCountry;
        
        this.logic.setQueryThreads(1);
        this.logic.setMaxValueExpansionThreshold(2);
        runTest(query, expect);
        
        // issue 156 - alter threshold to 1
        // this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testErrorThresholdMarkerAnyField() throws Exception {
        log.info("------  testErrorThresholdMarkerAnyField  ------");
        // correct when Issue #156 is resolved
        // the DefaultQueryPlanner should resolve this to a single EQNODE
        // currently this will generate an exception that is swallowed
        String state = RE_OP + "'oh.*'";
        String country = RE_OP + "'un.*'";
        String countryQuery = CitiesDataType.CityField.COUNTRY.name() + country + AND_OP + CitiesDataType.CityField.COUNTRY.name() + country.toUpperCase();
        String query = "((ExceededValueThresholdMarkerJexlNode = true)" + AND_OP + Constants.ANY_FIELD + state + ") && " + countryQuery;
        String anyCity = this.dataManager.convertAnyField(state);
        String anyCountry = this.dataManager.convertAnyField(country);
        // actual expect jexl
        // String expect = "(" + anyCity + ")" + AND_OP + anyCountry;
        // no results expect jexl
        String expect = "(x == '')";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(2);
        // TODO: add when #156 is resolved
        // try {
        // runTest(query, expect);
        // Assert.fail("exception expected");
        // } catch (DatawaveFatalQueryException e) {
        // // expected
        // }
        
        ivaratorConfig();
        
        runTest(query, expect);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
