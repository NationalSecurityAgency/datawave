package datawave.query;

import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import org.apache.log4j.Level;
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
import static org.junit.Assert.fail;

/**
 * These test apply to the threshold marker which is normally injected into the query tree during the processing by the {@link QueryPlanner}. The test cases
 * here essentially create a query with the threshold marker already inserted.
 */
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
        String countryQuery = CityField.COUNTRY.name() + RE_OP + country + AND_OP + CityField.COUNTRY.name() + RE_OP + country.toUpperCase();
        String query = "((ExceededValueThresholdMarkerJexlNode = true)" + AND_OP + CityField.STATE.name() + RE_OP + state + ") && " + countryQuery;
        String expect = "(" + CityField.STATE.name() + RE_OP + state + ")" + AND_OP + countryQuery;
        
        this.logic.setMaxValueExpansionThreshold(6);
        try {
            runTest(query, expect);
            fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, expect);
            fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        
        this.logic.setMaxValueExpansionThreshold(6);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
    }
    
    @Test
    public void testThresholdMarkerAnyField() throws Exception {
        log.info("------  testThresholdMarkerAnyField  ------");
        //
        String anyRegex = RE_OP + "'.*a'";
        String country = RE_OP + "'un.*'";
        String countryQuery = CityField.COUNTRY.name() + country + AND_OP + CityField.COUNTRY.name() + country.toUpperCase();
        String marker = "(ExceededValueThresholdMarkerJexlNode = true)" + AND_OP;
        String query = "(" + marker + Constants.ANY_FIELD + anyRegex + ") && " + countryQuery;
        String anyCity = this.dataManager.convertAnyField(anyRegex);
        String anyCountry = this.dataManager.convertAnyField(country);
        String expect = "(" + anyCity + ")" + AND_OP + anyCountry;
        
        ivaratorConfig();
        
        this.logic.setQueryThreads(1);
        this.logic.setMaxValueExpansionThreshold(2);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
