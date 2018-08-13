package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.MultiValueCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;

/**
 * Performs query test for multivalue fields.
 * <p>
 * TODO: Fix composite indexes when this is working.
 * </p>
 */
public class MultiValueQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(MultiValueQueryTest.class);
    private static final String[] TestStates = {"ohio", "missouri", "alabama"};
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig multi = new MultiValueCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.multivalue, multi));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public MultiValueQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testMultiValue() throws Exception {
        log.debug("------  testMultiValue  ------");
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + " == '" + city.name() + "'";
            runTest(query, query);
        }
    }
    
    @Test
    public void testCompoundMultiValue() throws Exception {
        log.debug("------  testMultiValue  ------");
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CONTINENT.name() + GT_OP + "'e'";
            runTest(query, query);
        }
    }
    
    @Test
    public void testSingleAndMultiValue() throws Exception {
        log.debug("------  testSingleAndMultiValue  ------");
        
        for (final String state : TestStates) {
            String query = CityField.STATE.name() + EQ_OP + "'" + state + "'";
            runTest(query, query);
        }
    }
    
    @Test
    public void testNotMatch() throws Exception {
        log.debug("------  testNotMatch  ------");
        
        String[] states = {"'no-match'", "'no-ohio'"};
        for (final String state : states) {
            String query = CityField.STATE.name() + EQ_OP + state;
            runTest(query, query);
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
}
