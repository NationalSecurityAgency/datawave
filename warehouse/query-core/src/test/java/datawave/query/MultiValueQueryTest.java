package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.MultiValueCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

/**
 * Performs query test for multivalue fields.
 */
public class MultiValueQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(MultiValueQueryTest.class);
    private static final String[] TestStates = {"'ohio'", "'missouri'", "'alabama'", "'idaho'"};
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig multi = new MultiValueCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.multivalue, multi));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
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
    public void testComposite() throws Exception {
        log.debug("------  testComposite  ------");
        
        for (final TestCities city : TestCities.values()) {
            for (final String st : TestStates) {
                String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + EQ_OP + st;
                runTest(query, query);
                return;
            }
        }
    }
    
    @Test
    public void testCompositeOrTerm() throws Exception {
        log.debug("------  testCompositeOrTerm  ------");
        
        String code = "'uSA'";
        
        for (final TestCities city : TestCities.values()) {
            for (final String st : TestStates) {
                String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.STATE.name() + EQ_OP + st + OR_OP
                                + CityField.CODE.name() + EQ_OP + code + ")";
                runTest(query, query);
            }
        }
    }
    
    @Test
    public void testCompositeWithVirtual() throws Exception {
        log.debug("------  testCompositeWithVirtual  ------");
        
        String cont = "'NORth AMerica'";
        
        for (final TestCities city : TestCities.values()) {
            for (final String st : TestStates) {
                String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + EQ_OP + st + AND_OP
                                + CityField.CONTINENT.name() + EQ_OP + cont;
                runTest(query, query);
            }
        }
    }
    
    @Test
    public void testVirtual() throws Exception {
        log.debug("------  testVirtual  ------");
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CONTINENT.name() + GT_OP + "'e'";
            runTest(query, query);
        }
    }
    
    @Test
    public void testSingleTerm() throws Exception {
        log.debug("------  testSingleTerm  ------");
        
        for (final String state : TestStates) {
            String query = CityField.STATE.name() + EQ_OP + state;
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
