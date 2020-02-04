package datawave.query;

import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_OR_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

/**
 * Tests for different types of string and numeric range specifications.
 */
public class RangeQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(RangeQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.NUM.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public RangeQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testSingleValue() throws Exception {
        log.info("------  testSingleValue  ------");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "'";
            
            // Test the plan with all expansions
            String expected = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "')";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            expected = CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "'";
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            expected = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "')";
            plan = getPlan(query, false, true);
            assertPlanEquals(expected, plan);
            
            // test running the query
            expected = query;
            runTest(query, expected);
        }
    }
    
    @Test
    public void testRangeWithTerm() throws Exception {
        for (final TestCities city : TestCities.values()) {
            String query = "((" + CityField.NUM.name() + LTE_OP + "100)" + AND_OP + "(" + CityField.NUM.name() + GTE_OP + "100))" + AND_OP
                            + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            
            // Test the plan with all expansions
            String expected = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+cE1'";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            expected = "((" + CityField.NUM.name() + LTE_OP + "'+cE1')" + JEXL_AND_OP + "(" + CityField.NUM.name() + GTE_OP + "'+cE1'))" + JEXL_AND_OP
                            + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            expected = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+cE1'";
            plan = getPlan(query, false, true);
            assertPlanEquals(expected, plan);
            
            // test running the query
            expected = query;
            runTest(query, expected);
        }
    }
    
    @Test
    public void testSingleValueAndMultiFieldWithParens() throws Exception {
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + AND_OP + "(" + CityField.NUM.name() + LTE_OP + "20" + AND_OP + CityField.NUM.name() + GTE_OP + "20)";
            
            // Test the plan with all expansions
            String expected = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+bE2'";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            expected = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + AND_OP + "(" + CityField.NUM.name() + LTE_OP + "'+bE2'" + JEXL_AND_OP + CityField.NUM.name() + GTE_OP + "'+bE2')";
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            expected = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+bE2'";
            plan = getPlan(query, false, true);
            assertPlanEquals(expected, plan);
            
            // test running the query
            expected = query;
            runTest(query, expected);
        }
    }
    
    @Test
    public void testSingleValueAndMultiFieldNoParens() throws Exception {
        log.info("------  testSingleValueAndMultiFieldNoParens  ------");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "'"
                            + AND_OP + CityField.NUM.name() + LTE_OP + "20" + AND_OP + CityField.NUM.name() + GTE_OP + "20";
            
            // Test the plan with all expansions
            String expected = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+bE2'";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            expected = CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "'"
                            + JEXL_AND_OP + CityField.NUM.name() + LTE_OP + "'+bE2'" + JEXL_AND_OP + CityField.NUM.name() + GTE_OP + "'+bE2'";
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            expected = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+bE2'";
            plan = getPlan(query, false, true);
            assertPlanEquals(expected, plan);
            
            // test running the query
            expected = query;
            runTest(query, expected);
        }
    }
    
    @Test
    public void testSingleValueOrMultiFieldWithParens() throws Exception {
        log.info("------  testSingleValueOrMultiFieldWithParens  ------");
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + OR_OP + "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.NUM.name() + GTE_OP + "100)";
            
            // Test the plan with all expansions
            String expected = "((" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'))" + JEXL_OR_OP + "((" + CityField.NUM.name() + EQ_OP + "'+cE1'))";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            expected = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + OR_OP + "(" + CityField.NUM.name() + LTE_OP + "'+cE1'" + JEXL_AND_OP + CityField.NUM.name() + GTE_OP + "'+cE1')";
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            expected = "((" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'))" + JEXL_OR_OP + "((" + CityField.NUM.name() + EQ_OP + "'+cE1'))";
            plan = getPlan(query, false, true);
            assertPlanEquals(expected, plan);
            
            // test running the query
            expected = query;
            runTest(query, expected);
        }
    }
    
    @Test
    public void testMultiFieldsNoResults() throws Exception {
        log.info("------  testMultiFieldsNoResults  ------");
        String state = "'ohio'";
        String qState = "(" + CityField.STATE.name() + LTE_OP + state + AND_OP + CityField.STATE.name() + GTE_OP + state + ")";
        
        String cont = "'europe'";
        String qCont = "(" + CityField.CONTINENT.name() + LTE_OP + cont + AND_OP + CityField.CONTINENT.name() + GTE_OP + cont + ")";
        String qNum = "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.NUM.name() + GTE_OP + "100)";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + AND_OP + qState + AND_OP + qCont + AND_OP + qNum;
            
            // Test the plan with all expansions
            String expected = CityField.NUM.name() + EQ_OP + "'+cE1'" + JEXL_AND_OP + CityField.CONTINENT + EQ_OP + cont + JEXL_AND_OP + CityField.STATE.name()
                            + EQ_OP + state + JEXL_AND_OP + CityField.CITY + EQ_OP + "'" + city.name() + "'";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            String expectedState = "(" + CityField.STATE.name() + LTE_OP + state + JEXL_AND_OP + CityField.STATE.name() + GTE_OP + state + ")";
            String expectedCont = "(" + CityField.CONTINENT.name() + LTE_OP + cont + JEXL_AND_OP + CityField.CONTINENT.name() + GTE_OP + cont + ")";
            String expectedNum = "(" + CityField.NUM.name() + LTE_OP + "'+cE1'" + JEXL_AND_OP + CityField.NUM.name() + GTE_OP + "'+cE1')";
            expected = "(" + CityField.CITY.name() + LTE_OP + "'" + city.name() + "'" + JEXL_AND_OP + CityField.CITY.name() + GTE_OP + "'" + city.name() + "')"
                            + AND_OP + expectedState + AND_OP + expectedCont + AND_OP + expectedNum;
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            expected = CityField.NUM.name() + EQ_OP + "'+cE1'" + JEXL_AND_OP + CityField.CONTINENT + EQ_OP + cont + JEXL_AND_OP + CityField.STATE.name()
                            + EQ_OP + state + JEXL_AND_OP + CityField.CITY + EQ_OP + "'" + city.name() + "'";
            plan = getPlan(query, false, true);
            assertPlanEquals(expected, plan);
            
            // test running the query
            expected = query;
            runTest(query, expected);
        }
    }
    
    @Test
    public void testRangeOpsInDiffSubTree() throws Exception {
        log.info("------  testRangeOpsInDiffSubTree  ------");
        String city = TestCities.rome.name();
        String query = "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.CITY.name() + EQ_OP + "'" + city + "')" + AND_OP + CityField.NUM.name()
                        + GTE_OP + "100";
        
        // Test the plan with all expansions
        String expected = CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+cE1'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expected, plan);
        
        // Test the plan sans value expansion
        expected = CityField.NUM.name() + LTE_OP + "'+cE1'" + JEXL_AND_OP + CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP
                        + CityField.NUM.name() + GTE_OP + "'+cE1'";
        plan = getPlan(query, true, false);
        assertPlanEquals(expected, plan);
        
        // Test the plan sans field expansion
        expected = CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP + CityField.NUM.name() + EQ_OP + "'+cE1'";
        plan = getPlan(query, false, true);
        assertPlanEquals(expected, plan);
        
        // test running the query
        expected = query;
        runTest(query, expected);
    }
    
    @Test
    public void testRangeInOut() throws Exception {
        log.info("------  testRangeInOut  ------");
        String city = TestCities.rome.name();
        String query = "(" + CityField.NUM.name() + LTE_OP + "100" + AND_OP + CityField.NUM.name() + GTE_OP + "100)";
        this.logic.setMaxValueExpansionThreshold(2);
        
        // Test the plan with all expansions
        String expected = "((" + CityField.NUM.name() + EQ_OP + "'+cE1'" + "))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expected, plan);
        
        // Test the plan sans value expansion
        expected = "(" + CityField.NUM.name() + LTE_OP + "'+cE1'" + JEXL_AND_OP + CityField.NUM.name() + GTE_OP + "'+cE1')";
        plan = getPlan(query, true, false);
        assertPlanEquals(expected, plan);
        
        // Test the plan sans field expansion
        expected = "((" + CityField.NUM.name() + EQ_OP + "'+cE1'" + "))";
        plan = getPlan(query, false, true);
        assertPlanEquals(expected, plan);
        
        // test running the query
        expected = query;
        runTest(query, expected);
    }
    
    @Test
    public void testRangeOrExp() throws Exception {
        log.info("------  testRangeOrExp  ------");
        String start = "'e'";
        String end = "'r'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + CityField.CITY.name() + EQ_OP + "'" + city.name()
                            + "-extra')" + AND_OP + "(" + CityField.STATE.name() + GTE_OP + start + AND_OP + CityField.STATE.name() + LTE_OP + end + ")";
            
            // test running the query
            String expected = query;
            runTest(query, expected);
        }
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorRangeOpsInDiffSubTree() throws Exception {
        log.info("------  testErrorRangeOpsInDiffSubTree  ------");
        String city = TestCities.rome.name();
        String query = CityField.NUM.name() + LTE_OP + "100" + AND_OP + "(" + CityField.CITY.name() + EQ_OP + "'" + city + "'" + OR_OP + CityField.NUM.name()
                        + GTE_OP + "100)";
        ((DefaultQueryPlanner) logic.getQueryPlanner()).setExecutableExpansion(false);
        
        // Test the plan with all expansions
        try {
            String plan = getPlan(query, true, true);
            Assert.fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // Test the plan sans value expansion
        try {
            String plan = getPlan(query, true, false);
            Assert.fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // Test the plan sans field expansion
        try {
            String plan = getPlan(query, false, true);
            Assert.fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // test running the query
        String expected = query;
        runTest(query, expected);
    }
    
    @Test
    public void testAvoidErrorRangeOpsInDiffSubTreeWithExpansion() throws Exception {
        log.info("------  testErrorRangeOpsInDiffSubTree  ------");
        String city = TestCities.rome.name();
        String query = CityField.NUM.name() + LTE_OP + "100" + AND_OP + "(" + CityField.CITY.name() + EQ_OP + "'" + city + "'" + OR_OP + CityField.NUM.name()
                        + GTE_OP + "100)";
        
        // Test the plan with all expansions
        String expected = "((" + CityField.CITY.name() + EQ_OP + "'" + city + "')" + JEXL_AND_OP + CityField.NUM.name() + LTE_OP + "'+cE1')" + JEXL_OR_OP
                        + CityField.NUM.name() + EQ_OP + "'+cE1'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expected, plan);
        
        // Test the plan sans value expansion
        expected = "(" + CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP + CityField.NUM.name() + LTE_OP + "'+cE1')" + JEXL_OR_OP + '('
                        + CityField.NUM.name() + LTE_OP + "'+cE1'" + JEXL_AND_OP + CityField.NUM.name() + GTE_OP + "'+cE1')";
        plan = getPlan(query, true, false);
        assertPlanEquals(expected, plan);
        
        // Test the plan sans field expansion
        expected = "((" + CityField.CITY.name() + EQ_OP + "'" + city + "')" + JEXL_AND_OP + CityField.NUM.name() + LTE_OP + "'+cE1')" + JEXL_OR_OP
                        + CityField.NUM.name() + EQ_OP + "'+cE1'";
        plan = getPlan(query, false, true);
        assertPlanEquals(expected, plan);
        
        // test running the query
        expected = query;
        runTest(query, expected);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorRangeGTE() throws Exception {
        log.info("------  testErrorRangeGTE  ------");
        String city = TestCities.rome.name();
        String query = "(" + CityField.NUM.name() + GTE_OP + "99" + AND_OP + CityField.NUM.name() + GTE_OP + "121)";
        
        // Test the plan with all expansions
        try {
            String plan = getPlan(query, true, true);
            Assert.fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // Test the plan sans value expansion
        try {
            String plan = getPlan(query, true, false);
            Assert.fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // Test the plan sans field expansion
        try {
            String plan = getPlan(query, false, true);
            Assert.fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // test running the query
        String expected = query;
        runTest(query, expected);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
