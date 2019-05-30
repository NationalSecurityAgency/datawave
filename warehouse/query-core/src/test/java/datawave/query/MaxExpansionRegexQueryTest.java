package datawave.query;

import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.MaxExpandCityFields;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.CitiesDataType.CityField;
import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NOT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;

/**
 * These tests are highly dependent upon the test data due to the fact that thresholds are tested. Because the test data contains multivalue fields with
 * multiple values (versus having a single value), the expected query may be significantly different from the original query. Thus the addition, modification,
 * or deletion of data could cause one or more test cases to fail.
 */
public class MaxExpansionRegexQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(MaxExpansionRegexQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig max = new MaxExpandCityFields();
        
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.maxExp, max));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public MaxExpansionRegexQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    // ===================================
    // test cases
    
    @Test
    public void testSingleRegex() throws Exception {
        log.info("------  testSingleRegex  ------");
        
        // set regex to match multiple fields
        String regPhrase = RE_OP + "'b-.*'";
        String expect = this.dataManager.convertAnyField(regPhrase);
        String query = Constants.ANY_FIELD + regPhrase;
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, expect);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueRegexAnyField() throws Exception {
        log.info("------  testMaxValueRegexAnyField  ------");
        // set regex to match multiple fields
        String regPhrase = RE_OP + "'a.*'";
        String expect = this.dataManager.convertAnyField(regPhrase);
        String query = Constants.ANY_FIELD + regPhrase;
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        // set regex to match more fields than are specified for the unified expansion
        try {
            runTest(query, expect);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    /**
     * This test case consists of three phases.
     * <ul>
     * <li>In phase one, the query should NOT exceed any thresholds.</li>
     * <li>In phase two, the query should exceed one threshold.</li>
     * <li>In phase three, the query should exceed the threshold for each index.</li>
     * </ul>
     * 
     * @throws Exception
     */
    @Test
    public void testMaxValueAnyField() throws Exception {
        log.info("------  testMaxValueAnyField  ------");
        String regex = RE_OP + "'b.*'";
        String city = EQ_OP + "'b-city'";
        String query = Constants.ANY_FIELD + regex + AND_OP + Constants.ANY_FIELD + city;
        
        String anyRegex = this.dataManager.convertAnyField(regex);
        String anyCity = this.dataManager.convertAnyField(city);
        String expect = anyRegex + AND_OP + anyCity;
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(4);
        runTest(query, expect);
        // threshold should exist for each index
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
        
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        // threshold should exist for each index
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 3);
    }
    
    @Test
    public void testMaxValueAnyFieldFilterExclude() throws Exception {
        log.info("------  testMaxValueAnyFieldFilterExclude  ------");
        String regexPhrase = RE_OP + "'b.*'";
        String exclude = "'.*de-a'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + FILTER_EXCLUDE_REGEX + "(" + CityField.CODE.name() + "," + exclude + ")";
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = anyState + AND_OP + CityField.CODE.name() + RN_OP + exclude;
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        parsePlan(FILTER_EXCLUDE_REGEX, 1);
        
        this.logic.setMaxValueExpansionThreshold(4);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (RuntimeException re) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        // city should have a threshold
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
        parsePlan(FILTER_EXCLUDE_REGEX, 1);
        
        this.logic.setMaxValueExpansionThreshold(2);
        ivaratorConfig();
        runTest(query, expect);
        // city,state, code should have all exceed threshold
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 3);
        parsePlan(FILTER_EXCLUDE_REGEX, 1);
    }
    
    @Test
    public void testMaxValueAnyFieldNegRegex() throws Exception {
        log.info("------  testMaxValueAnyFieldNegRegex  ------");
        String regexPhrase = RN_OP + "'b.*'";
        String fieldVal = EQ_OP + "'a-1'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + Constants.ANY_FIELD + fieldVal;
        
        // '!~' operation is not processed correctly - see QueryJexl docs
        // this is a hack for the expected results
        String expect = CityField.CITY.name() + EQ_OP + "'city-a'";
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(4);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
        
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 3);
    }
    
    @Test
    public void testMaxValueAnyFieldNegAnd() throws Exception {
        log.info("------  testMaxValueFieldNegAnd  ------");
        String regexA = RE_OP + "'a-.*'";
        String regexB = RE_OP + "'b.*'";
        String anyB = this.dataManager.convertAnyField(regexB);
        String city = "'a-1'";
        // @formatter:off
        String query = Constants.ANY_FIELD + regexB + AND_OP +
                NOT_OP + "(" + Constants.ANY_FIELD + regexA + AND_OP +
                CityField.CITY.name() + EQ_OP + city + ")";
        // not operation is not processed correctly - see QueryJexl docs
        // this is a replacement query for the expected results - may fail if data is changed
        String expect = anyB + AND_OP +
                "(" + CityField.CITY.name() + EQ_OP + "'b2-city'" + OR_OP +
                CityField.CITY.name() + EQ_OP + "'b3-city'" + ")";
        // @formatter:on
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(4);
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 4);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
