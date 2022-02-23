package datawave.query;

import datawave.query.exceptions.DatawaveIvaratorMaxResultsException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.MaxExpandCityFields;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static datawave.query.testframework.CitiesDataType.CityField;
import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NOT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * These tests are highly dependent upon the test data due to the fact that thresholds are tested. Because the test data contains multivalue fields with
 * multiple values (versus having a single value), the expected query may be significantly different from the original query. Thus the addition, modification,
 * or deletion of data could cause one or more test cases to fail.
 */
public class MaxExpansionRegexQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(MaxExpansionRegexQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig max = new MaxExpandCityFields();
        
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.maxExp, max));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
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
        
        ivaratorConfig();
        
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
        } catch (FullTableScansDisallowedException e) {
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
        
        ivaratorConfig();
        
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
    
    /**
     * This tests a query without an intersection such that when we force the ivarators to fail with a maxResults setting of 1, the query will fail.
     *
     * @throws Exception
     */
    @Test
    public void testMaxIvaratorResultsFailsQuery() throws Exception {
        log.info("------  testMaxIvaratorResultsFailsQuery  ------");
        String regex = RE_OP + "'b.*'";
        String query = Constants.ANY_FIELD + regex;
        
        String anyRegex = this.dataManager.convertAnyField(regex);
        String expect = anyRegex;
        
        List<String> dirs = ivaratorConfig();
        // set collapseUids to ensure we have shard ranges such that ivarators will actually execute
        this.logic.setCollapseUids(true);
        // force the regex lookup into an ivarator
        this.logic.setMaxValueExpansionThreshold(1);
        // set a small buffer size to ensure we actually persist the buffers so that we can detect this below
        this.logic.setIvaratorCacheBufferSize(2);
        
        runTest(query, expect);
        // verify that the ivarators ran and completed
        assertEquals(3, countComplete(dirs));
        
        // clear list before new set is added
        dirs.clear();
        // now get a new set of ivarator directories
        dirs = ivaratorConfig();
        // set the max ivarator results to 1
        this.logic.setMaxIvaratorResults(1);
        try {
            // verify the query actually fails
            runTest(query, expect);
            fail("Expected the query to fail with the ivarators fail");
        } catch (Exception e) {
            if (!hasCause(e, DatawaveIvaratorMaxResultsException.class)) {
                log.error("Unexpected exception", e);
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }
    
    private boolean hasCause(Throwable e, Class<? extends Exception> causeClass) {
        while (e != null && !causeClass.isInstance(e)) {
            e = e.getCause();
        }
        return e != null;
    }
    
    /**
     * This test case tests and query that has an intersection such that when we force the ivarators to fail with a maxResults setting of 1, that the query can
     * still complete.
     *
     * @throws Exception
     */
    @Test
    public void testMaxIvaratorResults() throws Exception {
        log.info("------  testMaxIvaratorResults  ------");
        String regex = RE_OP + "'b.*'";
        String city = EQ_OP + "'b-city'";
        String query = Constants.ANY_FIELD + regex + AND_OP + Constants.ANY_FIELD + city;
        
        String anyRegex = this.dataManager.convertAnyField(regex);
        String anyCity = this.dataManager.convertAnyField(city);
        String expect = anyRegex + AND_OP + anyCity;
        
        List<String> dirs = ivaratorConfig();
        // set collapseUids to ensure we have shard ranges such that ivarators will actually execute
        this.logic.setCollapseUids(true);
        // force the regex lookup into an ivarator
        this.logic.setMaxValueExpansionThreshold(1);
        // set a small buffer size to ensure we actually persist the buffers so that we can detect this below
        this.logic.setIvaratorCacheBufferSize(2);
        
        runTest(query, expect);
        // verify that the ivarators ran and completed
        assertEquals(3, countComplete(dirs));
        
        // clear list before new set is added
        dirs.clear();
        
        // now get a new set of ivarator directories
        dirs = ivaratorConfig();
        // set the max ivarator results to 1
        this.logic.setMaxIvaratorResults(1);
        // verify we still get our expected results
        runTest(query, expect);
        // and verify that the ivarators indeed did not complete (i.e. failed)
        assertEquals(0, countComplete(dirs));
    }
    
    private int countComplete(List<String> dirs) throws Exception {
        int count = 0;
        for (String dir : dirs) {
            File file = new File(new URI(dir));
            for (File leaf : getLeaves(file)) {
                if (leaf.getName().equals("complete")) {
                    count++;
                }
            }
        }
        return count;
    }
    
    private Collection<File> getLeaves(File file) {
        List<File> children = new ArrayList<>();
        for (File child : file.listFiles()) {
            if (child.isDirectory()) {
                children.addAll(getLeaves(child));
            } else {
                children.add(child);
            }
        }
        return children;
    }
    
    // ============================================
    // implemented abstract methods
    @Override
    protected void testInit() {
        this.auths = CitiesDataType.getExpansionAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
