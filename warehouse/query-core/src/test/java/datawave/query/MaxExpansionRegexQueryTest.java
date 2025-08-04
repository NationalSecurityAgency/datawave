package datawave.query;

import static datawave.query.testframework.CitiesDataType.CityField;
import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NOT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CityDataManager;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.MaxExpandCityFields;

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

        CityDataManager.newInstance();
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.maxExp, max));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log);
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
        String plan = getPlan(query, true, true);
        String expectedPlan = "CODE == 'b-code' || CITY == 'b-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b-state'";
        assertEquals(expectedPlan, plan);

        // a failure to fully expand a value means a field could be missed, that term is no longer executable
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        expectedPlan = "CODE == 'b-code' || CITY == 'b-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b-state'";
        assertEquals(expectedPlan, plan);
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
        String plan = getPlan(query, true, true);
        String expectedPlan = "CODE == 'a-code' || CITY == 'a-1' || STATE == 'a-state' || STATE == 'a-s2'";
        assertEquals(expectedPlan, plan);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        // value expansion threshold should not affect an unfielded regex
        this.logic.setMaxValueExpansionThreshold(1);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    /**
     * At no point should any value expansion threshold affect the final plan for an unfielded regex
     *
     * @throws Exception
     *             if there is an issue
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
        String expectedPlan = "(CODE == 'b2-code' || CODE == 'b-code' || CODE == 'b3-code' || CITY == 'b-city' || CITY == 'b2-city' || CITY == 'b3-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2') && CITY == 'b-city'";

        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        this.logic.setMaxValueExpansionThreshold(4);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
        ;

        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    @Test
    public void testMaxValueAnyFieldFilterExclude() throws Exception {
        log.info("------  testMaxValueAnyFieldFilterExclude  ------");
        String regexPhrase = RE_OP + "'b.*'";
        String exclude = "'.*de-a'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + FILTER_EXCLUDE_REGEX + "(" + CityField.CODE.name() + "," + exclude + ")";
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = anyState + AND_OP + CityField.CODE.name() + RN_OP + exclude;

        String expectedPlan = "(CODE == 'b2-code' || CODE == 'b-code' || CODE == 'b3-code' || CITY == 'b-city' || CITY == 'b2-city' || CITY == 'b3-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2') && filter:excludeRegex(CODE, '.*de-a')";
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    @Test
    public void testMaxValueAnyFieldNegRegex() throws Exception {
        log.info("------  testMaxValueAnyFieldNegRegex  ------");
        String regexPhrase = RN_OP + "'b.*'";
        String fieldVal = EQ_OP + "'a-1'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + Constants.ANY_FIELD + fieldVal;

        ivaratorConfig();
        String expectedPlan = "!(((_Delayed_ = true) && (_ANYFIELD_ =~ 'b.*')) || CODE == 'b2-code' || CODE == 'b-code' || CODE == 'b3-code' || CITY == 'b-city' || CITY == 'b2-city' || CITY == 'b3-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2') && CITY == 'a-1'";

        // '!~' operation is not processed correctly - see QueryJexl docs
        // this is a hack for the expected results
        String expect = CityField.CITY.name() + EQ_OP + "'city-a'";
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // changing the value expansion threshold should not affect the final query plan
        this.logic.setMaxValueExpansionThreshold(4);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // changing the value expansion threshold should not affect the final query plan
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
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

        String expectedPlan = "(CODE == 'b2-code' || CODE == 'b-code' || CODE == 'b3-code' || CITY == 'b-city' || CITY == 'b2-city' || CITY == 'b3-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2') && !((((_Delayed_ = true) && (_ANYFIELD_ =~ 'a-.*')) || CODE == 'a-code' || CITY == 'a-1' || STATE == 'a-state' || STATE == 'a-s2') && CITY == 'a-1')";

        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        this.logic.setMaxValueExpansionThreshold(4);
        ivaratorConfig();
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    /**
     * Demonstrate that an unfielded regex fully expands into fields and values, regardless of thresholds
     *
     * @throws Exception
     *             if there is an issue
     */
    @Test
    public void testMaxIvaratorResultsFailsQuery() throws Exception {
        log.info("------  testMaxIvaratorResultsFailsQuery  ------");
        String regex = RE_OP + "'b.*'";
        String query = Constants.ANY_FIELD + regex;
        // force the regex lookup into an ivarator
        this.logic.setMaxValueExpansionThreshold(1);
        // set a small buffer size to ensure we actually persist the buffers so that we can detect this below
        this.logic.setIvaratorCacheBufferSize(2);

        String expectedPlan = "CODE == 'b2-code' || CODE == 'b-code' || CODE == 'b3-code' || CITY == 'b-city' || CITY == 'b2-city' || CITY == 'b3-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2'";
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    /**
     * This test case tests and query that has an intersection such that when we force the ivarators to fail with a maxResults setting of 1, that the query can
     * still complete.
     *
     * @throws Exception
     *             if there is an issue
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

        // set collapseUids to ensure we have shard ranges such that ivarators will actually execute
        this.logic.setCollapseUids(true);
        // force the regex lookup into an ivarator
        this.logic.setMaxValueExpansionThreshold(1);
        // set a small buffer size to ensure we actually persist the buffers so that we can detect this below
        this.logic.setIvaratorCacheBufferSize(2);

        // verify query gets all expected results
        runTest(query, expect);

        String expectedPlan = "(CODE == 'b2-code' || CODE == 'b-code' || CODE == 'b3-code' || CITY == 'b-city' || CITY == 'b2-city' || CITY == 'b3-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2') && CITY == 'b-city'";
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    // ============================================
    // implemented abstract methods
    @Override
    protected void testInit() {
        this.auths = CitiesDataType.getExpansionAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
