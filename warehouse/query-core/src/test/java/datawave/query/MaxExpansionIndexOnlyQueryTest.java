package datawave.query;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NOT_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.query.planner.DatePartitionedQueryPlanner;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CityDataManager;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.MaxExpandCityFields;

public class MaxExpansionIndexOnlyQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(MaxExpansionIndexOnlyQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig max = new MaxExpandCityFields();
        max.addIndexOnlyField(CitiesDataType.CityField.CITY.name());
        max.addIndexOnlyField(CitiesDataType.CityField.STATE.name());

        CityDataManager.newInstance();
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.maxExp, max));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log);
    }

    public MaxExpansionIndexOnlyQueryTest() {
        super(CitiesDataType.getManager());
    }

    // ===================================
    // test cases

    @Test
    public void testMaxValueRegexIndexOnly_defaultQueryPlanner() throws Exception {
        log.info("------  testMaxValueRegexIndexOnly : " + DefaultQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DefaultQueryPlanner());

        // set regex to match multiple fields
        String city = EQ_OP + "'a-1'";
        String code = RE_OP + "'b.*'";

        String query = CitiesDataType.CityField.CITY.name() + city + AND_OP + CitiesDataType.CityField.STATE.name() + code;
        String expectedPlan = "CITY == 'a-1' && (STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2')";

        this.logic.setMaxValueExpansionThreshold(20);
        runTest(query, query);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // value threshold does not affect the final plan
        this.logic.setMaxValueExpansionThreshold(2);
        runTest(query, query);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // configuring ivarators does not affect the final plan
        ivaratorConfig();
        runTest(query, query);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    @Test
    public void testMaxValueRegexIndexOnly_federatedQueryPlanner() throws Exception {
        log.info("------  testMaxValueRegexIndexOnly : " + DatePartitionedQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());

        // set regex to match multiple fields
        String city = EQ_OP + "'a-1'";
        String code = RE_OP + "'b.*'";

        String query = CitiesDataType.CityField.CITY.name() + city + AND_OP + CitiesDataType.CityField.STATE.name() + code;
        String expectedPlan = "CITY == 'a-1' && (STATE == 'b3-state' || STATE == 'b-state' || STATE == 'bi-s' || STATE == 'b2-state' || STATE == 'ba-s2')";

        this.logic.setMaxValueExpansionThreshold(20);
        runTest(query, query);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // value threshold does not affect final query plan
        this.logic.setMaxValueExpansionThreshold(2);
        runTest(query, query);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // ivarator config does not affect final query plan
        ivaratorConfig();
        runTest(query, query);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    @Test
    public void testMaxValueAnyField_defaultQueryPlanner() throws Exception {
        log.info("------  testMaxValueAnyField : " + DefaultQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DefaultQueryPlanner());

        String regexT = RE_OP + "'b-.*'";
        String regexA = RE_OP + "'a-.*'";
        String query = Constants.ANY_FIELD + regexT + AND_OP + Constants.ANY_FIELD + regexA;
        String anyT = this.dataManager.convertAnyField(regexT);
        String anyA = this.dataManager.convertAnyField(regexA);
        String expect = anyT + AND_OP + anyA;

        String expectedPlan = "(CODE == 'b-code' || CITY == 'b-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b-state') && (CODE == 'a-code' || CITY == 'a-1' || STATE == 'a-state' || STATE == 'a-s2')";

        // value threshold does not affect final plan
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // value threshold does not affect final plan
        this.logic.setMaxValueExpansionThreshold(2);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // ivarator config does not affect final plan
        ivaratorConfig();
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // hit exists in shard 20151010_0
        // range is 20150404_0 to 20150404 + MAX_VALUE
        this.logic.setMaxValueExpansionThreshold(1);
        this.logic.setUseDocumentScheduler(false);
        ivaratorConfig();
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    @Test
    public void testMaxValueAnyField_federatedQueryPlanner() throws Exception {
        log.info("------  testMaxValueAnyField : " + DatePartitionedQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());

        String regexT = RE_OP + "'b-.*'";
        String regexA = RE_OP + "'a-.*'";
        String query = Constants.ANY_FIELD + regexT + AND_OP + Constants.ANY_FIELD + regexA;
        String anyT = this.dataManager.convertAnyField(regexT);
        String anyA = this.dataManager.convertAnyField(regexA);
        String expect = anyT + AND_OP + anyA;

        String expectedPlan = "(CODE == 'b-code' || CITY == 'b-city' || CITY == 'b-2' || CITY == 'b-1' || STATE == 'b-state') && (CODE == 'a-code' || CITY == 'a-1' || STATE == 'a-state' || STATE == 'a-s2')";

        // value threshold does not affect the final plan
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // value threshold does not affect the final plan
        this.logic.setMaxValueExpansionThreshold(2);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // ivarator config does not affect the final plan
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

    @Test
    public void testMaxValueNegAnyField_defaultQueryPlanner() throws Exception {
        log.info("------  testMaxValueNegAnyField : " + DefaultQueryPlanner.class.getSimpleName() + "  ------");

        this.logic.setQueryPlanner(new DefaultQueryPlanner());

        String regexPhrase = RE_OP + "'a.*'";
        String country = "'b-StaTe'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + NOT_OP + "(" + Constants.ANY_FIELD + regexPhrase + ")";
        String expect = CitiesDataType.CityField.STATE.name() + EQ_OP + "'bi-s'";

        String expectedPlan = "STATE == 'b-state' && !(((_Delayed_ = true) && (_ANYFIELD_ =~ 'a.*')) || CODE == 'a-code' || CITY == 'a-1' || STATE == 'a-state' || STATE == 'a-s2')";

        // value threshold has no effect on final plan
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // value threshold has no effect on final plan
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // ivarator config has no effect on final plan
        ivaratorConfig();
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    @Test
    public void testMaxValueNegAnyField_federatedQueryPlanner() throws Exception {
        log.info("------  testMaxValueNegAnyField : " + DatePartitionedQueryPlanner.class.getSimpleName() + "  ------");

        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());

        String regexPhrase = RE_OP + "'a.*'";
        String country = "'b-StaTe'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + NOT_OP + "(" + Constants.ANY_FIELD + regexPhrase + ")";
        String expect = CitiesDataType.CityField.STATE.name() + EQ_OP + "'bi-s'";

        String expectedPlan = "STATE == 'b-state' && !(((_Delayed_ = true) && (_ANYFIELD_ =~ 'a.*')) || CODE == 'a-code' || CITY == 'a-1' || STATE == 'a-state' || STATE == 'a-s2')";

        // value expansion does not affect final plan
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        String plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // value expansion does not affect final plan
        this.logic.setMaxValueExpansionThreshold(1);
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);

        // ivarator config does not affect final plan
        ivaratorConfig();
        runTest(query, expect);
        plan = getPlan(query, true, true);
        assertEquals(expectedPlan, plan);
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getExpansionAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
